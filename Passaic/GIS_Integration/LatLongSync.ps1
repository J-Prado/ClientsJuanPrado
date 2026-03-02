# =====================================================================
# CIS ← GIS Latitude / Longitude One-Time Sync
# =====================================================================

[CmdletBinding()]
param (
    [Parameter(Mandatory)]
    [string]$UserId,

    [Parameter(Mandatory)]
    [string]$Password,

    [switch]$WhatIf
)

# ---------------------------------------------------------------------
#region Configuration
# ---------------------------------------------------------------------

$Config = @{
    CisAppLocation = 'E:\CIS4TEST\'
    LogPath        = "C:\Temp\GIS_CIS_Sync_$(Get-Date -Format yyyyMMdd_HHmmss).log"
    CheckpointPath = "C:\Temp\GIS_CIS_Checkpoint.txt"
    PageSize       = 1000
    ThrottleMs     = 300
    Wkid           = 6527
    GisModule      = 'PASSAICGIS'
    ChunkSize      = 500
}

# ---------------------------------------------------------------------
#endregion Configuration
# ---------------------------------------------------------------------

# ---------------------------------------------------------------------
#region Logging
# ---------------------------------------------------------------------

function Write-Log {
    param(
        [string]$Message,
        [ValidateSet('INFO','WARN','ERROR')]
        [string]$Level = 'INFO'
    )

    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') [$Level] $Message"
    Add-Content -Path $Config.LogPath -Value $line

    if ($Level -ne 'INFO') {
        Write-Host $line
    }
}

# ---------------------------------------------------------------------
#endregion Logging
# ---------------------------------------------------------------------

# ---------------------------------------------------------------------
#region CIS Bootstrap
# ---------------------------------------------------------------------

function Invoke-WithCisSession {
    param(
        [string]$UserId,
        [string]$Password,
        [scriptblock]$Script
    )

    [void][System.Reflection.Assembly]::LoadFrom(
        (Join-Path $Config.CisAppLocation 'AdvancedUtility.ServicesLoader.dll')
    )

    $loader = New-Object AdvancedUtility.ServicesLoader.Loader
    $cis    = $loader.GetCisApplication()

    if (-not $cis.Open()) {
        throw "Failed to open CIS application"
    }

    try {
        $session = $cis.GetSession($UserId, $Password)
        if (-not $session.Open()) {
            throw "Failed to open CIS session"
        }

        & $Script $session
    }
    finally {
        if ($session) { $session.Close() }
        $cis.Close()
    }
}

# ---------------------------------------------------------------------
#endregion CIS Bootstrap
# ---------------------------------------------------------------------

# ---------------------------------------------------------------------
#region GIS Helpers
# ---------------------------------------------------------------------

function Get-GisToken {
    param($TokenUrl, $Params)

    (Invoke-RestMethod -Uri $TokenUrl -Method Post `
        -Body $Params -ContentType "application/x-www-form-urlencoded").token
}

function Get-GisFeatures {
    param($QueryUrl, $Token)

    $all    = @()
    $offset = 0

    do {
        $resp = Invoke-RestMethod -Uri $QueryUrl -Method Get -Body @{
            where="1=1"; outFields="OBJECTID,C_ACCOUNT"
            returnGeometry="true"; outSR=$Config.Wkid
            resultOffset=$offset; resultRecordCount=$Config.PageSize
            f="json"; token=$Token
        }

        $batch = $resp.features
        if ($batch) {
            $all += $batch
            $offset += $Config.PageSize
            Start-Sleep -Milliseconds $Config.ThrottleMs
        }
    }
    while ($batch.Count -eq $Config.PageSize)

    return $all
}

# ---------------------------------------------------------------------
#endregion GIS Helpers
# ---------------------------------------------------------------------

# ---------------------------------------------------------------------
#region Sync Engine
# ---------------------------------------------------------------------

function Invoke-LatLongSyncInternal {
    param($Session)

    $cisAccounts = [AdvancedUtility.Services.BusinessObjects.Account]::GetAll($Session)

    $cisByAccount = @{}
    foreach ($acct in $cisAccounts) {
        $cisByAccount[$acct.AccountNumber] = $acct
    }

    $integration = [AdvancedUtility.Services.BusinessObjects.IntegrationSetup]::GetAllWhere(
        $Session, "C_MODULE={0}", $Config.GisModule
    )

    function Get-Val($key) {
        ([AdvancedUtility.Services.BusinessObjects.IntegrationValue]::GetByIntegrationKey(
            $Session, $key
        )).ExternalValue
    }

    $queryUrl = Get-Val (($integration | Where Property -eq 'QUERYURL').IntegrationKey)
    $tokenUrl = Get-Val (($integration | Where Property -eq 'TOKENURL').IntegrationKey)

    $token = Get-GisToken $tokenUrl @{
        username = Get-Val (($integration | Where Property -eq 'USERNAME').IntegrationKey)
        password = Get-Val (($integration | Where Property -eq 'PASSWORD').IntegrationKey)
        client='referer'
        referer=Get-Val (($integration | Where Property -eq 'REFERER').IntegrationKey)
        f='json'
    }

    $features = Get-GisFeatures $queryUrl $token

    $gisByAccount = @{}
   foreach ($f in $features) {

    if (-not $f.attributes) { continue }

    $acct = $f.attributes.C_ACCOUNT

    # Skip GIS features with no account number
    if ([string]::IsNullOrWhiteSpace($acct)) {
        Write-Log "This '${$f.attributes.OBJECTID}' is being Skipped no Account information."
        continue
    }

    if (-not $gisByAccount.ContainsKey($acct)) {
        $gisByAccount[$acct] = @()
    }

    $gisByAccount[$acct] += $f
    }

    $accountKeys = $cisByAccount.Keys | Sort-Object
    $total       = $accountKeys.Count

    $resumeFrom = if (Test-Path $Config.CheckpointPath) {
        Get-Content $Config.CheckpointPath
    }

    if ($resumeFrom) {
        $accountKeys = $accountKeys | Where-Object { $_ -gt $resumeFrom }
        Write-Log "Resuming from account $resumeFrom"
    }

    $processed = 0
    $updated   = 0

    foreach ($chunk in ($accountKeys | ForEach-Object -Begin {$i=0} -Process {
        if ($i++ % $Config.ChunkSize -eq 0) { ,@() }
        $_
    })) {

        foreach ($acctKey in $chunk) {

            $processed++
            Write-Progress -Activity "CIS ← GIS Sync" `
                -Status "Processing $acctKey ($processed / $total)" `
                -PercentComplete (($processed / $total) * 100)

            if (-not $gisByAccount.ContainsKey($acctKey)) { 
                Write-Log "This $acctKey is not part of the GIS service."
                continue 
            }
            
            $multiple = $gisByAccount[$acctKey].Count

            if($multiple -gt 1){
                Write-Log "This $acctKey has $multiple records in GIS."
            }
            
            $gis = $gisByAccount[$acctKey] |
                Where-Object { $_.geometry.x -and $_.geometry.y } |
                Sort-Object { $_.attributes.OBJECTID } |
                Select-Object -First 1

            if (-not $gis) { continue }

            $cis = $cisByAccount[$acctKey]
            $lat = [math]::Round($gis.geometry.y, 9)
            $lon = [math]::Round($gis.geometry.x, 9)

            if ($cis.Latitude -ne $lat -or $cis.Longitude -ne $lon) {
                if (-not $WhatIf) {
                    $cis.Latitude = $lat
                    $cis.Longitude = $lon
                    $cis.Save() | Out-Null
                }
                $updated++
            }

            Set-Content -Path $Config.CheckpointPath -Value $acctKey
        }
    }

    Write-Progress -Activity "CIS ← GIS Sync" -Completed
    Write-Log "Updated accounts: $updated"
}

# ---------------------------------------------------------------------
#endregion Sync Engine
# ---------------------------------------------------------------------

Invoke-WithCisSession -UserId $UserId -Password $Password {
    param($session)
    Invoke-LatLongSyncInternal $session
}