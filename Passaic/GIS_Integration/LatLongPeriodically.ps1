#############################################
# CIS to GIS Lat / Long SYNC SCRIPT (FULL)
#############################################

# ===========================
# CONFIGURATION
# ===========================
$PageSize   = 1000
$ThrottleMs = 300
$LogFile    = "C:\Temp\GIS_CIS_Sync_Day_$(Get-Date -Format yyyyMMdd_HHmmss).log"
$Wkid       = 6527
$DebugCsv   = "C:\Temp\testLat2.csv"

#############################################
# LOGGING
#############################################
function Write-Log {
    param (
        [string]$Message,
        [string]$Level = "INFO"
    )
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') [$Level] $Message"
    Add-Content -Path $LogFile -Value $line
    Write-Host $line
}

#############################################
# TOKEN HANDLING
#############################################
$script:Token = $null
$script:TokenUrlValue = $null
$script:TokenParams = $null

function Get-GISToken {
    Write-Log "Requesting GIS token"

    $resp = Invoke-RestMethod `
        -Uri $script:TokenUrlValue `
        -Method Post `
        -Body $script:TokenParams `
        -ContentType "application/x-www-form-urlencoded"

    if (-not $resp.token) {
        throw "Failed to acquire GIS token"
    }

    $script:Token = $resp.token
    Write-Log "GIS token acquired"
}

#############################################
# INTEGRATION SETUP
#############################################
$module = "PASSAICGIS"
$where  = "C_MODULE={0}"

$IntegrationControl =
    [AdvancedUtility.Services.BusinessObjects.IntegrationSetup]::GetAllWhere(
        $CisSession, $where, $module
    )

function GetIntValue {
    param ($key)
    ([AdvancedUtility.Services.BusinessObjects.IntegrationValue]::GetByIntegrationKey(
        $CisSession, $key
    )).ExternalValue
}

$referer  = $IntegrationControl | Where-Object Property -eq "REFERER"
$queryUrl = $IntegrationControl | Where-Object Property -eq "QUERYURL"
$tokenUrl = $IntegrationControl | Where-Object Property -eq "TOKENURL"
$user     = $IntegrationControl | Where-Object Property -eq "USERNAME"
$password = $IntegrationControl | Where-Object Property -eq "PASSWORD"

$script:TokenUrlValue = GetIntValue $tokenUrl.IntegrationKey
$script:TokenParams = @{
    username   = GetIntValue $user.IntegrationKey
    password   = GetIntValue $password.IntegrationKey
    client     = "referer"
    referer    = GetIntValue $referer.IntegrationKey
    expiration = "120"
    f          = "json"
}

Get-GISToken

#############################################
# GIS HELPER
#############################################
function Select-GISFeature {
    param ([array]$Features)

    $withGeom = $Features | Where-Object {
        $_.geometry -and $_.geometry.x -and $_.geometry.y
    }

    if (-not $withGeom) {
        return $null
    }

    return $withGeom |
        Sort-Object { $_.attributes.OBJECTID } |
        Select-Object -First 1
}

#############################################
# LOAD GIS DATA
#############################################
Write-Log "Loading GIS features"

$GISQueryUrl = GetIntValue $queryUrl.IntegrationKey
$GISByAccount = @{}
$offset = 0

do {
    $body = @{
        where               = "EditDate>=CURRENT_TIMESTAMP - 7"
        outFields           = "OBJECTID,C_ACCOUNT"
        returnGeometry      = "true"
        outSR               = $Wkid
        resultOffset        = $offset
        resultRecordCount   = $PageSize
        f                   = "json"
        token               = $script:Token
    }

    $resp = Invoke-RestMethod `
        -Uri $GISQueryUrl `
        -Method Get `
        -Body $body `
        -ErrorAction Stop

    $features = $resp.features
    $count = if ($features) { $features.Count } else { 0 }

    foreach ($f in $features) {
        $acct = $f.attributes.C_ACCOUNT
        if (-not $GISByAccount.ContainsKey($acct)) {
            $GISByAccount[$acct] = @()
        }
        $GISByAccount[$acct] += $f
    }

    Write-Log "Fetched $count GIS records"
    $offset += $PageSize
    Start-Sleep -Milliseconds $ThrottleMs

} while ($count -eq $PageSize)

Write-Log "Loaded GIS accounts: $($GISByAccount.Count)"

#############################################
# SYNC CIS to GIS
#############################################
Write-Log "Starting CIS latitude/longitude update"

$updated = 0
$skipped = 0

foreach ($acctKey in $GISByAccount.Keys) {

    # Optional debug
    $acctKey | Out-File -FilePath $DebugCsv -Append

    $CISAccount =
        [AdvancedUtility.Services.BusinessObjects.Account]::GetByWhere(
            $CisSession,
            "C_ACCOUNT = {0}",
            $acctKey
        )

    if (-not $CISAccount) {
        Write-Log "CIS account not found: $acctKey" "WARN"
        $skipped++
        continue
    }

    $gisFeature = Select-GISFeature -Features $GISByAccount[$acctKey]

    if (-not $gisFeature) {
        Write-Log "No valid GIS geometry for account $acctKey" "WARN"
        $skipped++
        continue
    }

    $newLat = [Math]::Round([double]$gisFeature.geometry.y, 9)
    $newLon = [Math]::Round([double]$gisFeature.geometry.x, 9)

    if ($CISAccount.Latitude -ne $newLat -or
        $CISAccount.Longitude -ne $newLon) {

        $CISAccount.Latitude  = $newLat
        $CISAccount.Longitude = $newLon

        if ($CISAccount.Save()) {
            $updated++
            Write-Log "Updated account $acctKey → Lat:$newLat Long:$newLon"
        }
        else {
            Write-Log "Validation failed for $acctKey : $($CISAccount.Validate())" "WARN"
            $skipped++
        }
    }
}

#############################################
# SUMMARY
#############################################
Write-Log "SYNC COMPLETE"
Write-Log "Updated accounts: $updated"
Write-Log "Skipped accounts: $skipped"
