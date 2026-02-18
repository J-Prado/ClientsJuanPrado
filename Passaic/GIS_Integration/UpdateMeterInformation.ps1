#############################################
# CIS → GIS DETERMINISTIC VALIDATION SCRIPT
#############################################

# ===========================
# CONFIGURATION
# ===========================
$BatchSize = 500
$MaxFailuresAllowed = 0
$LogFile = "C:\Temp\CIS_GIS_Validation_$(Get-Date -Format yyyyMMdd_HHmmss).log"

#############################################
# LOGGING
#############################################
function Write-Log {
    param ([string]$Message,[string]$Level="INFO")
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') [$Level] $Message"
    Add-Content -Path $LogFile -Value $line
    Write-Host $line
}

#############################################
# HELPERS
#############################################
function Get-IntegrationKey {
    param ($account,$meter,$readtype)
    return "$account|$meter|$readtype"
}

function Get-CISHash {
    param ($cis)

    $raw = @(
        $cis.Account
        $cis.Meter
        $cis.ReadType
        $cis.BillCode
        $cis.RemoteId
        $cis.Meter_Lookup.MeterType
        $cis.Meter_Lookup.MeterSize
    ) -join "|"

    $bytes = [System.Text.Encoding]::UTF8.GetBytes($raw)
    $sha   = [System.Security.Cryptography.SHA256]::Create()
    $hash  = $sha.ComputeHash($bytes)

    return ($hash | ForEach-Object { $_.ToString("x2") }) -join ""
}

#############################################
# INTEGRATION SETUP
#############################################
$module = 'PASSAICGIS'
$where = "C_MODULE={0}"

$IntegrationControl = [AdvancedUtility.Services.BusinessObjects.IntegrationSetup]::GetAllWhere($CisSession,$where,$module)

$queryUrl = $IntegrationControl | Where-Object {$_.Property -eq "QUERYURL"}
$TokenUrl = $IntegrationControl | Where-Object {$_.Property -eq "TOKENURL"}
$user     = $IntegrationControl | Where-Object {$_.Property -eq "USERNAME"}
$password = $IntegrationControl | Where-Object {$_.Property -eq "PASSWORD"}
$referer  = $IntegrationControl | Where-Object {$_.Property -eq "REFERER"}
$updateurl  = $IntegrationControl | Where-Object {$_.Property -eq "UPDATEURL"}
$addurl  = $IntegrationControl | Where-Object {$_.Property -eq "ADDURL"}

function GetIntValue {
    param ($key)
    ([AdvancedUtility.Services.BusinessObjects.IntegrationValue]::GetByIntegrationKey(
        $CisSession,$key)).ExternalValue
}

#############################################
# TOKEN
#############################################
Write-Log "Requesting GIS token"

$token = (Invoke-RestMethod `
    -Uri (GetIntValue $TokenUrl.IntegrationKey) `
    -Method Post `
    -Body @{
        username=GetIntValue $user.IntegrationKey
        password=GetIntValue $password.IntegrationKey
        client="referer"
        referer=GetIntValue $referer.IntegrationKey
        f="json"
    }).token

Write-Log "Token acquired"

#############################################
# LOAD CIS DATA
#############################################
Write-Log "Loading CIS records"

#$CISMeters = [AdvancedUtility.Services.BusinessObjects.AccountMeterReadType]::GetAll($CisSession)
$CISMeters = [AdvancedUtility.Services.BusinessObjects.AccountMeterReadType]::GetAllwhere($CisSession,"C_ACCOUNT = '196964'")

$CISIndex = @{}
foreach ($m in $CISMeters) {
    $CISIndex[(Get-IntegrationKey $m.Account $m.Meter $m.ReadType)] = $m
}

Write-Log "Loaded $($CISIndex.Count) CIS records"

#############################################
# VALIDATION LOOP
#############################################
$GISQueryUrl = GetIntValue $queryUrl.IntegrationKey
$totalChecked = 0
$totalMissing = 0
$totalMismatch = 0

$keys = $CISIndex.Keys
$totalBatches = [math]::Ceiling($keys.Count / $BatchSize)

for ($batch = 0; $batch -lt $totalBatches; $batch++) {

    $subset = $keys[
        ($batch*$BatchSize)..([Math]::Min($keys.Count-1,($batch+1)*$BatchSize-1))
    ]

    foreach ($key in $subset) {

        $cis = $CISIndex[$key]
        $expectedHash = Get-CISHash $cis

        $where = "C_ACCOUNT='$($cis.Account)' AND C_METER='$($cis.Meter)' AND C_READTYPE='$($cis.ReadType)'"

        $resp = Invoke-RestMethod `
            -Uri $GISQueryUrl `
            -Method Post `
            -Body @{
                where=$where
                outFields="OBJECTID,c_sync_hash"
                returnGeometry="false"
                f="json"
                token=$token
            }

        if ($resp.features.Count -ne 1) {
            Write-Log "MISSING GIS record: $key" "ERROR"
            $totalMissing++
            continue
        }

        $gisHash = $resp.features[0].attributes.c_sync_hash

        if ($gisHash -ne $expectedHash) {
            Write-Log "HASH MISMATCH $key GIS=$gisHash CIS=$expectedHash" "ERROR"
            $totalMismatch++
        }

        $totalChecked++
    }

    Write-Log "Validated batch $($batch+1)/$totalBatches"
}

#############################################
# FINAL REPORT
#############################################
Write-Log "VALIDATION COMPLETE"
Write-Log "Checked     : $totalChecked"
Write-Log "Missing GIS : $totalMissing"
Write-Log "Hash Errors : $totalMismatch"

if ($totalMissing -gt 0 -or $totalMismatch -gt 0) {
    Write-Log "VALIDATION FAILED" "ERROR"
} else {
    Write-Log "VALIDATION PASSED — DATA IS CONSISTENT"
}
