#############################################
# CIS to GIS Lat Long SYNC SCRIPT (FULL)
#############################################

#############################################
# CONFIGURATION
#############################################
$BatchSize = 100
$MaxRetries = 3
$RetryDelaySeconds = 5
$AutoContinueOnFailure = $true
$ThrottleMs = 500
$LogFile = "C:\Temp\CIS_GIS_Sync_$(Get-Date -Format yyyyMMdd_HHmmss).log"
$CheckpointFile = "C:\Temp\CIS_GIS_Sync.checkpoint.json"

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
# HELPERS
#############################################
function Convert-ToEpoch {
    param ($date)
    if (-not $date) { return $null }
    return [int64]([DateTimeOffset]$date).ToUnixTimeMilliseconds()
}

function Get-IntegrationKey {
    param ($account, $meter, $readtype)
    return "$account|$meter|$readtype"
}

function Invoke-ArcGISPost {
    param(
        [Parameter(Mandatory)] [string] $Uri,
        [Parameter(Mandatory)] [hashtable] $Body
    )


    for ($attempt = 1; $attempt -le $MaxRetries; $attempt++) {
        try {
            # Inject current token
            if ($script:Token) { $Body.token = $script:Token }
            
            
            $resp = Invoke-RestMethod -Uri $Uri -Method Post -Body $Body -ErrorAction Stop
            #$resp |  out-file -filepath "C:\Temp\test.csv" -append
            # ArcGIS sometimes returns HTTP 200 with an error object
            if ($resp -and $resp.error -and ($resp.error.code -in 498, 499)) {
                Write-Log "Token invalid/expired (code $($resp.error.code)). Refreshing token..." "WARN"
                Get-GISToken
                # retry immediately with fresh token
                $Body.token = $script:Token
                $resp = Invoke-RestMethod -Uri $Uri -Method Post -Headers $headers -Body $Body -ErrorAction Stop
            }

            return $resp
        }
        catch {
            Write-Log "REST call failed (attempt $attempt): $($_.Exception.Message)" "WARN"
            if ($attempt -eq $MaxRetries) { throw }
            Start-Sleep -Seconds $RetryDelaySeconds
        }
    }
}
#############################################
# CHECKPOINTING
#############################################
function Checkpoint-Load {
    if (Test-Path $CheckpointFile) {
        return Get-Content $CheckpointFile | ConvertFrom-Json
    }
    return @{ AddsBatch = 0; UpdatesBatch = 0 }
}

function Save-Checkpoint {
    param ($AddsBatch, $UpdatesBatch)
    @{ AddsBatch = $AddsBatch; UpdatesBatch = $UpdatesBatch } |
        ConvertTo-Json | Set-Content $CheckpointFile
}

#############################################
# INTEGRATION SETUP
#############################################
$module = "PASSAICGIS"
$where = "C_MODULE={0}"

$IntegrationControl = [AdvancedUtility.Services.BusinessObjects.IntegrationSetup]::GetAllWhere($CisSession,$where,$module)

$referer  = $IntegrationControl | Where-Object {$_.Property -eq "REFERER"}
$addUrl   = $IntegrationControl | Where-Object {$_.Property -eq "ADDURL"}
$updateUrl   = $IntegrationControl | Where-Object {$_.Property -eq "UPDATEURL"}
$queryUrl = $IntegrationControl | Where-Object {$_.Property -eq "QUERYURL"}
$TokenUrl = $IntegrationControl | Where-Object {$_.Property -eq "TOKENURL"}
$user     = $IntegrationControl | Where-Object {$_.Property -eq "USERNAME"}
$password = $IntegrationControl | Where-Object {$_.Property -eq "PASSWORD"}

function GetIntValue {
    param ($key)
    ([AdvancedUtility.Services.BusinessObjects.IntegrationValue]::GetByIntegrationKey($CisSession,$key)).ExternalValue
}

#############################################
# TOKEN
#############################################
Write-Log "Requesting GIS token"

$tokenParams = @{
    username   = GetIntValue $user.IntegrationKey
    password   = GetIntValue $password.IntegrationKey
    client     = "referer"
    referer    = GetIntValue $referer.IntegrationKey
    expiration = "20188888888888888"
    f          = "json"
}

$token = (Invoke-RestMethod -Uri (GetIntValue $TokenUrl.IntegrationKey) -Method Post -Body $tokenParams).token

Write-Log "Token acquired"

#############################################
# LOAD CIS DATA
#############################################
Write-Log "Loading CIS meters"
#$CISMeters = [AdvancedUtility.Services.BusinessObjects.AccountMeterReadType]::GetAll($CisSession)
$CISAccounts = [AdvancedUtility.Services.BusinessObjects.Account]::GetAll($CisSession)

$CISIndex = @{}

foreach($a in $CISAccounts){
    $CISMeters = [AdvancedUtility.Services.BusinessObjects.AccountMeterReadType]::GetAllWhere($CisSession,"C_ACCOUNT = {0}",$a)
    foreach ($m in $CISMeters) {
        $CISIndex[(Get-IntegrationKey $m.Account $m.Meter $m.ReadType)] = $m
    }

}

# foreach ($m in $CISMeters) {
#     $CISIndex[(Get-IntegrationKey $m.Account $m.Meter $m.ReadType)] = $m
# }

Write-Log "Loaded $($CISIndex.Count) CIS records"

#############################################
# LOAD GIS DATA (PAGINATED)
#############################################
Write-Log "Loading GIS features (paginated)"

$GISQueryUrl = GetIntValue $queryUrl.IntegrationKey
$GISIndex = @{}
$offset = 0
$pageSize = 1000

do {
    $queryParams = @{
            where= "1=1"
            outFields="OBJECTID,C_ACCOUNT,C_METER,C_READTYPE,d_dateremoved"
            returnGeometry="false"
            resultOffset=$offset
            resultRecordCount=$pageSize
            f="json"
            token=$token
    }
    
    $GISResponse = Invoke-RestMethod -Uri $GISQueryUrl -Method Get -Body $queryParams -ErrorAction Stop

    $count = $GISResponse.features.Count
    foreach ($f in $GISResponse.features) {
        $a = $f.attributes
    
        $readType = $a.C_READTYPE
        $meter = $a.C_METER
        if($meter -ne '' -or $null -ne $meter){
            if($readType -eq '' -or $null -eq $readType){
                $readType = 'WT'
            }
        }
        
        $GISIndex[(Get-IntegrationKey $a.C_ACCOUNT $meter $readType)] = $f
    }

    Write-Log "Fetched $count GIS records"
    $offset += $pageSize
} while ($count -eq $pageSize)

Write-Log "Loaded TOTAL GIS records: $($GISIndex.Count)"

#############################################
# BUILD ADD / UPDATE LISTS
#############################################
$Adds = @()
$Updates = @()

foreach ($key in $CISIndex.Keys) {

    $cis = $CISIndex[$key]
    $gis = $GISIndex[$key]

    $customerAccount = [AdvancedUtility.Services.BusinessObjects.CustomerAccount]::GetByAccount($CisSession,$cis.Account,$null,$null,$false,$false,$null)

    $AccountObj = $customerAccount.Account_Lookup
    #$AccountObj.Apt |  out-file -filepath "C:\Temp\test.csv" -append
    $CustomerObj = $customerAccount.Customer_Lookup
    $InstLocation = [AdvancedUtility.Services.BusinessObjects.MeterReadingInstructionLocation]::GetByServiceIDMeter($CisSession,$cis.ServiceId,$cis.Meter)

    # Prepare attributes
    $cisReadType = $cis.ReadType
    $attrs = @{
        c_account       = $cis.Account
        c_meter         = $cis.Meter
        c_readtype      = $cisReadType
        i_dials         = $cis.Dials
        c_billcode      = $cis.BillCode
        c_book1         = $customerAccount.Book
        c_cycle         = $customerAccount.Cycle
        c_division      = $customerAccount.Division
        c_remoteid      = $cis.RemoteId
        c_remotetype    = $cis.RemoteType
        c_metertype     = $cis.Meter_Lookup.MeterType
        c_metersize     = $cis.Meter_Lookup.MeterSize
        d_dateinstall   = Convert-ToEpoch $cis.StartDate
        d_dateremoved   = Convert-ToEpoch $cis.EndDate
        c_meternotes    = $InstLocation.MeterNotes
        c_lastname      = $CustomerObj.LastName
        c_nametype      = $CustomerObj.NameType
        c_customer      = $customerAccount.Customer
        c_address1      = $AccountObj.AddressLine1
        c_apt           = $AccountObj.Apt
        c_postcode      = $AccountObj.PostalCode
        c_prov          = $AccountObj.State
        c_town          = $AccountObj.Town
        c_street        = $AccountObj.StreetName
        c_streetprefix  = $AccountObj.StreetPrefix
        c_streetsuffix  = $AccountObj.StreetSuffix
        c_streetnumber  = $AccountObj.StreetNumber
        c_accountstatus = $customerAccount.AccountStatus
        c_accounttype   = $customerAccount.AccountType
    }


    if (-not $gis) {
        # ADD: only for active CIS records (EndDate is null)
        if (-not $cis.EndDate) {

            # Per confirmation: if CIS ReadType is blank/null AND record is new (not on GIS), set to WT for the payload
            if ([string]::IsNullOrWhiteSpace($cisReadType)) { $attrs.c_readtype = 'WT' }

            # Include geometry only for new features
            $feature = @{
                attributes = $attrs
                geometry   = @{
                    x = $GeomX
                    y = $GeomY
                    spatialReference = @{ wkid = $Wkid }
                }
            }

            $Adds += $feature
        }
    }
    else {
        # UPDATE: include OBJECTID; do NOT send geometry (per policy)
        $attrs.OBJECTID = $gis.attributes.OBJECTID
        $feature = @{ attributes = $attrs }
        $Updates += $feature
    }
}
#$Adds |  out-file -filepath "C:\Temp\test.csv" -append
#$Updates |  out-file -filepath "C:\Temp\test.csv" -append

#############################################
# APPLY EDITS WITH SAFE THROTTLING
#############################################
function Send-Batches {
    param (
        [Parameter(Mandatory)][array] $Items,
        [Parameter(Mandatory)][string] $Type,
        [Parameter(Mandatory)][string] $Token
    )

    $checkpoint = Checkpoint-Load
    $startBatch = if ($Type -eq "adds") { $checkpoint.AddsBatch } else { $checkpoint.UpdatesBatch }
    $totalBatches = if ($Items.Count -gt 0) { [math]::Ceiling($Items.Count / $BatchSize) } else { 0 }
    #$checkpoint |  out-file -filepath "C:\Temp\test.csv" -append
    #$startBatch |  out-file -filepath "C:\Temp\test.csv" -append
    #$totalBatches |  out-file -filepath "C:\Temp\test.csv" -append
    for ($batchIndex = $startBatch; $batchIndex -lt $totalBatches; $batchIndex++) {
        $start = $batchIndex * $BatchSize
        $end   = [Math]::Min($Items.Count - 1, ($batchIndex + 1) * $BatchSize - 1)
        $batch = $Items[$start..$end]

        Write-Log "Processing $Type batch $($batchIndex+1)/$totalBatches"
        if ($Type -eq "adds") {
            $TargetUrl = GetIntValue $addUrl.IntegrationKey
        } else {
            $TargetUrl = GetIntValue $updateUrl.IntegrationKey
        }

        $params = @{
            features = (ConvertTo-Json @($batch) -Compress)
            f = "json"
            rollbackOnFailure = "true" 
            token = $Token
        }

        try {
            $response = Invoke-ArcGISPost -Uri $TargetUrl -Body $params
            #$response.error |  out-file -filepath "C:\Temp\test.csv" -append
        }
        catch {
            Write-Log "$Type batch $($batchIndex+1) failed: $($_.Exception.Message)" "ERROR"
            if (-not $AutoContinueOnFailure) { throw }
            # continue to next batch
            if ($Type -eq "adds") {
                Save-Checkpoint ($batchIndex+1) $checkpoint.UpdatesBatch
            } else {
                Save-Checkpoint $checkpoint.AddsBatch ($batchIndex+1)
            }
            Start-Sleep -Milliseconds $ThrottleMs
            continue
        }

        # Summarize results
        $results = if ($Type -eq "adds") { $response.addResults } else { $response.updateResults }
      
        if (-not $results) {
            Write-Log "No results returned for $Type batch $($batchIndex+1)" "WARN"
        } else {
            $success = (@($results | Where-Object { $_.success -eq $true })).Count
            
            $failed  = $results.Count - $success
            Write-Log "$Type batch result: Success=$success Failed=$failed"
            if ($failed -gt 0) {
                # Print a few first errors for visibility
                ($results | Where-Object { $_.success -ne $true } | Select-Object -First 3) | ForEach-Object {
                    Write-Log "Error: code=$($_.error.code) desc=$($_.error.description)" "WARN"
                }
            }
        }

        # Advance checkpoint
        if ($Type -eq "adds") {
            Save-Checkpoint ($batchIndex+1) $checkpoint.UpdatesBatch
        } else {
            Save-Checkpoint $checkpoint.AddsBatch ($batchIndex+1)
        }

        Start-Sleep -Milliseconds $ThrottleMs
    }
}

Write-Log "Prepared Adds=$($Adds.Count) Updates=$($Updates.Count)"
Send-Batches -Items $Adds -Type "adds" -Token $token
Send-Batches -Items $Updates -Type "updates" -Token $token

#############################################
# FINAL CLEANUP
#############################################
Remove-Item $CheckpointFile -ErrorAction SilentlyContinue
Write-Log "Checkpoint cleared"
Write-Log "SYNC COMPLETE — Adds=$($Adds.Count) Updates=$($Updates.Count)"
