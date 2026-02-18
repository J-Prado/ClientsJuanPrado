$queryUrl = "https://gis.pvwc.com/hostingtest/rest/services/TEST_CustomerLocations/FeatureServer/0/query"

$queryParams = @{
    where     = "C_account='186348'"
    outFields = "OBJECTID"
    f         = "json"
    token     = $token
}

$queryResponse = Invoke-RestMethod -Uri $queryUrl -Method Get -Body $queryParams

if ($queryResponse.features.Count -eq 0) {
    Write-Error "No features found for account 186348"
    exit
}

$objectId = $queryResponse.features[0].attributes.OBJECTID

Write-Host "Updating OBJECTID: $objectId"


$updateUrl = "https://gis.pvwc.com/hostingtest/rest/services/TEST_CustomerLocations/FeatureServer/0/updateFeatures"

$updateFeature = @{
    attributes = @{
        OBJECTID = $objectId
        InLucity = $true
    }
}

$updateParams = @{
    features = (ConvertTo-Json @($updateFeature) -Compress)
    f        = "json"
    token    = $token
}

$updateResponse = Invoke-RestMethod -Uri $updateUrl -Method Post -Body $updateParams
$updateResponse
