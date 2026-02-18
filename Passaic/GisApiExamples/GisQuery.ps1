# Define the token request URL (ensure this is the correct URL for your ArcGIS instance)
$tokenUrl = "https://gis.pvwc.com/portaltest/sharing/rest/generateToken"

# Define the request parameters for token generation
$tokenParams = @{
    username = "CIS_User"
    password = '$qv!Ks4Bt0vPxL'
    client = "requestip"
    expiration = "60"  
    f = "json"  
}

# Request the token
$tokenResponse = Invoke-RestMethod -Uri $tokenUrl -Method Post -Body $tokenParams

# Extractx the token from the response
$token = $tokenResponse.token

# Check if the token was successfully generated
if (-not $token) {
    Write-Error "Failed to generate token. Response: $($tokenResponse | ConvertTo-Json)"
    exit
}



# Define the GIS service query URL
$serviceUrl = "https://gis.pvwc.com/hostingtest/rest/services/TEST_CustomerLocations/FeatureServer/0/query"

# Define the query parameters
$queryParams = @{
    "where"                = "C_account='057888*2' OR C_account='057888'"
    "outFields"            = "*"
    "returnGeometry"       = "true"
    "resultRecordCount"    = "10"
    "f"                    = "json"
    "token"                = $token
}
try {
    # Make the request using the hashtable as parameters
    $response = Invoke-RestMethod -Uri $serviceUrl -Method Get -Body $queryParams -ErrorAction Stop

    # Check if data was returned
    if ($response -and $response.features) {
        Write-Host "Data retrieved successfully!"
        $response
    } else {
        Write-Error "No data found in the response."
    }
} catch {
    Write-Error "An error occurred while making the request: $_"
}