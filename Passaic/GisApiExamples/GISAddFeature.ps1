# Request token
$tokenUrl = "https://gis.pvwc.com/portaltest/sharing/rest/generateToken"
$tokenParams = @{
    username   = "CIS_User"
    password   = '$qv!Ks4Bt0vPxL'
    client     = "referer"
    referer    = "https://gis.pvwc.com/portaltest"
    expiration = "60"
    f          = "json"
}
$tokenResponse = Invoke-RestMethod -Uri $tokenUrl -Method Post -Body $tokenParams
$token = $tokenResponse.token

if (-not $token) {
    Write-Error "Failed to get token"
    exit
}

# Add Feature URL
$addUrl = "https://gis.pvwc.com/hostingtest/rest/services/TEST_CustomerLocations/FeatureServer/0/addFeatures"

# New feature with only filled attributes
$newFeature = @{
    attributes = @{
        c_account            = "186348"
        c_accounttype        = "RE"
        c_accountstatus      = "AC"
        c_streetnumber       = "165"
        c_streetprefix       = "N"
        c_street             = "MAIN"
        c_streetsuffix       = "ST"
        c_town               = "PATERSON"
        c_prov               = "NJ"
        c_postcode           = "07522"
        c_customer           = "0318424"
        c_lastname           = "PEREIRA"
        c_nametype           = "R"
        c_meter              = "39609805"
        c_remotetype         = "RR"
        Add_Valid_Type       = "fv"
        adj_street           = "0"
        service_size         = "unk"
        mc_mat               = "c"
        mc_inspect_date      = 1729468800000
        cb_mat               = "c"
        cb_inspect_date      = 1729468800000
        info_source          = "testpit"
    }
    # geometry only if required (replace coords with actual values)
    geometry = @{
        x = 0
        y = 0
        spatialReference = @{ wkid = 4326 }
    }
}

# POST parameters
$addParams = @{
    features = (ConvertTo-Json @($newFeature) -Compress)
    f        = "json"
    token    = $token
}

# Send request
$addResponse = Invoke-RestMethod -Uri $addUrl -Method Post -Body $addParams
$addResponse