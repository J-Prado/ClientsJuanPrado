# Sample PowerShell V4 script to open a CIS application with the given user and password and do some basic API interactions
param(
[Parameter(Mandatory = $true)] [string] $userid,
[Parameter(Mandatory = $true)] [string] $password
)
# make sure your script is signed, or your scripting environment can execute unsigned scripts ("set-executionpolicy unrestricted")

# check that we are using at least version 4 (just for fun)
if ($Host.Version.Major -lt 4)
{
    Write-Host "This script requires at least PowerShell version 4."
    exit
}

# !!! adjust the $appLocation below to point to the ServicesLoader.DLL of the applicable system
$appLocation = "C:\CISInfinity\UC2019_V4\CIS4\"

[void] [System.Reflection.Assembly]::loadfrom($appLocation + "AdvancedUtility.ServicesLoader.dll")

$loader = new-object AdvancedUtility.ServicesLoader.Loader

# If the config file for this system does not reside in the default location (alongside the servicesloader.dll), 
#     then configure the loader now with the required settings...
#    $loader.ServicesLocation = "\\MyServer\OtherShare\CISV4\"

write-host "Get instance of CIS application object now..."
$cis = $loader.GetCisApplication()

# configure the application object now before opening it - not normally necessary

# open the CIS application now - this may throw exceptions if the application is not configured correctly or there are connectivity issues, etc
$ok = $false
try
{
    write-host "Open CIS now..."
    $ok = $cis.Open()
}
Catch 
{
    $ok = $false
    write-host "Application.Open() failed:`n" + $_
}
if (!$ok)
{
    write-host "Could not open CIS - script cancelled"
    return
}

try
{
    $sess = $null
    try
    {
        write-host "Open CIS session now..."
        $sess = $cis.GetSession($userid, $password)
        if ($sess -eq $null -or !$sess.Open() -or !$sess.IsOpen)
            { throw }
    }
    catch
    {
        write-host "Session could not be opened:`n" + $_
        $sess = $null
    }

    if ($sess -ne $null)
    {
        # proceed with API interactions now...
        write-host "CIS Name: $($cis.DisplayName)"
        write-host "Home Folder: $($cis.HomeDir)"
        write-host "CIS version: $($cis.Version)"
        write-host ("FormulaProc version:  Date " + [string]$cis.FormulaProcDate + ", Revision " + [string]$cis.FormulaProcRevision)

        # retrieve the last accessed main inquiry cust/acct for our session user

        # if the last used custacct object exists, so we know the customer# and account#
        $custacct = $null
        $custacctinq = $null
        $custacct = [AdvancedUtility.Services.BusinessObjects.CustomerAccount]::GetLastUsedCustomerAccount($sess)

        if ($custacct -ne $null)

        {
            $custacctinq = new-object AdvancedUtility.Services.Inquiry.CustomerAccountInquiry -arg ($sess, $custacct)

            write-host ("Last accessed customer/account:  " + $custacct.Customer + "/" + $custacct.Account)
            write-host ("Customer: " + $custacct.Customer_Lookup.CustomerName())
            write-host ("Account:  " + $custacct.Account_Lookup.ServiceAddress)
            write-host ("Balance: " + $custacctinq.Balance())

        }
        else
        {
            write-host "No last accessed customer/account for your profile"
        }

        # clean up
        $custacct = $null
        $custacctinq = $null

    }
    else
    { write-host "Session could not be opened - check userid or password" }
}
catch
{
    # error occurred during API use...
    write-host "Error occurred:`n" + $_
}
finally
{
    # must make sure session and application are properly shut down
    if ($sess -ne $null)
    { [void] $sess.Close() }
    $sess = $null
    if ($cis -ne $null)
    { [void] $cis.Close() }
    $cis = $null
    write-host "Bye`n`n"
}