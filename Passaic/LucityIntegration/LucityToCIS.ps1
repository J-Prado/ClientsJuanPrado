
#SQL Configuration
#TESTSERVER: $global:sqlserver = 'lcsqlTest.polk-county.net'
#Production Script Location
#E:\LucityServer\Lucity\Lucity\Scripts\ps

#Test Script Location
#C:\Users\LucityU2\Desktop\PVWC Scripts\

#TEST Config
$Logfile = "E:\LucityServer\Lucity\Lucity\Scripts\ps\lucity2cis.log"
$LogfilePath = "E:\LucityServer\Lucity\Lucity\Scripts\ps"

$Logfile = "lucity2cis.log"
$LogfileRunTimeFileName = "lucity2cis_runtime.log"
$global:sqlserver = 'LUCITY'
$global:sqldatabase = 'Lucity'
$global:user = 'Lucity_USER'
$global:pw = 'IdaPE3ELtw'

$global:conn = new-object System.Data.SqlClient.SqlConnection("Data Source=$global:sqlserver;Initial Catalog=$global:sqldatabase;Uid=$global:user;Pwd=$global:pw;") 

$targetAPIRoot = "https://cisresttest.pvwc.com"
#CIS API Configuration
$global:credPair = "$('LUCITEST'):$('Passaic123')"
$global:cisUserAPIname = 'LUCITEST'

$global:cis_service_order_uri = $targetAPIRoot + '/data/serviceorder'
$global:cis_meter_reading_uri = $targetAPIRoot + '/data/informationonlyreading'
$global:cis_customer_account_uri = $targetAPIRoot + '/data/customeraccount'  
$global:cis_informational_meter_reading_uri = $targetAPIRoot + '/data/informationonlyreading'
$global:cis_meter_reading_uri = $targetAPIRoot + '/data/meterreading'
$global:cis_account_meter_uri = $targetAPIRoot + '/data/accountmeter'
$global:cis_water_meter_remote_uri = $targetAPIRoot + '/data/watermeterremote'
$global:cis_water_meter_uri = $targetAPIRoot + '/data/watermeter'
$global:cis_account_service_uri = $targetAPIRoot + '/data/accountservice'
$global:cis_account_meter_readtype_uri = $targetAPIRoot + '/data/accountmeterreadtype'

#Global Fields
$global:commentConcat
$global:progressConcat
$global:meterIdConcat
$global:serviceOrderID
$global:accountID
$global:customerID
$global:customerAccountID
$global:completionCode #PVWC Required.
$global:currentWOID
$global:currentArrayMeters
$global:currentWO_NUMBER
$global:existingSO
$global:existingSOComment
$global:serviceOrderAdded
$global:serviceOrderUpdated
$global:errorOnWorkOrder
$global:newMeterID
$global:newMeterRecNumber
$global:oldMeterRecNumber
$global:newMeterReading
$global:meterServiceNumber
$global:remoteReplaceMeterReading
$global:oldMeterID
$global:meterEndpointID
$global:oldMeterReading
$global:updateMeterID
$global:current_installed_meter
$global:currentMeterReading
$global:remoteID
$global:remoteType
$global:oldRemoteID
$global:serviceID
$global:IsCancelled
$global:isCompleted
$global:completedTimestamp
$global:runInformationalRead


$taskReplaceRemoteOnly = @('0022')
$taskReplaceMeter = @('CM', 'RMM', 'IME1', 'IME2', 'INC1', 'INC3', 'INC2', 'DRB')
$taskAddMeter = @('NSEP', 'NSE3', 'NS', 'NS3', 'NS2')
$taskPendingMeterReading = @('FRMC', 'FRSR', 'FRRO', 'SOFR')
$taskRemoveMeter = @('RCUT', 'RMV')
$taskReplaceUME = @('RUME') #NEW ITEM

#$taskMeterTurnOff = $('0097', '0098')


   
function CheckforFirstTime {
    $filePath = $LogfilePath + "\" + $LogfileRunTimeFileName
    $datetime = $(Get-Date -format 'yyyy-MM-dd HH:mm:ss')
    $stringToAdd = $datetime
    if (!(Test-Path $filePath)) {
        New-Item -path  $LogfilePath -name $LogfileRunTimeFileName -type "file" -Value "" -Force    
    } 

    $filePath = $LogfilePath + "\" + $Logfile
   
    $datetime = $(Get-Date -format 'yyyy-MM-dd HH:mm:ss')
    $stringToAdd = $datetime
    if (!(Test-Path $filePath)) {
        New-Item -path  $LogfilePath -name $Logfile -type "file"   
    }   

    
}

function ClearRunDT {
    $filePath = $LogfilePath + "\" + $LogfileRunTimeFileName
    $datetime = $(Get-Date -format 'yyyy-MM-dd HH:mm:ss')
    $stringToAdd = $datetime
    if ((Test-Path $filePath)) {
        New-Item -path  $LogfilePath -name $LogfileRunTimeFileName -type "file" -Value "" -Force    
    } 
}

function CurrentDateTimeWrite {
    
   
    $filePath = $LogfilePath + "\" + $LogfileRunTimeFileName
    $datetime = $(Get-Date -format 'yyyy-MM-dd HH:mm:ss')
    $stringToAdd = $datetime
    Add-Content $filePath -value $stringToAdd

   
}

function CheckToSeeLastRun {
    $safeToRun = $false
   
    $filePath = $LogfilePath + "\" + $LogfileRunTimeFileName

    $strLastRun = $null
    
    $fileSize = (Get-Item -Path $filePath).Length
    

   
    if ($fileSize -gt 25 ) {
        $safeToRun = $true
        ClearRunDT
    }

    if ($fileSize -lt 2 ) {
        $safeToRun = $true
        ClearRunDT
    }
    
    if ($safeToRun -eq $false) {
        $fileModDate = (Get-Item -Path $filePath).LastWriteTime
        #Check Start Time compare and restart new session if stalled for over 20 mins
        if (![string]::IsNullOrEmpty($fileModDate)) {
            $datetime = $(Get-Date -format 'yyyy-MM-dd HH:mm:ss')    
            $currentDateTime = [datetime]::ParseExact("$datetime", 'yyyy-MM-dd HH:mm:ss', $null)
            #$lastranDateTime = [datetime]::ParseExact("$fileModDate", 'yyyy-MM-dd HH:mm:ss', $null)
            $duration = $currentDateTime - $fileModDate
            if ($duration.TotalMinutes -gt 20) {
                #restart
                $strLog = "!!!!!Process Error Occured during run started at: " + $strLastRun + "process restarted."
                LogWrite $strLog
                $safeToRun = $true
                ClearRunDT
            }

        }
        

    }


    return $safeToRun
}


function StartProcess {
    
    Write-Host "START!"
   
    Clear-Host
    CheckforFirstTime
    $safeToRun = CheckToSeeLastRun

   
    if ($safeToRun -eq $true) {
        CurrentDateTimeWrite
        $global:serviceOrderAdded = 0
        $global:serviceOrderUpdated = 0
       

        $datetime = $(Get-Date -format 'yyyy-MM-dd HH:mm')
        $tempdatetime = (Get-Date).AddDays(-1)
        $datetimeminus1day = $tempdatetime.ToString('yyyy-MM-dd HH:mm') 
    

        #Check for new Work orders
        $sqlquery = 'Select * From WKORDER WHERE WO_CAT_CD = ' + "'" + 'WTDT' + "' and WO_MOD_DT >= '" + $datetimeminus1day + "'" #PVWC uses WTDT for the category
        #$sqlquery = 'Select * From WKORDER WHERE WO_CAT_CD = ' + "'" + 'WTDT' + "' Order BY WO_ID DESC"
        #TESTING
        #$sqlquery = 'Select * From WKORDER WHERE WO_ID = 1460'# AND WO_EXTERNALID IS NULL and WO_CAT_CD = ' + "'" + 'W-MT' + "'"
        #$sqlquery = 'Select * From WKORDER WHERE WO_ACTN_CD = ' + "'" + '0021'  + "'"
        #$sqlquery = 'Select * From WKORDER WHERE WO_ID = 3204 AND WO_EXTERNALID IS NULL'
        ProcessWorkOrders ($sqlquery)   
        if (($global:serviceOrderAdded -gt 0) -OR ($global:serviceOrderUpdated -gt 0)) {
            $strLog = "End Process - Service Orders Added:" + $global:serviceOrderAdded.ToString() + " Service Orders Updated:" + $global:serviceOrderUpdated.ToString()
            LogWrite $strLog
        }
        CurrentDateTimeWrite
    }
}




function ProcessWorkOrders ($sqlquery) {
  
    #Starts the process, looks for Work Orders in Lucity where the field WO_EXTERNALID IS NULL and the WO_CAT_CD is equal to 'W-MT'
   
    $datetime = $(Get-Date -format 'yyyy-MM-dd HH:mm')
    $tempdatetime = (Get-Date).AddMinutes(-21)
    $datetimeminus21minutes = $tempdatetime.ToString('yyyy-MM-dd HH:mm') 

    
    $command = new-object system.data.sqlclient.sqlcommand($sqlquery, $conn)
    $conn.Open()
  
    $adapter = New-Object System.Data.sqlclient.sqlDataAdapter $command
    $dataset = New-Object System.Data.DataSet
    $adapter.Fill($dataset) | Out-Null
    $global:conn.Close()
    $workOrders = $dataset.tables[0]

    foreach ($workOrder in $workOrders) {
        
        resetGlobals
        
        if (![string]::IsNullOrEmpty($workOrder.WO_STAT_CD)) {

            if ($workOrder.WO_STAT_CD -eq "951") {
                $global:IsCancelled = "true"
            }
            if ($workOrder.WO_STAT_CD -eq "999") {
                $global:isCompleted = "true"
                $global:completedTimestamp = $datetime
            }
        }        
       
        $global:currentWOID = $workOrder.WO_ID
        $global:currentWO_NUMBER = $workOrder.WO_NUMBER

        #Create Date From WO_MOD_DT and WO_MOD_TM        
        $WODT = $workOrder.WO_MOD_DT.ToString('yyyy-MM-dd') 
        $WOTM = $workOrder.WO_MOD_TM.ToString('HH:mm') 
        $WODTTM = $WODT + " " + $WOTM
       

        $ProcessWorkOrder = 0
        if ($WODTTM -gt $datetimeminus21minutes) {
            $ProcessWorkOrder = 1        
        }

        #$ProcessWorkOrder = 1  

        if ($ProcessWorkOrder -eq 1) {
            
            BuildProgressString($workorder)
            $workOrder
            BuildCommentString($workOrder)

            $global:runInformationalRead = "true" 
            if ([string]::IsNullOrEmpty($workOrder.WO_EXTERNALID)) {
                if (![string]::IsNullOrEmpty($workorder.WO_ACTN_CD)) {
                    $array_meters = ProcessMeterItems($workOrder)  
                   
                    if (![string]::IsNullOrEmpty($global:accountID)) {
                        $new_service_order = CreateServiceOrderJSON($workOrder) 
                       
                        Write-Host "NEW - WO_ID: '$($workOrder.WO_ID)'" + " - WO_MOD_DT: '$($workOrder.WO_MOD_DT)'" + " - WO_EXTERNALID: '$($workOrder.WO_EXTERNALID)'" + " - WO_ACCOUNT: '$($workOrder.WO_ACCOUNT)'" + " -WO_BCUSTNO: '$($workOrder.WO_BCUSTNO)'" + + " - WO_ACCOUNT: '$($workorder.WO_ACCOUNT)'" " - WO_STAT_T: '$($workorder.WO_STAT_T)'"
                        #Step 5: Use Service Order JSON and POST record via API
                        $service_order_id = AddServiceOrder($new_service_order)
                        #Step 6: With the new Service Order id Created add it back to the Work Order in WO_EXTERNALID field 
                        if (![string]::IsNullOrEmpty($global:serviceOrderID)) {
                            UpdateWorkOrder($global:serviceOrderID)
                            #Check all Tasks for Replacement or Install Tasks
                            CheckForInstallReplaceTasks($workOrder)
                            if ($global:runInformationalRead -eq "true") {
                                ProcessInformationalReadingData
                            }
                        }
                    }
                }

                #********PROCESS NEW WORK ORDER*********
           
                
            
       
            }
            else {

                #********PROCESS Existing WORK ORDER*********

                $global:serviceOrderID = $workOrder.WO_EXTERNALID
                $global:existingSOComment = GetExistingServiceOrder($workOrder.WO_EXTERNALID)
                if (![string]::IsNullOrEmpty($global:serviceOrderID)) {
                    #STEP 2: Builds a comment string from Lucity's comments for current work order
                  
            
                    $array_meters = ProcessMeterItems($workOrder)    
                    if (![string]::IsNullOrEmpty($global:accountID)) {
                    

                        UpdateServiceOrder($global:serviceOrderID)
                        CheckForInstallReplaceTasks($workOrder)
                        if ($global:runInformationalRead -eq "true") {
                            ProcessInformationalReadingData
                        }
                        if (![string]::IsNullOrEmpty($global:current_installed_meter)) {
                            
                            if (![string]::IsNullOrEmpty($global:remoteID)) {
                                
                                UpdateRemoteIDPVWC($global:remoteID)
                           
                            }
                        }
                    }
                }
       
            }
        }

    }

    Write-Host "Done!"

}

function resetGlobals() {

    $global:errorOnWorkOrder = "false"       
    $global:existingSO = $null
    $global:IsCancelled = "false"
    $global:existingSOComment = $null
    $global:customerID = $null
    $global:customerAccountID = $null
    $global:accountID = $null
    $global:serviceOrderID = $null 
    $global:currentArrayMeters = $null
    $global:newMeterID = $null 
    $global:newMeterReading = $null
    $global:meterEndpointID = $null       
    $global:oldMeterID = $null    
    $global:remoteReplaceMeterReading = $null
    $global:meterServiceNumber = $null
    $global:oldMeterReading = $null
    $global:updateMeterID = $null        
    $global:remoteID = $null
    $global:oldRemoteID = $null
    $global:newMeterRecNumber = $null
    $global:oldMeterRecNumber = $null
    $global:remoteType = $null
    $global:current_installed_meter = $null
    $global:currentMeterReading = $null
    $global:isCompleted = $null
    $global:completedTimestamp = $null   
    $global:oldMeterEndpointID = $null
    $global:progressConcat = $null
    $global:commentConcat = $null

}

function BuildProgressString($workorder) {
    $sqlquery = 'Select * From WKWOTRAK WHERE WK_WO_ID = ' + $workorder.WO_ID
    
   
    $command = new-object system.data.sqlclient.sqlcommand($sqlquery, $global:conn)

    $global:conn.Open()
        
    $adapter = New-Object System.Data.sqlclient.sqlDataAdapter $command
    $dataset = New-Object System.Data.DataSet
    $adapter.Fill($dataset) | Out-Null
    $global:conn.Close()
    $progressNotes = $dataset.tables[0]
    foreach ($progressNote in $progressNotes) {
        #Create Date From WO_MOD_DT and WO_MOD_TM        
        $WODT = $progressNote.WK_DT.ToString('M/d/yyyy') 
        $WOTM = $progressNote.WK_TM.ToString('hh:mm tt') 
        $WODTTM = $WODT + " " + $WOTM  
        $currentdatetime = "[" + $WODTTM + "]" 
        if (![string]::IsNullOrEmpty($progressNote.WK_DESC)) {
            #$strComment = $currentdatetime + $progressNote.WK_DESC #PVWC CHANGE
            $strComment = $currentdatetime + $progressNote.WK_TRACK + ' ' + $progressNote.WK_DESC
            checkProgressNote($strComment)
        }
    }
    
    


}

function BuildCommentString($workorder) {

    $sqlquery = 'Select * From WKGDMEMO WHERE GM_PARENT = ' + "'" + 'WKORDER' + "'" + ' and GM_PAR_ID = ' + $workorder.WO_ID
    
    #checkComment("TEST") 

    if (![string]::IsNullOrEmpty($workorder.WO_DOC_FLG)) {
     
        if ($workorder.WO_DOC_FLG -eq $true) {
            $strComment = "File Attached to Lucity Work Order"
            checkComment($strComment)       
        }
    }   

    if (![string]::IsNullOrEmpty($workorder.WO_USER15)) {
        $strComment = "Supervisor Comment: " + $workorder.WO_USER15
        checkComment($strComment)       
    }   

    
    
    $command = new-object system.data.sqlclient.sqlcommand($sqlquery, $global:conn)

    $global:conn.Open()
        
    $adapter = New-Object System.Data.sqlclient.sqlDataAdapter $command
    $dataset = New-Object System.Data.DataSet
    $adapter.Fill($dataset) | Out-Null
    $global:conn.Close()
    $comments = $dataset.tables[0]
    foreach ($comment in $comments) {
        if (![string]::IsNullOrEmpty($comment.GM_MEMO)) {
            $strComment = "Grid Comment: " + $comment.GM_MEMO
            checkComment($strComment)
        }
    }

    $sqlquery = "Select * From WKMEMO WHERE CO_REC_ID = " + $workorder.WO_ID + " and CO_FIELD = 'WO_MEMO1'"    
   
    Write-Host $sqlquery
    $command = new-object system.data.sqlclient.sqlcommand($sqlquery, $global:conn)

    $global:conn.Open()
    
    $adapter = New-Object System.Data.sqlclient.sqlDataAdapter $command
    $dataset = New-Object System.Data.DataSet
    $adapter.Fill($dataset) | Out-Null
    $global:conn.Close()
    $comments2 = $dataset.tables[0]

    foreach ($comment1 in $comments2) {
        if (![string]::IsNullOrEmpty($comment1.CO_TEXT)) {
            $strComment = "Requestor Comment: " + $comment1.CO_TEXT
            checkComment($strComment)             
        }
    }

   
    $sqlquery = "Select * From WKMEMO WHERE CO_REC_ID = " + $workorder.WO_ID + " and CO_FIELD = 'WO_MEMO2'"
   
    
    $command = new-object system.data.sqlclient.sqlcommand($sqlquery, $global:conn)

    $global:conn.Open()

    
    $adapter = New-Object System.Data.sqlclient.sqlDataAdapter $command
    $dataset = New-Object System.Data.DataSet
    $adapter.Fill($dataset) | Out-Null
    $global:conn.Close()
    $comments2 = $dataset.tables[0]
    foreach ($comment1 in $comments2) {
        if (![string]::IsNullOrEmpty($comment1.CO_TEXT)) {
            $strComment = "Crew Comment: " + $comment1.CO_TEXT
            checkComment($strComment)           
        }
    }

    $sqlquery = 'Select * From WKWOTSK where WT_WO_ID = ' + $workOrder.WO_ID

    $command = new-object system.data.sqlclient.sqlcommand($sqlquery, $conn)
    $conn.Open()
  
    $adapter = New-Object System.Data.sqlclient.sqlDataAdapter $command
    $dataset = New-Object System.Data.DataSet
    $adapter.Fill($dataset) | Out-Null
    $global:conn.Close()
    $WKWOTSKItems = $dataset.tables[0]
        
    foreach ($WKWOTSKItem in $WKWOTSKItems) {
        if (![string]::IsNullOrEmpty($WKWOTSKItem.WT_COMP_CD)) {
            $global:completionCode = $WKWOTSKItem.WT_COMP_CD;
        }
        
        if (![string]::IsNullOrEmpty($WKWOTSKItem.WT_USER14TY)) {
            $strComment = "Line Flushed: " + $WKWOTSKItem.WT_USER14TY
            checkComment($strComment) 
        }
        if (![string]::IsNullOrEmpty($WKWOTSKItem.WT_USER15TY)) {
            $strComment = "Arrival Status: " + $WKWOTSKItem.WT_USER15TY
            checkComment($strComment) 
        }
        if (![string]::IsNullOrEmpty($WKWOTSKItem.WT_USER16TY)) {
            $strComment = "Departure Status: " + $WKWOTSKItem.WT_USER16TY
            checkComment($strComment) 
        }
        if (![string]::IsNullOrEmpty($WKWOTSKItem.WT_USER17TY)) {
            $strComment = "Replace Reason: " + $WKWOTSKItem.WT_USER17TY
            checkComment($strComment) 
        }
        if (![string]::IsNullOrEmpty($WKWOTSKItem.WT_USER18TY)) {
            $strComment = "Was a leak repaired?: " + $WKWOTSKItem.WT_USER18TY
            checkComment($strComment) 
        }       
      
    }

    

   

}

function checkProgressNote {
    
    Param ([string]$commentToAdd)

    $blnAddComment = $false
    $backslash = "\"

   
    if (![string]::IsNullOrEmpty($global:progressConcat)) {
        $blnAddComment = ($commentToAdd.ToUpper() | % { $global:progressConcat.Contains($_) }) -contains $true
    }

    if ($blnAddComment -eq $false) {
        if (![string]::IsNullOrEmpty($global:progressConcat)) {
            $global:progressConcat = $global:progressConcat + $backslash + "r" + $backslash + "n" + $commentToAdd.ToUpper()
        }
        else {
            $global:progressConcat = $commentToAdd.ToUpper()
        }
    }

}


function checkComment {
    
    Param ([string]$commentToAdd)

    $blnAddComment = $false
    $backslash = "\"

    #Remove bad characters
    $commentToAdd = $commentToAdd -replace ('"', " ")
    $currentdatetime = "[" + $(Get-Date -format 'M/d/yyyy hh:mm tt') + "]"
    if (![string]::IsNullOrEmpty($global:commentConcat)) {
        $blnAddComment = ($commentToAdd.ToUpper() | % { $global:commentConcat.Contains($_) }) -contains $true
    }

    if ($blnAddComment -eq $false) {
        if (![string]::IsNullOrEmpty($global:commentConcat)) {
            $global:commentConcat = $global:commentConcat + $backslash + "r" + $backslash + "n" + $commentToAdd.ToUpper()
        }
        else {
            $global:commentConcat = $commentToAdd.ToUpper()
        }
    }

    


}




function ProcessMeterItems($workorder) {
   
    [PsObject[]]$array_meters = @()
    $sqlquery = 'Select * From WKWOASSET WHERE AS_WO_ID = ' + $workorder.WO_ID
    
    $command = new-object system.data.sqlclient.sqlcommand($sqlquery, $global:conn)

    $global:conn.Open()

  
    $adapter = New-Object System.Data.sqlclient.sqlDataAdapter $command
    $dataset = New-Object System.Data.DataSet
    $adapter.Fill($dataset) | Out-Null
    $global:conn.Close()
    $meters = $dataset.tables[0]
    $global:meterIdConcat = ""
    $global:oldMeterID = $null
    $global:newMeterID = $null
    $global:meterServiceNumber = $null
    $global:oldMeterReading = $null
    $global:current_installed_meter
    $global:newMeterReading = $null
    $global:newMeterRecNumber = $null
    $global:oldMeterRecNumber = $null
    $global:meterEndpointID = $null
    $global:remoteID = $null
    $global:currentMeterReading = $null
    foreach ($meter in $meters) {
      
        $mt_id = $null
        $current_read_1 = $null
        $current_read_2 = $null
        $current_read_3 = $null
        $mt_number = $null

        $mt_id = $meter.AS_INV_ID
        $current_read_1 = $meter.AS_NEW_RD1
        $current_read_2 = $meter.AS_NEW_RD2
        $current_read_3 = $meter.AS_NEW_RD3
        $global:newMeterReading = $meter.AS_NEW_RD1
        $global:currentMeterReading = $meter.AS_NEW_RD1
        
        if (![string]::IsNullOrEmpty($meter.AS_NEWMTR)) {
            $global:newMeterID = $meter.AS_NEWMTR 
            $current_installed_meter = $meter.AS_NEWMTR 
            $global:current_installed_meter = $meter.AS_NEWMTR 
            $global:newMeterReading = $current_read_1          
        }
      
        if (![string]::IsNullOrEmpty($meter.AS_MTRDEV)) {
            $global:oldMeterID = $meter.AS_MTRDEV 
            $global:oldMeterReading = $meter.AS_CUR_RD1

        } 
        if ([string]::IsNullOrEmpty($global:oldMeterReading)) {
            
            $global:oldMeterReading = $meter.AS_CUR_RD1

        } 
        if (![string]::IsNullOrEmpty($meter.AS_USR19)) {
            $global:current_installed_meter = $meter.AS_USR19
        
            $current_installed_meter = $meter.AS_USR19
        }
        if (![string]::IsNullOrEmpty($meter.AS_USR20)) {
            $global:remoteID = $meter.AS_USR20        
           
        }
        
        if ($mt_id) {
            $sqlquery = 'Select * From WTMETER WHERE MT_ID = ' + $mt_id
    
            $command = new-object system.data.sqlclient.sqlcommand($sqlquery, $global:conn)

            $global:conn.Open()
         
            $adapter = New-Object System.Data.sqlclient.sqlDataAdapter $command
            $dataset = New-Object System.Data.DataSet
            $adapter.Fill($dataset) | Out-Null
            $global:conn.Close()
            $mt_items = $dataset.tables[0]
            $dataset.tables[0]
            foreach ($mt_item in $mt_items) {
                $test = $mt_item.MT_AMR_ID
                $tes2t = $mt_item.MT_AMR_NUMBER
                $global:accountID = $mt_item.MT_ACCTNO
                $meterServiceNumber = $mt_item.MT_TYPE_CD
                $global:meterServiceNumber = $meterServiceNumber
                if (![string]::IsNullOrEmpty($mt_item.MT_NUMBER)) {
                    $global:meterEndpointID = $mt_item.MT_NUMBER 
                }
                
                $global:newMeterRecNumber = $mt_item.MT_MD_ID
                $mt_number = $mt_item.MT_ACCTNO   #MT_NUMBER
            }
        }


        $array_meters += [PsObject]@{ mt_id = $mt_id; current_read_1 = $current_read_1; current_read_2 = $current_read_2; current_read_3 = $current_read_3; mt_number = $mt_number; current_installed_meter = $global:current_installed_meter; meterServiceNumber = $global:meterServiceNumber; notes = "" }
       
    }
    $test = $global:oldMeterReading
    $meterIdConcat = '';

    if (![string]::IsNullOrEmpty($global:accountID)) {
        GetAccountInfo($global:accountID)

    }
    if ([string]::IsNullOrEmpty($global:customerID)) {
        $global:accountID = $null
        
    }

    if (![string]::IsNullOrEmpty($global:accountID)) {
        if ($array_meters.Length -gt 0) {

            $global:currentArrayMeters = $array_meters

            foreach ($meter in $array_meters) {
       
                if (![string]::IsNullOrEmpty($meter.current_installed_meter)) {
                    if (![string]::IsNullOrEmpty($meterIdConcat)) {            
                        $meterIdConcat = $meterIdConcat + $meter.current_installed_meter + ',' + $global:meterServiceNumber + 'WT;'
                
                    }
                    else {
                        $meterIdConcat = $meter.current_installed_meter + ',' + $global:meterServiceNumber + 'WT;'
                    }
               
                }
          
            }
       
        }    
   
        
        $global:meterIdConcat = $meterIdConcat
    }
    
}

function ProcessInformationalReadingData() {
    if ($global:currentArrayMeters.Length -gt 0) {
        foreach ($meterReadingItem in $global:currentArrayMeters) {
       
            [Int32]$outNumber = $null
            if (![string]::IsNullOrEmpty($meterReadingItem.current_read_1)) {
                if ([Int32]::TryParse($meterReadingItem.current_read_1, [ref]$outNumber)) { 
                    $meterReadingItem.notes = "Current Reading 1"
                        
                    CreateInfomationalMeterReadingJSON($meterReadingItem)
                }
                else {
                    #LogWrite "Current Reading 1 has a invalid value:" + $meterReadingItem.current_read_1
                }
            }
            if (![string]::IsNullOrEmpty($meterReadingItem.current_read_2)) {
                if ([Int32]::TryParse($meterReadingItem.current_read_2, [ref]$outNumber)) { 
                    $meterReadingItem.current_read_1 = $meterReadingItem.current_read_2
                    $meterReadingItem.notes = "Current Reading 2"
                        
                    CreateInfomationalMeterReadingJSON($meterReadingItem)
                }
                else {
                    #LogWrite "Current Reading 2 has a invalid value:" + $meterReadingItem.current_read_2
                }
            }
            if (![string]::IsNullOrEmpty($meterReadingItem.current_read_3)) {
                if ([Int32]::TryParse($meterReadingItem.current_read_3, [ref]$outNumber)) {
                    $meterReadingItem.current_read_1 = $meterReadingItem.current_read_3
                       
                    $meterReadingItem.notes = "Current Reading 3"
                    CreateInfomationalMeterReadingJSON($meterReadingItem)
                }
                else {
                    #LogWrite "Current Reading 3 has a invalid value:" + $meterReadingItem.current_read_3
                }
            }
        }
    }


}

function GetAccountInfo($accountID) {

    $foundAccount = $false
    if (![string]::IsNullOrEmpty($accountID)) {
         
    
        $encodedCredentials = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($global:credPair))
 
        # Form the header and add the Authorization attribute to it
        $headers = @{ Authorization = "Basic $encodedCredentials" }
    
        $body = @{
            where = "account eq " + $accountID
   
        } 

        try {
            $r = Invoke-WebRequest -Uri $global:cis_customer_account_uri -Method GET -Headers $headers -ContentType "application/json; charset=utf-8" -Body($body)
       

            $result = ConvertFrom-Json -InputObject $r.Content

            foreach ($account in $result._embedded.customeraccount) {
                $global:customerID = $account.customer
                $global:customerAccountID = $account.customerAccountId
                $foundAccount = $true 
            }
       
        }
        catch {
            $strLog = "Cannot Find Account Number: " + $accountID
            LogWrite $strLog
       
        }
      
    }
    else {
        $strLog = "Invalid Account ID"
        LogWrite $strLog
    }

    
}


function CreateInfomationalMeterReadingJSON($meter) {
    
    $datetime = $(Get-Date -format 'yyyy-MM-ddTHH:MM:ss')
   
    if (![string]::IsNullOrEmpty($meter.current_read_1)) {   
        if ($meter.current_read_1 -gt 0) {
            
            if ($global:serviceOrderID -ne $null) {
                $serviceID = GetServiceID
                $readTypeID = GetMeterReadTypeID($serviceID)   
                $MyJsonHashTable = @{
  
                    'account'           = $global:accountID
                    'meter'             = $meter.current_installed_meter 
                    'currentRead'       = $meter.current_read_1
                    'customer'          = $global:customerID
                    'customerAccountId' = $global:customerAccountID                  
                    'readDate'          = $datetime
                    'readType'          = "WT"
                    'readTypeId'        = $readTypeID
                    'status'            = "InformationOnlyRead" #PVWC Required Field
                    'serviceId'         = $serviceID
                    'serviceOrder'      = $global:serviceOrderID
                    'service'           = $meter.meterServiceNumber
                    'notes'             = $meter.notes #"Created from Lucity"  
                    'readStatus'        = "RE"
                    'units'             = "GA"       
           
                }       
       
                $isMeterActive = CheckForActiveMeter($meter.current_installed_meter)
                
                if ($isMeterActive -eq $true) {
                   

                    # Encode the pair to Base64 string
                    $encodedCredentials = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($global:credPair))
 
                    # Form the header and add the Authorization attribute to it
                    $headers = @{ Authorization = "Basic $encodedCredentials" }  
                    try {
                       
                        $body = @{
                            where = "meter eq " + $MyJsonHashTable.meter
   
                        } 


                        $r = Invoke-WebRequest -Uri $global:cis_meter_reading_uri -Method GET -Headers $headers -ContentType "application/json; charset=utf-8" -Body($body)
       

                        $result = ConvertFrom-Json -InputObject $r.Content
                        $blnFound = $false
      
                        foreach ($meterReading in $result._embedded.meterreading) { 
                            
                            $MyJsonHashTable.units = $meterReading.units
                            break                              
                        }


                        $body = @{
                            where = "serviceOrder eq '" + $global:serviceOrderID + "'"
                            #where = "meter eq 12205144"
                        } 

                        $r = Invoke-WebRequest -Uri $global:cis_informational_meter_reading_uri -Method GET -Headers $headers -ContentType "application/json; charset=utf-8" -Body($body)
       

                        $result = ConvertFrom-Json -InputObject $r.Content
                        $blnFound = "false"
      
                        foreach ($meterReading in $result._embedded.informationonlyreading) {
                        
                            $blnFound = $true
                            if ($meterReading.currentRead -ne $meter.current_read_1) {
                                $blnFound = $false;
                            }
                         
                                   
                        }
                        $MyJsonVariable = $MyJsonHashTable | ConvertTo-Json
      
                        if ($blnFound -eq $false) {
                            try { 
                                #if ($global:currentWOID = 361) {
                                #     $test = 1
                                #}
       
                                $r = Invoke-WebRequest -Uri $global:cis_informational_meter_reading_uri -Method POST -Headers $headers -ContentType "application/json; charset=utf-8"  -Body ($MyJsonVariable)        
                                $result = ConvertFrom-Json -InputObject $r.Content
                                $meter.callNumber = $result.readingId
                            
                                checkComment($result.meter + " - " + $result.notes + " : " + $result.currentRead)
                            
                                UpdateMeterID($meter)
                           
                            }
                            catch {
                                #throw $_
                                $strLog = $_ #"Unable to create meter reading via the api for: " + $meter.notes
                                LogWrite $strLog
                            }
                        }
      
        
                    }
                    catch {
                  
                        $strLog = $_#"Unable to query meter reading via the api for: " + $meter.notes
                        LogWrite $strLog
                   
                    }
                }
            }
            else {
                $strLog = "Missing Service Order Number when creating a meter reading for: " + $meter.notes
                LogWrite $strLog
            }
        }
        else {
            $strLog = "2. Invalid Meter for: " + $meter.notes
            LogWrite $strLog
        }
    }
    else {
        $strLog = "1. Invalid Meter for: " + $meter.notes
        LogWrite $strLog
    }
   
}

function CreateMeterReadingJSON() {


   
    $success = $false
    $datetime = $(Get-Date -format 'yyyy-MM-ddTHH:MM:ss')
    $serviceID = GetServiceID
    $readTypeID = GetMeterReadTypeID($serviceID)
    $MeterReadinginfo = @{
  
        'account'      = $global:accountID
        'meter'        = $global:current_installed_meter 
        'reading'      = $global:currentMeterReading
        #'customerAccountId' = $global:customerAccountID
        #'status'= "InformationOnlyRead" 
        'customer'     = $global:customerID                                                      
        'readingDate'  = $datetime
        'readType'     = "WT"
        'readTypeId'   = $readTypeID#241358
        'serviceOrder' = $global:serviceOrderID
        'service'      = 30#$global:meterServiceNumber
        'serviceID'    = $serviceID
        'notes'        = "Meter Reading from Lucity" 
        'billType'     = 'RegularBill' 
        'readStatus'   = "RE"
        'units'        = "CF"       
           
    }     
   
    if (![string]::IsNullOrEmpty($MeterReadinginfo)) {          
            
        if ($global:serviceOrderID -ne $null) {                   
       

            #$MyJsonVariable = $MeterReadinginfo | ConvertTo-Json

            # Encode the pair to Base64 string
            $encodedCredentials = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($global:credPair))
 
            # Form the header and add the Authorization attribute to it
            $headers = @{ Authorization = "Basic $encodedCredentials" }  
            try {
                    
                $body = @{
                    where = "meter eq " + $MeterReadinginfo.meter
   
                } 

                $r = Invoke-WebRequest -Uri $global:cis_meter_reading_uri -Method GET -Headers $headers -ContentType "application/json; charset=utf-8" -Body($body)
       

                $result = ConvertFrom-Json -InputObject $r.Content
                $blnFound = $false
      
                foreach ($meterReading in $result._embedded.meterreading) { 
                            
                    $MeterReadinginfo.units = $meterReading.units
                    break                              
                }


                $body = @{
                    where = "serviceOrder eq " + $global:serviceOrderID
   
                } 

                $r = Invoke-WebRequest -Uri $global:cis_meter_reading_uri -Method GET -Headers $headers -ContentType "application/json; charset=utf-8" -Body($body)
       

                $result = ConvertFrom-Json -InputObject $r.Content
                $blnFound = $false
      
                foreach ($meterReading in $result._embedded.meterreading) {
                    if ($MeterReadinginfo.meter -eq $meterReading.meter) {
                        $blnFound = $true
                        $MeterReadinginfo.units = $meterReading.units
                        if ($meterReading.reading -ne $MeterReadinginfo.reading) {
                            $blnFound = $false;
                        }
                        else {
                            $success = $true;
                        }
                         
                    }            
                }
                $MyJsonVariable = $MeterReadinginfo | ConvertTo-Json
                write-host $MyJsonVariable
      
                if ($blnFound -eq $false) {
                    try {                           
       
                        $r = Invoke-WebRequest -Uri $global:cis_meter_reading_uri -Method POST -Headers $headers -ContentType "application/json; charset=utf-8"  -Body ($MyJsonVariable)        
                        $result = ConvertFrom-Json -InputObject $r.Content
                        $success = $true;
                           
                    }
                    catch {
                        #throw $_
                        $strLog = $_ #"Unable to create meter reading via the api for: " + $global:current_installed_meter 
                        LogWrite $strLog
                    }
                }
      
        
            }
            catch {
                  
                $strLog = $_#"Unable to query meter reading via the api for: " + $meter.notes
                LogWrite $strLog
                   
            }
        }
        else {
            $strLog = "Missing Service Order Number when creating a meter reading for: " + $MeterReadinginfo.meter
            LogWrite $strLog
        }
        
    }
    else {
        $strLog = "Invalid Meter Reading element."
        LogWrite $strLog
    }

    return $success
   
}


function UpdateMeterID($meter) {

  
   
    $command = new-object system.data.sqlclient.sqlcommand
    $command.Connection = $global:conn
   
    if (![string]::IsNullOrEmpty($meter.callNumber)) {
        switch ($meter.notes) {
            "Current Reading 1" {
                $command.CommandText = 'Update  WKWOASSET Set AS_USR11 = ' + $meter.callNumber + ' WHERE AS_WO_ID = ' + $global:currentWOID
            }
            "Current Reading 2" {
                $command.CommandText = 'Update  WKWOASSET Set AS_USR12 = ' + $meter.callNumber + ' WHERE AS_WO_ID = ' + $global:currentWOID
            }
            "Current Reading 3" {
                $command.CommandText = 'Update  WKWOASSET Set AS_USR13 = ' + $meter.callNumber + ' WHERE AS_WO_ID = ' + $global:currentWOID
            }

        }
 
        $global:conn.Open()

        $command.ExecuteNonQuery()   
        $global:conn.Close()
    }
  


}




function CreateServiceOrderJSON($workorder) {
     
    #Converts Work Order Data to CIS Service Order JSON Object, this object is then used add ServiceOrder record via CIS API

    #TEST ACCOUNT: 123974 AND CUSTOMER: 054259 and customerAccountId: 115676  


    $datetime = $(Get-Date -format 'yyyy-MM-ddTHH:MM:ss')
     
     
    $dateToPrint = $workOrder.WO_SSTR_DT.ToString('yyyy-MM-ddTHH:MM:ss') #PVWC tracks Scheduled Task
    #$timeToPrint = $workOrder.WO_MOD_TM.ToString('HH:mm') 
    #$combinedToPrint = $($dateToPrint + " " + $timeToPrint
       
    $MyJsonHashTable = @{
  
              
        'dateCreated'          = $datetime
        'dateToPrint'          = $dateToPrint #'2023-09-08T03:03:00'#$datetime #PVWC tracks Scheduled Task
        'equipmentNumbers'     = ""              
        'soCustom1'            = "Created from Lucity"                          
        'meterNumbers'         = $global:meterIdConcat              
        'workOrderKey'         = $workorder.WO_ID
        'isExternalProcessed'  = "false"
        'createdByUserId'      = $global:cisUserAPIname
        'serviceRequestId'     = $global:currentWO_NUMBER  
        'serviceOrderType'     = $workorder.WO_ACTN_CD 
        'isCompleted'          = $global:isCompleted
        'completedTimestamp'   = $global:completedTimestamp
        #'assignedToUserId' = $workorder.WO_INISTAF
        'assignedToDepartment' = 'MC'
        'laborHours'           = $workorder.WO_LH_EST     
        'status'               = $workorder.WO_STAT_TY
        'afterHours'           = 'NA'
        'payType'              = 'NA'
             
    }


    if (![string]::IsNullOrEmpty($global:progressConcat)) {
        $MyJsonHashTable.progressNotes = $global:progressConcat
    }
    if (![string]::IsNullOrEmpty($global:commentConcat)) {
        $MyJsonHashTable.completedNotes = $global:commentConcat  
    }
    if (![string]::IsNullOrEmpty($workorder.WO_ACTN_TY)) {
        $MyJsonHashTable.serviceMessage = $workorder.WO_ACTN_TY + " Lead Worker:" + $workorder.WO_EMP_TY #Might be another text field we can use
    }              

    if (![string]::IsNullOrEmpty($global:customerID)) {
        $MyJsonHashTable.customer = $global:customerID
        $MyJsonHashTable.customerAccountId = $global:customerAccountID  
    }
        
    if (![string]::IsNullOrEmpty($global:accountID)) {
        $MyJsonHashTable.account = $global:accountID
    }

    $MyJsonVariable = $MyJsonHashTable | ConvertTo-Json | % { [Regex]::Unescape($_) }

    $err = $null
    $MyJsonObject = [Microsoft.PowerShell.Commands.JsonObject]::ConvertFromJson($MyJsonVariable, [ref]$err)

    return $MyJsonVariable

}

function AddServiceOrder ($serviceorder) {

    #Step 5: POST json object to Service Order API  
  
    $newServiceOrder = $null
    # Encode the pair to Base64 string
    $encodedCredentials = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($global:credPair))
 
    # Form the header and add the Authorization attribute to it
    $headers = @{ Authorization = "Basic $encodedCredentials" }
  
    try {
    
        $r = Invoke-WebRequest -Uri $global:cis_service_order_uri -Method POST -Headers $headers -ContentType "application/json; charset=utf-8"  -Body ($serviceorder) 

        $result = ConvertFrom-Json -InputObject $r.Content
        $global:serviceOrderID = $result.serviceOrderNumber
        $global:serviceOrderAdded = $global:serviceOrderAdded + 1
        $newServiceOrder = $result
        
    }
    catch {
        #throw $_
        $strLog = $_#"Unable to create Service Order for Work Order: " + $global:currentWO_NUMBER
        LogWrite $strLog
    }

    return  $newServiceOrder
}

function GetExistingServiceOrder ($serviceorderid) {
    # Encode the pair to Base64 string
    $encodedCredentials = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($global:credPair))
 
    # Form the header and add the Authorization attribute to it
    $headers = @{ Authorization = "Basic $encodedCredentials" }
    $existingComment = ""
    
    if (![string]::IsNullOrEmpty($serviceorderid)) {
         
        try {
       
       
            $newURL = $global:cis_service_order_uri + '/' + $serviceorderid
   
            $r = Invoke-WebRequest -Uri $newURL -Method GET -Headers $headers -ContentType "application/json; charset=utf-8"

            $result = ConvertFrom-Json -InputObject $r.Content
        
            $existingServiceOrder = $result
            $global:accountID = $existingServiceOrder.account
            $existingComment = $existingServiceOrder.completedNotes
           
       
        }
        catch {            
            #throw $_
            $strLog = "Service Order " + $global:serviceOrderID + " no longer exist in CIS"
            $global:serviceOrderID = $null
            LogWrite $strLog
          
        }
      
    }
    else {
        $strLog = "Invalid Service Order Number in GetExistingServiceOrder function"
        LogWrite $strLog
    }   

    return $existingComment

   
}



function UpdateServiceOrder ($serviceorderid) {

  
    # Encode the pair to Base64 string
    $encodedCredentials = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($global:credPair))
 
    # Form the header and add the Authorization attribute to it
    $headers = @{ Authorization = "Basic $encodedCredentials" }
    
    if (![string]::IsNullOrEmpty($serviceorderid)) {   
         
        try {       
       
            $newURL = $global:cis_service_order_uri + '/' + $serviceorderid
   
            $r = Invoke-WebRequest -Uri $newURL -Method GET -Headers $headers -ContentType "application/json; charset=utf-8"

            $result = ConvertFrom-Json -InputObject $r.Content 

            $putServiceOrder = $result

            # Need to append to comments to existing progressNotes #add Timestamp     
            
            #$putServiceOrder.assignedToUserId = $workorder.WO_INISTAF
            if (![string]::IsNullOrEmpty($global:meterIdConcat)) {
                $putServiceOrder.meterNumbers = $global:meterIdConcat    
            }
            else {
                # $putServiceOrder.meterNumbers = {};
            }
            #$global:IsCancelled = "true"
            if (![string]::IsNullOrEmpty($global:IsCancelled)) {
                $putServiceOrder.isCancelled = $global:IsCancelled 
            }   
            if (![string]::IsNullOrEmpty($global:commentConcat)) {
                $putServiceOrder.completedNotes = "cccccc"#$global:commentConcat
            }              
            
            if (![string]::IsNullOrEmpty($global:progressConcat)) {
                $putServiceOrder.progressNotes = "pppppp"#$global:progressConcat
            }

            #$putServiceOrder.serviceRequestId = $global:currentWO_NUMBER  
            if (![string]::IsNullOrEmpty($global:customerID)) {
                $putServiceOrder.customer = $global:customerID
                $putServiceOrder.customerAccountId = $global:customerAccountID  
            }
        
            if (![string]::IsNullOrEmpty($global:accountID)) {
                $putServiceOrder.account = $global:accountID
            }
            #if (![string]::IsNullOrEmpty($global:completionCode)) {
            #    $putServiceOrder.completionCode = $global:completionCode
            #}
            
            if ($global:isCompleted) {
                if (![string]::IsNullOrEmpty($global:completionCode)) {
                    $putServiceOrder.completionCode = $global:completionCode
                    if ($putServiceOrder.isCompleted -ne $global:isCompleted) {
                        $putServiceOrder.completedTimestamp = $global:completedTimestamp
                    }
                }
            }
            $putServiceOrder.isCompleted = $global:isCompleted
            $putServiceOrder.completedTimestamp = $global:completedTimestamp
       
            
            $newPutServiceOrder = $putServiceOrder | ConvertTo-Json | % { [Regex]::Replace($_, "\\u(?<Value>[a-zA-Z0-9]{4})", { param($m) ([char]([int]::Parse($m.Groups['Value'].Value, [System.Globalization.NumberStyles]::HexNumber))).ToString() } ) }
                        
            $newString1 = $newPutServiceOrder -replace "cccccc", [Regex]::Unescape($global:commentConcat)
            $newString2 = $newString1 -replace "pppppp", [Regex]::Unescape($global:progressConcat)
                   
            $r = Invoke-WebRequest -Uri $newURL -Method PUT -Headers $headers -ContentType "application/json; charset=utf-8" -Body ($newString2) 

            $result = ConvertFrom-Json -InputObject $r.Content
            #$global:serviceOrderUpdated = $global:serviceOrderUpdated + 1
            $newServiceOrder = $result
           
       
        }
        catch {          
            $strLog = "Error Updating Service Order: " + $global:serviceOrderID
            LogWrite $strLog
            $strLog = $_
            LogWrite $strLog
                
        }
        
    }
    else {
        $strLog = "Invalid Service Order Number in UpdateServiceOrder function"
        LogWrite $strLog
    }   


    return  $newServiceOrder
}

function UpdateServiceOrderCommentsOnly ($serviceorderid) {

  
    # Encode the pair to Base64 string
    $encodedCredentials = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($global:credPair))
 
    # Form the header and add the Authorization attribute to it
    $headers = @{ Authorization = "Basic $encodedCredentials" }
    
    if (![string]::IsNullOrEmpty($serviceorderid)) {   
         
        try {       
       
            $newURL = $global:cis_service_order_uri + '/' + $serviceorderid
   
            $r = Invoke-WebRequest -Uri $newURL -Method GET -Headers $headers -ContentType "application/json; charset=utf-8"

            $result = ConvertFrom-Json -InputObject $r.Content

            $putServiceOrder = $result

            # Need to append to comments to existing progressNotes #add Timestamp     
            
            if (![string]::IsNullOrEmpty($global:commentConcat)) {
                $putServiceOrder.completedNotes = "cccccc"#$global:commentConcat
            }              
            
            if (![string]::IsNullOrEmpty($global:progressConcat)) {
                $putServiceOrder.progressNotes = "pppppp"#$global:progressConcat
            }
                      
            
            $putServiceOrder.serviceRequestId = $global:currentWO_NUMBER  
            if (![string]::IsNullOrEmpty($global:customerID)) {
                $putServiceOrder.customer = $global:customerID
                $putServiceOrder.customerAccountId = $global:customerAccountID  
            }
            if (![string]::IsNullOrEmpty($global:IsCancelled)) {
                $putServiceOrder.isCancelled = $global:IsCancelled 
            } 
        
            if (![string]::IsNullOrEmpty($global:accountID)) {
                $putServiceOrder.account = $global:accountID
            }
            #if (![string]::IsNullOrEmpty($global:completionCode)) {
            #    $putServiceOrder.completionCode = $global:completionCode
            #} 

            if ($global:isCompleted) {
                if (![string]::IsNullOrEmpty($global:completionCode)) {
                    $putServiceOrder.completionCode = $global:completionCode
                    if ($putServiceOrder.isCompleted -ne $global:isCompleted) {
                        $putServiceOrder.completedTimestamp = $global:completedTimestamp
                    }
                }
            }
            $putServiceOrder.isCompleted = $global:isCompleted
            $putServiceOrder.completedTimestamp = $global:completedTimestamp
           
            try {    
                
                $newPutServiceOrder = $putServiceOrder | ConvertTo-Json | % { [Regex]::Replace($_, "\\u(?<Value>[a-zA-Z0-9]{4})", { param($m) ([char]([int]::Parse($m.Groups['Value'].Value, [System.Globalization.NumberStyles]::HexNumber))).ToString() } ) }
                        
                $newString1 = $newPutServiceOrder -replace "cccccc", [Regex]::Unescape($global:commentConcat)
                $newString2 = $newString1 -replace "pppppp", [Regex]::Unescape($global:progressConcat)
                   
                $r = Invoke-WebRequest -Uri $newURL -Method PUT -Headers $headers -ContentType "application/json; charset=utf-8" -Body ($newString2) 

                $result = ConvertFrom-Json -InputObject $r.Content
                $global:serviceOrderUpdated = $global:serviceOrderUpdated + 1
                $newServiceOrder = $result
            }
            catch {
               
                $strLog = "Error Updating Service Order Comments: " + $serviceorderid
                LogWrite $strLog
                $strLog = $_
                LogWrite $strLog
            }
           
       
        }
        catch {
          
            $strLog = "Error Updating Service Order Comments: " + $serviceorderid
            LogWrite $strLog
            $strLog = $_
            LogWrite $strLog
                
        }
        
    }
    else {
        $strLog = "Invalid Service Order Number in UpdateServiceOrderCommentsOnly function"
        LogWrite $strLog
    }   


    return  $newServiceOrder
}


function UpdateWorkOrder($currentServiceOrderID) {

     
    $command = new-object system.data.sqlclient.sqlcommand
    $command.Connection = $global:conn
   

    $command.CommandText = 'Update WKORDER Set WO_EXTERNALID = @WO_EXTERNALID WHERE WO_ID = @WO_ID'
    $command.Parameters.AddWithValue("@WO_EXTERNALID", $currentServiceOrderID);
    $command.Parameters.AddWithValue("@WO_ID", $global:currentWOID);
 
    $global:conn.Open()

    $command.ExecuteNonQuery()   
    $global:conn.Close()
  


}






function CheckForInstallReplaceTasks($workOrder) {
    
    
    $sqlquery = 'Select * From WKWOTSK where WT_WO_ID = ' + $workOrder.WO_ID

    $command = new-object system.data.sqlclient.sqlcommand($sqlquery, $conn)
    $conn.Open()
  
    $adapter = New-Object System.Data.sqlclient.sqlDataAdapter $command
    $dataset = New-Object System.Data.DataSet
    $adapter.Fill($dataset) | Out-Null
    $global:conn.Close()
    $WKWOTSKItems = $dataset.tables[0]


    #0039 is Install New

    
    foreach ($WKWOTSKItem in $WKWOTSKItems) {
        $editMeterTask = 'nothing'
        $editMeterTask = CheckTaskCode($WKWOTSKItem.WT_TASK_CD) 
        if ([string]::IsNullOrEmpty($WKWOTSKItem.WT_USER29)) {                
           
            if (![string]::IsNullOrEmpty($workorder.WO_ACTN_CD)) {
                if ($workorder.WO_ACTN_CD -eq $WKWOTSKItem.WT_TASK_CD) {
                    #Activity is Main Task
                    $command = new-object system.data.sqlclient.sqlcommand
                    $command.Connection = $global:conn 
                    $command.CommandText = 'Update WKWOTSK Set WT_USER29 = @WT_USER29 WHERE WT_ID = @WT_ID'
                    $command.Parameters.AddWithValue("@WT_USER29", $global:serviceOrderID);
                    $command.Parameters.AddWithValue("@WT_ID", $WKWOTSKItem.WT_ID);
 
                    $global:conn.Open()

                    $command.ExecuteNonQuery()   
                    $global:conn.Close()
                       
                }
                else {
                    #Activity is not Main Task Create new Service Order
                    $workorder.WO_ACTN_CD = $WKWOTSKItem.WT_TASK_CD
                    $workorder.WO_ACTN_TY = $WKWOTSKItem.WT_TASK_TY
                    $new_service_order = CreateServiceOrderJSON($workOrder)
                    $service_order_id = AddServiceOrder($new_service_order)
                    $command = new-object system.data.sqlclient.sqlcommand
                    $command.Connection = $global:conn 
                    $command.CommandText = 'Update WKWOTSK Set WT_USER29 = @WT_USER29 WHERE WT_ID = @WT_ID'
                    $command.Parameters.AddWithValue("@WT_USER29", $service_order_id.serviceOrderNumber);
                    $command.Parameters.AddWithValue("@WT_ID", $WKWOTSKItem.WT_ID);
 
                    $global:conn.Open()

                    $command.ExecuteNonQuery()   
                    $global:conn.Close()
                }
            }
           
       
            
        }
        #$WKWOTSKItem.WT_USER11 =$false
        if ($WKWOTSKItem.WT_USER11 -eq $false) {
            $datetime = $(Get-Date -format 'yyyy-MM-ddTHH:MM:ss')
            switch ($editMeterTask) {
            
                'addMeter' {
                    if (![string]::IsNullOrEmpty($global:newMeterID)) { 
                        if (![string]::IsNullOrEmpty($global:accountID)) { 
                                
                            $t = $global:remoteID
                            $success = AddNewAccountMeter($WKWOTSKItem.WT_USER29)

                            if ($success -eq $true) {
                                        
                                        
                                        
                                        
                                $command = new-object system.data.sqlclient.sqlcommand
                                $command.Connection = $global:conn 
                                $command.CommandText = 'Update WKWOTSK Set WT_USER11 = @WT_USER11 WHERE  WT_ID = @WT_ID'
                                $command.Parameters.AddWithValue("@WT_USER29", $global:serviceOrderID);
                                $command.Parameters.AddWithValue("@WT_ID", $WKWOTSKItem.WT_ID);
                                $command.Parameters.AddWithValue("@WT_USER11", $true);
 
                                $global:conn.Open()

                                $command.ExecuteNonQuery()   
                                $global:conn.Close()
                                $strComment = "Added New Meter"
                                checkComment($strComment) 
                                $strComment = "New Meter:" + $global:newMeterID
                                checkComment($strComment) 
                                $strComment = "New Remote:" + $global:remoteID
                                #checkComment($strComment) PVWC Does not track Remotes
                                $strComment = "Reading: " + $global:newMeterReading
                                checkComment($strComment)
                                       
                            }
                        }
                    }

                }
                'replaceMeter' {
                    if (![string]::IsNullOrEmpty($global:newMeterID)) { 
                        if (![string]::IsNullOrEmpty($global:oldMeterID)) { 
                            if (![string]::IsNullOrEmpty($global:accountID)) { 
                                if (![string]::IsNullOrEmpty($global:meterEndpointID)) {                           
                                   
                                 
                                    $success = ReplaceAccountMeter($WKWOTSKItem.WT_USER29)

                                    #Add New Meter Reading
                                 
                                    if ($success -eq $true) {
                                       
                                        #$global:oldMeterReading
                                        #$strComment = "Replace Meter: " + $global:oldMeterID + "(Reading: " + $global:oldMeterReading + ") with New Meter:" + $global:newMeterID + "(Reading: " + $global:newMeterReading + ")"
                                        $strComment = "Replace Meter"
                                        checkComment($strComment) 
                                        $strComment = "Old Meter:" + $global:oldMeterID
                                        checkComment($strComment) 
                                        $strComment = "Old Remote:" + $global:oldRemoteID
                                        #checkComment($strComment)  PVWC Does not track Remotes
                                        $strComment = "Old Meter Reading:" + $global:oldMeterReading
                                        checkComment($strComment) 
                                        $strComment = "New Meter:" + $global:newMeterID
                                        checkComment($strComment) 
                                        $strComment = "New Remote:" + $global:remoteID #PVWC Does not track Remotes
                                        checkComment($strComment) #PVWC Does not track Remotes
                                        $strComment = "New Meter Reading: " + $global:newMeterReading
                                        checkComment($strComment)
                                       
                                       


                                        $command = new-object system.data.sqlclient.sqlcommand
                                        $command.Connection = $global:conn 
                                        $command.CommandText = 'Update WKWOTSK Set WT_USER11 = @WT_USER11 WHERE  WT_ID = @WT_ID'
                                        $command.Parameters.AddWithValue("@WT_USER29", $global:serviceOrderID);
                                        $command.Parameters.AddWithValue("@WT_ID", $WKWOTSKItem.WT_ID);
                                        $command.Parameters.AddWithValue("@WT_USER11", $true);
 
                                        $global:conn.Open()

                                        $command.ExecuteNonQuery()   
                                        $global:conn.Close()
                                        #Add New Meter Reading
                                        #Add Old Meter Reading    
                                       
                                    }
                                }
                            }
                        }
                    }
                }
                'replaceRemote' {
                    if (![string]::IsNullOrEmpty($global:current_installed_meter)) {                         
                        if (![string]::IsNullOrEmpty($global:accountID)) { 
                            if (![string]::IsNullOrEmpty($global:meterEndpointID)) {                           
                                 

                                $success = ReplaceRemote($WKWOTSKItem.WT_USER29)

                                if ($success -eq $true) {
                                    
                                    $strComment = "Replace Remote On Existing Meter"
                                    checkComment($strComment) 
                                    $strComment = "Meter:" + $global:current_installed_meter
                                    checkComment($strComment) 
                                    $strComment = "Remote:" + $global:oldRemoteID
                                    checkComment($strComment)                                    
                                    #$strComment = "New Remote:" + $global:remoteID PVWC Does not track Remote
                                    #checkComment($strComment) PVWC Does not track Remote
                                   

                                    if (![string]::IsNullOrEmpty($global:remoteReplaceMeterReading)) {  
                                        $global:currentMeterReading = $global:remoteReplaceMeterReading
                                    }


                                    if (![string]::IsNullOrEmpty($global:currentMeterReading)) {                                        
                                    
                                        $strComment = "Removed Reading: " + $global:currentMeterReading
                                        checkComment($strComment)  
                                    }
                                    else {
                                        $strComment = "No Meter Reading identified for new Remote!"
                                        #checkComment($strComment) PVWC Does not track Remotes
                                    } 
                                    if ($global:currentMeterReading -eq $global:newMeterReading) {
                                        $global:newMeterReading = 0
                                    }
                                    if ([string]::IsNullOrEmpty($global:newMeterReading)) {  
                                        $global:newMeterReading = 0
                                    }
                                    $strComment = "New Reading: " + $global:newMeterReading
                                    
                                    checkComment($strComment)
                                       
                               
                                    $command = new-object system.data.sqlclient.sqlcommand
                                    $command.Connection = $global:conn 
                                    $command.CommandText = 'Update WKWOTSK Set WT_USER11 = @WT_USER11 WHERE  WT_ID = @WT_ID'
                                    $command.Parameters.AddWithValue("@WT_USER29", $global:serviceOrderID);
                                    $command.Parameters.AddWithValue("@WT_ID", $WKWOTSKItem.WT_ID);
                                    $command.Parameters.AddWithValue("@WT_USER11", $true);
 
                                    $global:conn.Open()

                                    $command.ExecuteNonQuery()   
                                    $global:conn.Close()
                                    #Add New Meter Reading
                                    #Add Old Meter Reading    
                                       
                                }
                            }
                        }
                        
                    }
                }
                'addPendingMeterReading' {
                    if (![string]::IsNullOrEmpty($global:current_installed_meter)) {                         
                        if (![string]::IsNullOrEmpty($global:accountID)) { 
                            if (![string]::IsNullOrEmpty($global:meterEndpointID)) {
                                
                                if (![string]::IsNullOrEmpty($global:currentMeterReading)) {  
                                    $success = CreateMeterReadingJSON
                                    $global:runInformationalRead = "false"
                                    if ($success -eq $true) {
                                        $command = new-object system.data.sqlclient.sqlcommand
                                        $command.Connection = $global:conn 
                                        $command.CommandText = 'Update WKWOTSK Set WT_USER11 = @WT_USER11 WHERE  WT_ID = @WT_ID'
                                        $command.Parameters.AddWithValue("@WT_USER29", $global:serviceOrderID);
                                        $command.Parameters.AddWithValue("@WT_ID", $WKWOTSKItem.WT_ID);
                                        $command.Parameters.AddWithValue("@WT_USER11", $true);
 
                                        $global:conn.Open()

                                        $command.ExecuteNonQuery()   
                                        $global:conn.Close()
                                        $strComment = "Meter:" + $global:current_installed_meter                                   
                                        checkComment($strComment)  
                                        $strComment = "Current Reading: " + $global:currentMeterReading
                                        checkComment($strComment)  
                                    }               
                                }
                                
                            }
                        }
                    }
                }
                'removeMeter' {
                    
                    if (![string]::IsNullOrEmpty($global:current_installed_meter)) {                         
                        if (![string]::IsNullOrEmpty($global:accountID)) { 
                            if (![string]::IsNullOrEmpty($global:meterEndpointID)) {                           
                                if (![string]::IsNullOrEmpty($global:oldMeterReading)) {  
                                    
                                    $success = RemoveMeterOnly

                                    if ($success -eq $true) {
                                    
                                        $strComment = "Removed Meter"
                                        checkComment($strComment) 
                                        $strComment = "Meter:" + $global:current_installed_meter
                                        checkComment($strComment) 
                                    
                                  
                                        if (![string]::IsNullOrEmpty($global:oldMeterReading)) {
                                    
                                            $strComment = "Removed Meter Reading: " + $global:oldMeterReading
                                            checkComment($strComment) 
                                        }
                                        else {
                                            $strComment = "No Meter Reading identified for old Meter"
                                            checkComment($strComment)
                                        }                                     

                                        $command = new-object system.data.sqlclient.sqlcommand
                                        $command.Connection = $global:conn 
                                        $command.CommandText = 'Update WKWOTSK Set WT_USER11 = @WT_USER11 WHERE  WT_ID = @WT_ID'
                                        $command.Parameters.AddWithValue("@WT_USER29", $global:serviceOrderID);
                                        $command.Parameters.AddWithValue("@WT_ID", $WKWOTSKItem.WT_ID);
                                        $command.Parameters.AddWithValue("@WT_USER11", $true);
 
                                        $global:conn.Open()

                                        $command.ExecuteNonQuery()   
                                        $global:conn.Close()
                                        
                                       
                                    }
                                }
                            }
                        }
                        
                    }

                }
                'replaceUME' {
                    if (![string]::IsNullOrEmpty($global:current_installed_meter)) { 
                        if (![string]::IsNullOrEmpty($global:current_installed_meter)) { 
                            if (![string]::IsNullOrEmpty($global:accountID)) { 
                                if (![string]::IsNullOrEmpty($global:meterEndpointID)) {  
                                   
                                    $success = ReplaceUME($WKWOTSKItem.WT_USER29)

                                    #Add New Meter Reading
                                 
                                    if ($success -eq $true) {
                                       
                                        #$global:oldMeterReading
                                        #$strComment = "Replace Meter: " + $global:oldMeterID + "(Reading: " + $global:oldMeterReading + ") with New Meter:" + $global:newMeterID + "(Reading: " + $global:newMeterReading + ")"
                                        $strComment = "Replace UME"
                                        checkComment($strComment) 
                                        $strComment = "Old Meter:" + $global:current_installed_meter
                                        checkComment($strComment)                                    
                                        $strComment = "Old Meter Reading:" + $global:newMeterReading
                                        checkComment($strComment) 
                                        $strComment = "New Meter:" + $global:current_installed_meter
                                        checkComment($strComment) 
                                        $strComment = "New Remote:" + $global:remoteID
                                        #checkComment($strComment) PVWC Does not track Remote
                                        $strComment = "New Meter Reading: 0" 
                                        checkComment($strComment)
                                       
                                       


                                        $command = new-object system.data.sqlclient.sqlcommand
                                        $command.Connection = $global:conn 
                                        $command.CommandText = 'Update WKWOTSK Set WT_USER11 = @WT_USER11 WHERE  WT_ID = @WT_ID'
                                        $command.Parameters.AddWithValue("@WT_USER29", $global:serviceOrderID);
                                        $command.Parameters.AddWithValue("@WT_ID", $WKWOTSKItem.WT_ID);
                                        $command.Parameters.AddWithValue("@WT_USER11", $true);
 
                                        $global:conn.Open()

                                        $command.ExecuteNonQuery()   
                                        $global:conn.Close()
                                        #Add New Meter Reading
                                        #Add Old Meter Reading    
                                       
                                    }
                                    




                                }
                            }
                        }
                    }
                }
                'nothing' {

                }
                Default {

                }
            
            } 
        }
        else {
            switch ($editMeterTask) {
            
                'addPendingMeterReading' {
                    if (![string]::IsNullOrEmpty($global:current_installed_meter)) {                         
                        if (![string]::IsNullOrEmpty($global:accountID)) { 
                            if (![string]::IsNullOrEmpty($global:meterEndpointID)) {                                 
                                if (![string]::IsNullOrEmpty($global:currentMeterReading)) {
                                    $global:runInformationalRead = "false"
                                    $strComment = "Meter:" + $global:current_installed_meter                                   
                                    checkComment($strComment)  
                                    $strComment = "Current Reading: " + $global:currentMeterReading
                                    checkComment($strComment)   
                                }  
                            }
                        }
                    }
                }
                
                
                'addMeter' {   
                    if (![string]::IsNullOrEmpty($global:newMeterID)) { 
                        if (![string]::IsNullOrEmpty($global:accountID)) {                                 
                            $test = $global:currentWOID    
                            GetRemoteDataNoEndpoint($global:newMeterRecNumber)
                            $strComment = "Added New Meter"
                            checkComment($strComment) 
                            $strComment = "New Meter:" + $global:newMeterID
                            checkComment($strComment) 
                            $strComment = "New Remote:" + $global:remoteID
                            #checkComment($strComment) PVWC Does Not Track Remotes
                            $strComment = "New Reading: " + $global:newMeterReading
                            checkComment($strComment)
                          
                        }                   
                    }

                }
                'replaceMeter' {
                    if (![string]::IsNullOrEmpty($global:newMeterID)) { 
                        if (![string]::IsNullOrEmpty($global:oldMeterID)) { 
                            if (![string]::IsNullOrEmpty($global:accountID)) { 
                                if (![string]::IsNullOrEmpty($global:meterEndpointID)) {   
                                    

                                    
                                    if (![string]::IsNullOrEmpty($global:accountID)) {
         
                                        $encodedCredentials = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($global:credPair))
 
                                        # Form the header and add the Authorization attribute to it
                                        $headers = @{ Authorization = "Basic $encodedCredentials" }
    
                                        $body = @{
                                            where = "account eq " + $global:accountID   
                                        } 

                                        try {
                                            $r = Invoke-WebRequest -Uri $global:cis_account_meter_uri -Method GET -Headers $headers -ContentType "application/json; charset=utf-8" -Body($body)
       

                                            $result = ConvertFrom-Json -InputObject $r.Content

                                            foreach ($accountmeter in $result._embedded.accountmeter) {
                     
                                                if ($global:oldMeterID -eq $accountmeter.meter) {
                  
                                                    $global:oldRemoteID = $accountmeter.remoteId
                       

                                                }
            
                                            }
       
                                        }
                                        catch {               
               
       
                                        }
      
      
                                    }
                                    else {
                                        $strLog = "Invalid Account ID"
                                        LogWrite $strLog
                                    }
                                   
                                    GetRemoteData($global:meterEndpointID)                   
                                    $strComment = "Replace Meter"
                                    checkComment($strComment) 
                                    $strComment = "Old Meter:" + $global:oldMeterID
                                    checkComment($strComment) 
                                    $strComment = "Old Remote:" + $global:oldRemoteID
                                    checkComment($strComment) 
                                    $strComment = "Old Meter Reading:" + $global:oldMeterReading
                                    checkComment($strComment) 
                                    $strComment = "New Meter:" + $global:newMeterID
                                    checkComment($strComment) 
                                    $strComment = "New Remote:" + $global:remoteID
                                    #checkComment($strComment) PVWC Does not track Remote
                                    $strComment = "New Meter Reading: " + $global:newMeterReading
                                    checkComment($strComment)
                                    
                                }
                            }
                        } 
                    }

                   
                }
                'replaceRemote' {
                    if (![string]::IsNullOrEmpty($global:current_installed_meter)) {                         
                        if (![string]::IsNullOrEmpty($global:accountID)) { 
                            if (![string]::IsNullOrEmpty($global:meterEndpointID)) { 
                   
                                #GetRemoteData($global:meterEndpointID)    PVWC Does not use Remotes


                                if (![string]::IsNullOrEmpty($global:accountID)) {
         
                                    $encodedCredentials = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($global:credPair))
 
                                    # Form the header and add the Authorization attribute to it
                                    $headers = @{ Authorization = "Basic $encodedCredentials" }
    
                                    $body = @{
                                        where = "account eq " + $global:accountID   
                                    } 

                                    try {
                                        $r = Invoke-WebRequest -Uri $global:cis_account_meter_uri -Method GET -Headers $headers -ContentType "application/json; charset=utf-8" -Body($body)
       

                                        $result = ConvertFrom-Json -InputObject $r.Content

                                        foreach ($accountmeter in $result._embedded.accountmeter) {
                     
                                            if ($global:current_installed_meter -eq $accountmeter.meter) {
                                                if (![string]::IsNullOrEmpty($accountmeter.dateRemoved)) {
                                                    $global:oldRemoteID = $accountmeter.remoteId
                                                }

                                            }
            
                                        }
       
                                    }
                                    catch {               
               
       
                                    }
      
      
                                }
                                else {
                                    $strLog = "Invalid Account ID"
                                    LogWrite $strLog
                                }                
                                $strComment = "Replace Remote On Existing Meter"
                                checkComment($strComment) 
                                $strComment = "Meter:" + $global:current_installed_meter
                                checkComment($strComment) 
                                $strComment = "Old Remote:" + $global:oldRemoteID
                                checkComment($strComment)                                    
                                $strComment = "New Remote:" + $global:remoteID
                                #checkComment($strComment)  PVWC Does not Track Remote
                  
                    
                                if (![string]::IsNullOrEmpty($global:remoteReplaceMeterReading)) {  
                                    $global:currentMeterReading = $global:remoteReplaceMeterReading
                                }
                            
                                $strComment = "Removed Reading: " + $global:currentMeterReading
                                checkComment($strComment)
                                if ($global:currentMeterReading -eq $global:newMeterReading) {
                                    $global:newMeterReading = 0
                                }

                                if ([string]::IsNullOrEmpty($global:newMeterReading)) {  
                                    $global:newMeterReading = 0
                                }
                            

                                  
                                $strComment = "New Reading: " + $global:newMeterReading
                                checkComment($strComment)
                           
                                
                            }
                        }                        
                    }
                }
                'removeMeter' {
                    if (![string]::IsNullOrEmpty($global:current_installed_meter)) {                         
                        if (![string]::IsNullOrEmpty($global:accountID)) { 
                            if (![string]::IsNullOrEmpty($global:meterEndpointID)) {                           
                                 

                                

                    
                                #GetRemoteData($global:meterEndpointID)
                                if (![string]::IsNullOrEmpty($global:accountID)) {
         
                                    $encodedCredentials = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($global:credPair))
 
                                    # Form the header and add the Authorization attribute to it
                                    $headers = @{ Authorization = "Basic $encodedCredentials" }
    
                                    $body = @{
                                        where = "account eq " + $global:accountID   
                                    } 

                                    try {
                                        $r = Invoke-WebRequest -Uri $global:cis_account_meter_uri -Method GET -Headers $headers -ContentType "application/json; charset=utf-8" -Body($body)
       

                                        $result = ConvertFrom-Json -InputObject $r.Content

                                        foreach ($accountmeter in $result._embedded.accountmeter) {
                     
                                            if ($global:current_installed_meter -eq $accountmeter.meter) {
                                                if (![string]::IsNullOrEmpty($accountmeter.dateRemoved)) {
                                                    $global:oldRemoteID = $accountmeter.remoteId
                                                    $global:oldMeterReading = $accountmeter.accountMeterReadTypeList.removeReading
                                                }

                                            }
            
                                        }
       
                                    }
                                    catch {               
               
       
                                    }
      
      
                                }
                                else {
                                    $strLog = "Invalid Account ID"
                                    LogWrite $strLog
                                }  
                                
                                
                                    
                                $strComment = "Removed Meter"
                                checkComment($strComment) 
                                $strComment = "Meter:" + $global:current_installed_meter
                                checkComment($strComment)
                                $strComment = "Remote:" + $global:oldRemoteID
                                #checkComment($strComment)  'PVWC Does not track Remotes
                                $strComment = "Removed Meter Reading: " + $global:oldMeterReading
                                checkComment($strComment) 
                               

                            }
                        }
                    }
                    
                                    
                                   
                   

                }
                'replaceUME' {
                    if (![string]::IsNullOrEmpty($global:current_installed_meter)) { 
                        if (![string]::IsNullOrEmpty($global:current_installed_meter)) { 
                            if (![string]::IsNullOrEmpty($global:accountID)) { 
                                if (![string]::IsNullOrEmpty($global:meterEndpointID)) {

                                    if (![string]::IsNullOrEmpty($global:accountID)) {
         
                                        $encodedCredentials = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($global:credPair))
 
                                        # Form the header and add the Authorization attribute to it
                                        $headers = @{ Authorization = "Basic $encodedCredentials" }
    
                                        $body = @{
                                            where = "account eq " + $global:accountID   
                                        } 

                                        try {
                                            $r = Invoke-WebRequest -Uri $global:cis_account_meter_uri -Method GET -Headers $headers -ContentType "application/json; charset=utf-8" -Body($body)
       

                                            $result = ConvertFrom-Json -InputObject $r.Content

                                            foreach ($accountmeter in $result._embedded.accountmeter) {
                     
                                                if ($global:newMeterID -eq $accountmeter.meter) {
                  
                                                    //$global:oldRemoteID = $accountmeter.remoteId
                       

                                                }
            
                                            }
       
                                        }
                                        catch {               
               
       
                                        }
      
      
                                    }
                                    else {
                                        $strLog = "Invalid Account ID"
                                        LogWrite $strLog
                                    }
                                   
                                    GetRemoteData($global:meterEndpointID)                   
                                    $strComment = "Replace UME"
                                    checkComment($strComment) 
                                    $strComment = "Old Meter:" + $global:current_installed_meter
                                    checkComment($strComment)                                    
                                    $strComment = "Old Meter Reading:" + $global:newMeterReading
                                    checkComment($strComment) 
                                    $strComment = "New Meter:" + $global:current_installed_meter
                                    checkComment($strComment) 
                                    $strComment = "New Remote:" + $global:remoteID
                                    #checkComment($strComment) PVWC Does not track Remote
                                    $strComment = "New Meter Reading: 0" 
                                    checkComment($strComment)




                                }
                            }
                        }
                    }
                }
                'nothing' {

                }
                Default {

                }
            
            } 

        }
        if (![string]::IsNullOrEmpty($WKWOTSKItem.WT_USER29)) {
            UpdateServiceOrderCommentsOnly($WKWOTSKItem.WT_USER29)
        }

    }

}





function CreateAccountMeter($currentServiceOrderID) {
    $global:serviceID = $null   
    $datetime = $(Get-Date -format 'yyyy-MM-ddTHH:MM:ss')
    $serviceID = GetServiceID  
    $global:serviceID = $serviceID
    $test = $global:remoteID
    $MyJsonHashTable = @{

        'account'                  = $global:accountID
        'meter'                    = $global:newMeterID
        'oldMeter'                 = ""
        'dateInstalled'            = $datetime
        'dateRemoved'              = ""
        'mustReplace'              = "false"
        'isNotActive'              = "false"
        'serviceOrderNumber'       = $currentServiceOrderID
        'remoteType'               = '01' #$global:remoteType PVWC Doesn't track remote
        'remoteId'                 = $global:remoteID
        'remoteDateInstalled'      = $datetime
        'mustRead'                 = "false"
        'isLocked'                 = "false"
        'longitude'                = ""
        'latitude'                 = ""
        'custom1'                  = ""       
        'isOutForRead'             = "false"
        'installation'             = ""
        'serialNumber'             = $global:newMeterID
        'useRemoteInventory'       = "true"      
        'changeCode'               = ""  
        'inventoryType'            = "WT"
        'meterInventoryLocation'   = "" 
        'accountMeterReadTypeList' = @(@{           
          
                'account'             = $global:accountID
                'meter'               = $global:newMeterID       
                'startDate'           = $datetime
                'endDate'             = ""
                'callNumber'          = "00000"
                'dials'               = 4
                'decimals'            = 0
                'multiplier'          = 1
                'multiplier2'         = 0
                'serviceEntranceId'   = 0
                'previousReading'     = $global:newMeterReading
                'removeReading'       = 0
                'remoteId'            = $global:remoteID
                'remoteDateInstalled' = $datetime
                'minimumRange'        = 0
                'maximumRange'        = 0
                'isNotActive'         = "false"
                'serviceOrderNumber'  = $currentServiceOrderID          
                'oldReadTypeId'       = 0
                'serviceId'           = $serviceID
                'meterMultiplier'     = 0
                'isExternalProcessed' = "false"
                'isMasterMeter'       = "false"
                'channelNumber'       = ""
                'readTypeId'          = $global:newMeterID  #NEED NEW VALUES FROM LUCITY
                'endPointId'          = $global:meterEndpointID  #Blank for new adds
                'intervalMeterType'   = ""
                'meterChangeType'     = "None"
                'readType'            = "WT"
                'remoteType'          = '01'#$global:remoteType
                'units'               = "CF"
           
            })       
    }

    
      
    $MyJsonVariable = $MyJsonHashTable | ConvertTo-Json | % { [Regex]::Unescape($_) }

    # $err = $null
    # $MyJsonObject = [Microsoft.PowerShell.Commands.JsonObject]::ConvertFromJson($MyJsonVariable, [ref]$err)

    return $MyJsonVariable

}

function AddNewAccountMeter($currentServiceOrderID) {
    $AddMeterSuccessful = $false  
   

    $global:updateMeterID = $global:newMeterID
   
    GetRemoteDataNoEndpoint($global:newMeterRecNumber)
    

    $meterFound = 'false'
    $meterID = 0
    if (![string]::IsNullOrEmpty($global:accountID)) {
         
    
        $encodedCredentials = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($global:credPair))
 
        # Form the header and add the Authorization attribute to it
        $headers = @{ Authorization = "Basic $encodedCredentials" }
    
        $body = @{
            where = "account eq " + $global:accountID   
        } 

        try {
            $r = Invoke-WebRequest -Uri $global:cis_account_meter_uri -Method GET -Headers $headers -ContentType "application/json; charset=utf-8" -Body($body)
                

            $result = ConvertFrom-Json -InputObject $r.Content

            foreach ($accountmeter in $result._embedded.accountmeter) {
                $meterID = $accountmeter.meterId
                if ($global:newMeterID -eq $accountmeter.meter) {
                    $meterFound = 'true'
                }
            
            }

            if ($meterFound -eq 'false') {

                if ([string]::IsNullOrEmpty($global:remoteID)) {
                    $global:remoteID = $global:newMeterID
                }

                        
                $new_account_meter = CreateAccountMeter($currentServiceOrderID)
                if ($global:serviceID -gt 0) {
                    if (![string]::IsNullOrEmpty($global:remoteID)) {
                        #Changed due to PVWC not using remotes
                        # Encode the pair to Base64 string
                        $encodedCredentials = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($global:credPair))
 
                        # Form the header and add the Authorization attribute to it
                        $headers = @{ Authorization = "Basic $encodedCredentials" }
                        #$newURI =  $global:cis_account_meter_uri + "/" + $meterID + "/change"    
                        try {
        
                            $r = Invoke-WebRequest -Uri $global:cis_account_meter_uri -Method POST -Headers $headers -ContentType "application/json; charset=utf-8"  -Body ($new_account_meter) 
       
                            $AddMeterSuccessful = $true  
                            $result = ConvertFrom-Json -InputObject $r.Content
                        
        
                        }
                        catch {
                            $strLog = "Unable to create Account Meter for Meter: " + $global:newMeterID
                            LogWrite $strLog
                            $strLog = $_
                            LogWrite $strLog
                            $strComment = $_
                            checkComment($strComment)                            
                             
                        
                        }
                    }
                }
                else {
                    $strLog = "Unable to create Account Meter for Meter: " + $global:newMeterID + " because account:" + $global:accountID + " with service = 30 was not found in Accounts."
                    LogWrite $strLog
                    checkComment($strLog)                            
                        
                }

            }

       
        }
        catch {
            $strLog = "Cannot Find Account Number: " + $accountID
            LogWrite $strLog
       
        }
      
    }
    else {
        $strLog = "Invalid Account ID"
        LogWrite $strLog
    }



    return $AddMeterSuccessful

}


function ReplaceAccountMeter($currentServiceOrderID) {

    $replaceSuccessful = $false  
    #UpdateMeterStatus("SC") Removed because this will occur automaticly
    #GetRemoteData($global:meterEndpointID)
    $datetime = $(Get-Date -format 'yyyy-MM-ddTHH:MM:ss')
    $meterFound = 'false'
    $meterID = 0
    $meterToUpdate = $null
    $meterReadTypeID = $null
    $meterBillTypeCode = $null
    $meterCallNumber = $null
    $checkForDup = $global:newMeterID
    $meterUnits = $null
    $remoteType = $null
    if (![string]::IsNullOrEmpty($global:accountID)) {
         
        $encodedCredentials = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($global:credPair))
 
        # Form the header and add the Authorization attribute to it
        $headers = @{ Authorization = "Basic $encodedCredentials" }
    
        $body = @{
            where = "account eq " + $global:accountID   
        } 

        try {
            $r = Invoke-WebRequest -Uri $global:cis_account_meter_uri -Method GET -Headers $headers -ContentType "application/json; charset=utf-8" -Body($body)
       

            $result = ConvertFrom-Json -InputObject $r.Content

            foreach ($accountmeter in $result._embedded.accountmeter) {
                     
                if ($global:oldMeterID -eq $accountmeter.meter) {
                    $meterID = $accountmeter.meterId
                    $meterFound = $true
                    $meterToUpdate = $accountmeter
                    $global:oldRemoteID = $accountmeter.remoteId
                    $meterBillTypeCode = $meterToUpdate.accountMeterReadTypeList.billCode
                    $meterReadTypeID = $meterToUpdate.accountMeterReadTypeList.readTypeId
                    $meterCallNumber = $meterToUpdate.accountMeterReadTypeList.callNumber
                    $meterUnits = $meterToUpdate.accountMeterReadTypeList.units
                    $remoteType = $accountmeter.remoteType
                    if ($checkForDup -eq $meterID) {
                        $meterFound = $false
                        $replaceSuccessful = $true
                    }
                    if (![string]::IsNullOrEmpty($global:remoteReplaceMeterReading)) {                    
                        if ([string]::IsNullOrEmpty($global:oldMeterReading)) {
                            $global:oldMeterReading = $global:remoteReplaceMeterReading
                        }
                        if ([string]::IsNullOrEmpty($global:newMeterReading)) {
                            $global:newMeterReading = $global:remoteReplaceMeterReading
                        }
                    }
                }
            
            }

            if ($meterFound -eq $true) {
                   
                if (![string]::IsNullOrEmpty($meterToUpdate)) {                   
                       

                    # Encode the pair to Base64 string
                    $encodedCredentials = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($global:credPair))
 
                    # Form the header and add the Authorization attribute to it
                    $headers = @{ Authorization = "Basic $encodedCredentials" }
                    $newURI = $global:cis_account_meter_uri + "/" + $meterID + "/change"  
                    if ([string]::IsNullOrEmpty($global:remoteID)) {
                        $global:remoteID = $global:oldRemoteID                            
                    }
                       
                        

                    if (![string]::IsNullOrEmpty($global:remoteID)) {
                        #changed due to PVWC not using Remotes
                        $serviceID = GetServiceID
                        if (!$serviceID -eq 0) {
                            $MyJsonHashTable = @{
                                'ActionType'                    = "C"
                                'ChangeDate'                    = $datetime
                                'ChangeCode'                    = "13"  #PVWC uses 13 instead of 06
                                'RemovedMeterInventoryStatus'   = "SC"
                                'RemovedMeterInventoryLocation' = ""        
                                'RemoveAccountMeterChangeInfo'  = @(@{   
                                        'account'       = $global:accountID  
                                        'meter'         = $global:oldMeterID              
                                        'DateRemoved'   = $datetime
                                        'meterid'       = $meterID
                                        #add readTypeID #CIS Number from old meter
                                        'readTypeId'    = $meterReadTypeID
                                        'removeReading' = $global:oldMeterReading   
                                    })          
                                'NewAccountMeter'               = @{
                                    'account'                  = $global:accountID
                                    'meter'                    = $global:newMeterID
                                    'oldMeter'                 = ""
                                    'dateInstalled'            = $datetime
                                    'dateRemoved'              = ""
                                    'mustReplace'              = "false"
                                    'isNotActive'              = "false"
                                    'serviceOrderNumber'       = $currentServiceOrderID
                                    'remoteType'               = $remoteType #$global:remoteType PVWC does not use remotes so 01 is default Value   
                                    'remoteId'                 = $global:remoteID#   PVWC does not use remotes
                                    'remoteDateInstalled'      = $datetime
                                    'mustRead'                 = "false"
                                    'isLocked'                 = "false"
                                    'longitude'                = ""
                                    'latitude'                 = ""
                                    'custom1'                  = ""       
                                    'isOutForRead'             = "false"
                                    'installation'             = ""
                                    'serialNumber'             = $global:newMeterID
                                    'useRemoteInventory'       = "true"      
                                    'changeCode'               = ""  
                                    'inventoryType'            = "WT"
                                    'meterInventoryLocation'   = "" 
                                    'accountMeterReadTypeList' = @(@{  
                                            'account'             = $global:accountID
                                            'meter'               = $global:newMeterID       
                                            'startDate'           = $datetime
                                            'endDate'             = ""
                                            'callNumber'          = $meterCallNumber
                                            'dials'               = 4
                                            'decimals'            = 0
                                            'multiplier'          = 1
                                            'multiplier2'         = 0
                                            'serviceEntranceId'   = 0
                                            'previousReading'     = $global:newMeterReading
                                            'removeReading'       = 0
                                            'remoteId'            = $global:remoteID
                                            'remoteDateInstalled' = $datetime
                                            'minimumRange'        = 0
                                            'maximumRange'        = 0
                                            'isNotActive'         = "false"
                                            'serviceOrderNumber'  = $currentServiceOrderID          
                                            'oldReadTypeId'       = 0
                                            'serviceId'           = $serviceID
                                            'meterMultiplier'     = 0
                                            'isExternalProcessed' = "false"
                                            'isMasterMeter'       = "false"
                                            'channelNumber'       = ""
                                            'billCode'            = $meterBillTypeCode
                                            'readTypeId'          = $global:newMeterID  #NEED NEW VALUES FROM LUCITY
                                            'endPointId'          = $meterToUpdate.accountMeterReadTypeList.endPointId #$global:meterEndpointID  #Waiting on value to get populated but it will be the OLD Meters endPointId
                                            'intervalMeterType'   = ""
                                            'meterChangeType'     = "None"
                                            'readType'            = "WT"
                                            'remoteType'          = $remoteType #$global:remoteType #PVWC Does not track Remote
                                            'units'               = $meterUnits          
                                        })    
                                }
                                
                            }
                            
                            $changeMeterJSON = $MyJsonHashTable | ConvertTo-Json -Depth 4 | % { [Regex]::Unescape($_) }
                           
                            try {
       
                            
                        
                                $r = Invoke-WebRequest -Uri $newURI -Method POST -Headers $headers -ContentType "application/json; charset=utf-8"  -Body ($changeMeterJSON) 
       
                                 
                                $result = ConvertFrom-Json -InputObject $r.Content
                                #UpdateMeterRemoteStatus $global:oldRemoteID "SC" PVWC does not use Remotes
                                $replaceSuccessful = $true
                        
        
                            }
                            catch {
                                $strComment = $_
                                checkComment($_) 
                                $strLog = $_
                                LogWrite $strLog
                                    

                            }
                        }
                    }
                    else {
                        checkComment("Missing New Remote(AMR) information") 
                            
                    }
                }

            }

       
        }
        catch {               
            $strLog = "Cannot Find Account Number: " + $accountID
            LogWrite $strLog
            $strLog = $_
            LogWrite $strLog
       
        }
      
      
    }
    else {
        $strLog = "Invalid Account ID"
        LogWrite $strLog
    }

    return $replaceSuccessful
}


function ReplaceUME($currentServiceOrderID) {

    $replaceSuccessful = $false  
    #UpdateMeterStatus("SC") Removed because this will occur automaticly
    #GetRemoteData($global:meterEndpointID)
    $datetime = $(Get-Date -format 'yyyy-MM-ddTHH:MM:ss')
    $meterFound = 'false'
    $meterID = 0
    $meterToUpdate = $null
    $meterReadTypeID = $null
    $meterBillTypeCode = $null
    $meterCallNumber = $null
    $meterUnits = $null
    $remoteType = $null
    $checkForDup = $global:newMeterID
    if (![string]::IsNullOrEmpty($global:accountID)) {
         
        $encodedCredentials = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($global:credPair))
 
        # Form the header and add the Authorization attribute to it
        $headers = @{ Authorization = "Basic $encodedCredentials" }
    
        $body = @{
            where = "account eq " + $global:accountID   
        } 

        try {
            $r = Invoke-WebRequest -Uri $global:cis_account_meter_uri -Method GET -Headers $headers -ContentType "application/json; charset=utf-8" -Body($body)
       

            $result = ConvertFrom-Json -InputObject $r.Content

            foreach ($accountmeter in $result._embedded.accountmeter) {
                     
                if ($global:current_installed_meter -eq $accountmeter.meter) {
                    $meterID = $accountmeter.meterId
                    $meterFound = $true
                    $meterToUpdate = $accountmeter
                    $global:oldRemoteID = $accountmeter.remoteId
                    $meterBillTypeCode = $meterToUpdate.accountMeterReadTypeList.billCode
                    $meterReadTypeID = $meterToUpdate.accountMeterReadTypeList.readTypeId
                    $meterCallNumber = $meterToUpdate.accountMeterReadTypeList.callNumber
                    $serviceID = $meterToUpdate.accountMeterReadTypeList.serviceId
                    $meterUnits = $meterToUpdate.accountMeterReadTypeList.units
                    $remoteType = $accountmeter.remoteType
                    if ($checkForDup -eq $meterID) {
                        $meterFound = $false
                        $replaceSuccessful = $true
                    }
                    if (![string]::IsNullOrEmpty($global:remoteReplaceMeterReading)) {                    
                        if ([string]::IsNullOrEmpty($global:oldMeterReading)) {
                            $global:oldMeterReading = $global:remoteReplaceMeterReading
                        }
                        #$global:newMeterReading = 0
                          
                    }
                }
            
            }

            if ($meterFound -eq $true) {
                   
                if (![string]::IsNullOrEmpty($meterToUpdate)) {                   
                       

                    # Encode the pair to Base64 string
                    $encodedCredentials = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($global:credPair))
 
                    # Form the header and add the Authorization attribute to it
                    $headers = @{ Authorization = "Basic $encodedCredentials" }
                    $newURI = $global:cis_account_meter_uri + "/" + $meterID + "/change"  
                    if ([string]::IsNullOrEmpty($global:remoteID)) {
                        $global:remoteID = $global:oldRemoteID                            
                    }
                       
                        

                    if (![string]::IsNullOrEmpty($global:remoteID)) { 
                        #$serviceID = GetServiceID
                        if (!$serviceID -eq 0) {
                            $MyJsonHashTable = @{
                                'ActionType'                    = "M"
                                'ChangeDate'                    = $datetime
                                'ChangeCode'                    = "02"  #Maintenance
                                'RemovedMeterInventoryStatus'   = "AC"
                                'RemovedMeterInventoryLocation' = ""        
                                'RemoveAccountMeterChangeInfo'  = @(@{   
                                        'account'       = $global:accountID  
                                        'meter'         = $global:current_installed_meter         
                                        'DateRemoved'   = $datetime
                                        'meterid'       = $meterID
                                        #add readTypeID #CIS Number from old meter
                                        'readTypeId'    = $meterReadTypeID
                                        'removeReading' = $global:newMeterReading   
                                    })          
                                'NewAccountMeter'               = @{
                                    'account'                  = $global:accountID
                                    'meter'                    = $global:current_installed_meter
                                    'oldMeter'                 = ""
                                    'dateInstalled'            = $datetime
                                    'dateRemoved'              = ""
                                    'mustReplace'              = "false"
                                    'isNotActive'              = "false"
                                    'serviceOrderNumber'       = $currentServiceOrderID
                                    'remoteType'               = $remoteType #$global:remoteType PVWC does not use remotes so 01 is default Value   
                                    'remoteId'                 = $global:remoteID#   PVWC does not use remotes
                                    'remoteDateInstalled'      = $datetime
                                    'mustRead'                 = "false"
                                    'isLocked'                 = "false"
                                    'longitude'                = ""
                                    'latitude'                 = ""
                                    'custom1'                  = ""       
                                    'isOutForRead'             = "false"
                                    'installation'             = ""
                                    'serialNumber'             = $global:newMeterID
                                    'useRemoteInventory'       = "true"      
                                    'changeCode'               = ""  
                                    'inventoryType'            = "WT"
                                    'meterInventoryLocation'   = "" 
                                    'accountMeterReadTypeList' = @(@{  
                                            'account'             = $global:accountID
                                            'meter'               = $global:current_installed_meter       
                                            'startDate'           = $datetime
                                            'endDate'             = ""
                                            'callNumber'          = $meterCallNumber
                                            'dials'               = 4
                                            'decimals'            = 0
                                            'multiplier'          = 1
                                            'multiplier2'         = 0
                                            'serviceEntranceId'   = 0
                                            'previousReading'     = 0
                                            'removeReading'       = 0
                                            'remoteId'            = $global:remoteID
                                            'remoteDateInstalled' = $datetime
                                            'minimumRange'        = 0
                                            'maximumRange'        = 0
                                            'isNotActive'         = "false"
                                            'serviceOrderNumber'  = $currentServiceOrderID          
                                            'oldReadTypeId'       = 0
                                            'serviceId'           = $serviceID
                                            'meterMultiplier'     = 0
                                            'isExternalProcessed' = "false"
                                            'isMasterMeter'       = "false"
                                            'channelNumber'       = ""
                                            'billCode'            = $meterBillTypeCode
                                            'readTypeId'          = $meterReadTypeID 
                                            'endPointId'          = $meterToUpdate.accountMeterReadTypeList.endPointId #$global:meterEndpointID  #Waiting on value to get populated but it will be the OLD Meters endPointId
                                            'intervalMeterType'   = ""
                                            'meterChangeType'     = "None"
                                            'readType'            = "WT"
                                            'remoteType'          = $remoteType #$global:remoteType #PVWC Does not track Remote
                                            'units'               = $meterUnits           
                                        })    
                                }
                                
                            }
                            
                            $changeMeterJSON = $MyJsonHashTable | ConvertTo-Json -Depth 4 | % { [Regex]::Unescape($_) }
                           
                            try {
                                   
                        
                                $r = Invoke-WebRequest -Uri $newURI -Method POST -Headers $headers -ContentType "application/json; charset=utf-8"  -Body ($changeMeterJSON) 
       
                                 
                                $result = ConvertFrom-Json -InputObject $r.Content
                                #UpdateMeterRemoteStatus $global:oldRemoteID "SC" PVWC does not use Remotes
                                $replaceSuccessful = $true
                        
        
                            }
                            catch {
                                $strComment = $_
                                checkComment($_) 
                                $strLog = $_
                                LogWrite $strLog
                                    

                            }
                        }
                    }
                    else {
                        checkComment("Missing New Remote(AMR) information") 
                            
                    }
                }

            }

       
        }
        catch {               
            $strLog = "Cannot Find Account Number: " + $accountID
            LogWrite $strLog
            $strLog = $_
            LogWrite $strLog
       
        }
      
      
    }
    else {
        $strLog = "Invalid Account ID"
        LogWrite $strLog
    }

    return $replaceSuccessful
}


function ReplaceRemote($currentServiceOrderID) {

    $replaceSuccessful = $false  
    #UpdateMeterStatus("SC") Removed because this will occur automaticly
    GetRemoteData($global:meterEndpointID)
    $datetime = $(Get-Date -format 'yyyy-MM-ddTHH:MM:ss')
    $meterFound = 'false'
    $meterID = 0
    $meterBillTypeCode = $null
    $meterToUpdate = $null
    $meterReadTypeID = $null
    $meterCallNumber = $null
    $meterUnits = $null
    $remoteType = $null
    if (![string]::IsNullOrEmpty($global:accountID)) {
         
    
        $encodedCredentials = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($global:credPair))
 
        # Form the header and add the Authorization attribute to it
        $headers = @{ Authorization = "Basic $encodedCredentials" }
    
        $body = @{
            where = "account eq " + $global:accountID   
        } 

        try {
            $r = Invoke-WebRequest -Uri $global:cis_account_meter_uri -Method GET -Headers $headers -ContentType "application/json; charset=utf-8" -Body($body)
       

            $result = ConvertFrom-Json -InputObject $r.Content

            if (![string]::IsNullOrEmpty($global:remoteReplaceMeterReading)) {                    
                if ([string]::IsNullOrEmpty($global:oldMeterReading)) {
                    $global:oldMeterReading = $global:remoteReplaceMeterReading
                }
               
                if ([string]::IsNullOrEmpty($global:newMeterReading)) {
                    $global:newMeterReading = 0
                }
            }
              
            foreach ($accountmeter in $result._embedded.accountmeter) {
                     
                if ($global:current_installed_meter -eq $accountmeter.meter) {
                    $meterID = $accountmeter.meterId
                    $meterFound = 'true'
                    $meterToUpdate = $accountmeter
                    $global:oldRemoteID = $accountmeter.remoteId
                    $remoteType = $accountmeter.remoteType
                    $meterBillTypeCode = $meterToUpdate.accountMeterReadTypeList.billCode
                    $meterReadTypeID = $meterToUpdate.accountMeterReadTypeList.readTypeId
                    $meterCallNumber = $meterToUpdate.accountMeterReadTypeList.callNumber
                    $meterUnits = $meterToUpdate.accountMeterReadTypeList.units
                }
            
            }
                
            if ($meterFound -eq 'true') {
                   
                if (![string]::IsNullOrEmpty($meterToUpdate)) {                   
                       

                       
                       
                       
                    if (![string]::IsNullOrEmpty($global:remoteID)) {
                        $meterToUpdate.remoteID = $global:remoteID
                        $meterToUpdate.remoteType = $global:remoteType
                        $meterToUpdate.remoteDateInstalled = $datetime
                        $meterToUpdate.dateRemoved = $null
                    }
                    $serviceID = GetServiceID
                    # Encode the pair to Base64 string
                    $encodedCredentials = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($global:credPair))
                    $changeMeterJSON = $meterToUpdate | ConvertTo-Json -Depth 4 | % { [Regex]::Unescape($_) }
                    # Form the header and add the Authorization attribute to it
                    $headers = @{ Authorization = "Basic $encodedCredentials" }
                    $newURI = $global:cis_account_meter_uri + "/" + $meterID + "/change"  
                        
                    if (![string]::IsNullOrEmpty($global:oldRemoteID)) {
                        $MyJsonHashTable = @{
                            'ActionType'                    = "M"
                            'ChangeDate'                    = $datetime 
                            'ChangeCode'                    = "02"
                            'RemovedMeterInventoryStatus'   = "AC"
                            'RemovedMeterInventoryLocation' = ""        
                            'RemoveAccountMeterChangeInfo'  = @(@{   
                                    'account'       = $meterToUpdate.account
                                    'meter'         = $meterToUpdate.meter             
                                    'DateRemoved'   = $datetime
                                    'meterid'       = $meterToUpdate.meterId
                                    'readTypeId'    = $meterReadTypeID
                                    'removeReading' = $global:oldMeterReading   
                                        
                                })                                                
                            'NewAccountMeter'               = @{                                  
                                'account'                  = $meterToUpdate.account
                                'meter'                    = $meterToUpdate.meter                                 
                                'serviceOrderNumber'       = $currentServiceOrderID
                                'remoteType'               = $remoteType
                                'remoteId'                 = $global:remoteID
                                'remoteDateInstalled'      = $datetime                                   
                                'accountMeterReadTypeList' = @(@{           
          
                                        'account'             = $meterToUpdate.account
                                        'meter'               = $meterToUpdate.meter  
                                        'callNumber'          = $meterCallNumber   
                                        'dials'               = 4
                                        'decimals'            = 0
                                        'multiplier'          = 1
                                        'multiplier2'         = 0
                                        'serviceEntranceId'   = 0                            
                                        'previousReading'     = $global:newMeterReading
                                        'removeReading'       = 0
                                        'remoteId'            = $global:remoteID
                                        'remoteDateInstalled' = $datetime                                          
                                        'serviceOrderNumber'  = $currentServiceOrderID          
                                        'oldReadTypeId'       = 0
                                        'serviceId'           = $serviceID                                         
                                        'readTypeId'          = $meterToUpdate.meter  #NEED NEW VALUES FROM LUCITY
                                        'endPointId'          = $global:meterEndpointID 
                                        'intervalMeterType'   = ""
                                        'billCode'            = $meterBillTypeCode
                                        'meterChangeType'     = "None"
                                        'readType'            = "WT"
                                        'remoteType'          = $remoteType
                                        'units'               = $meterUnits
           
                                    })  
                                   
                                      
                            }
                                
                        }

      
                        $changeMeterJSON = $MyJsonHashTable | ConvertTo-Json -Depth 4 | % { [Regex]::Unescape($_) }
                            
                        try {
                        
                            $r = Invoke-WebRequest -Uri $newURI -Method POST -Headers $headers -ContentType "application/json; charset=utf-8"  -Body ($changeMeterJSON) 
       
                           
                            $result = ConvertFrom-Json -InputObject $r.Content

                            UpdateMeterRemoteStatus $global:oldRemoteID "SC"
                            UpdateMeterRemoteStatus $global:remoteID "AC"
                            

                            $replaceSuccessful = $true  
                            $result = ConvertFrom-Json -InputObject $r.Content
        
                        }
                        catch {
                            $strLog = "Unable to Remove the Remote from account:" + $accountID
                            LogWrite $strLog
                            $strLog = $_
                            LogWrite $strLog
                            $strComment = $_
                            checkComment($strComment)                            
                          
                        }
                    }
                       
                }

            }

       
        }
        catch {            
            $strLog = "Cannot Find Account Number: " + $accountID
            LogWrite $strLog
       
        }
      
    }
    else {
        $strLog = "Invalid Account ID"
        LogWrite $strLog
    }

    return $replaceSuccessful
}


function UpdateRemoteIDPVWC($tempRemoteID) {

    $updateRemoteID = $false  
    $accountmeterItem = $null
    $accountMeterID = $null
    $accountMeterInventoryType = $null
    $accountMeterReadTypeItem = $null

    if (![string]::IsNullOrEmpty($global:accountID)) {
        if (![string]::IsNullOrEmpty($tempRemoteID)) {   
            
            $encodedCredentials = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($global:credPair))
 
            # Form the header and add the Authorization attribute to it
            $headers = @{ Authorization = "Basic $encodedCredentials" }
    
            $body = @{
                where = "meter eq " + $global:current_installed_meter
            } 
            #"account eq " + $global:accountID  
            try {
                $r = Invoke-WebRequest -Uri $global:cis_account_meter_uri -Method GET -Headers $headers -ContentType "application/json; charset=utf-8" -Body($body)
       

                $result = ConvertFrom-Json -InputObject $r.Content

              
                foreach ($accountmeter in $result._embedded.accountmeter) {
                     
                    if ($global:accountID -eq $accountmeter.account) {

                        if ($accountmeter.remoteId -ne $global:remoteID) {
                            $updateRemoteID = $true
                            $accountMeterID = $accountmeter.meterId #meterId 
                            $accountmeterItem = $accountmeter                           
                            $accountMeterInventoryType = $accountmeter.inventoryType 
                            $accountmeterItem.remoteId = $global:remoteID
                            break
                        }   
                    
                    }
            
                }
                
                if ($updateRemoteID -eq $true) {
                    try {    
                        $newURL = $global:cis_account_meter_uri + '/' + $accountMeterID
                        $newPutAccountMeter = $accountmeterItem | ConvertTo-Json | % { [Regex]::Replace($_, "\\u(?<Value>[a-zA-Z0-9]{4})", { param($m) ([char]([int]::Parse($m.Groups['Value'].Value, [System.Globalization.NumberStyles]::HexNumber))).ToString() } ) }
                       
                        $r = Invoke-WebRequest -Uri $newURL -Method PUT -Headers $headers -ContentType "application/json; charset=utf-8" -Body ($newPutAccountMeter) 

                        $result = ConvertFrom-Json -InputObject $r.Content
                        try {    
                            $newURL = $global:cis_account_meter_readtype_uri + '/' + $accountMeterID + "-" + $accountMeterInventoryType
                            $r = Invoke-WebRequest -Uri $newURL -Method GET -Headers $headers -ContentType "application/json; charset=utf-8"
       

                            $result = ConvertFrom-Json -InputObject $r.Content
                            $result.remoteId = $global:remoteID
              
                          
                            $newPutAccountMeter = $result | ConvertTo-Json | % { [Regex]::Replace($_, "\\u(?<Value>[a-zA-Z0-9]{4})", { param($m) ([char]([int]::Parse($m.Groups['Value'].Value, [System.Globalization.NumberStyles]::HexNumber))).ToString() } ) }
                           
                            $r = Invoke-WebRequest -Uri $newURL -Method PUT -Headers $headers -ContentType "application/json; charset=utf-8" -Body ($newPutAccountMeter) 

                            $result = ConvertFrom-Json -InputObject $r.Content
                        
                        }
                        catch {
               
                            $strLog = "Error Updating Account Meter: " + $accountMeterID
                            LogWrite $strLog
                            $strLog = $_
                            LogWrite $strLog
                        }
                    }
                    catch {
               
                        $strLog = "Error Updating Account Meter: " + $accountMeterID
                        LogWrite $strLog
                        $strLog = $_
                        LogWrite $strLog
                    }
                }
       
            }
            catch {            
                $strLog = $_#"Cannot Find Account Number: " + $accountID
                LogWrite $strLog
       
            }
        }
      
    }
    else {
        $strLog = "Invalid Account ID"
        LogWrite $strLog
    }

    return $replaceSuccessful
}


function RemoveMeterOnly($currentServiceOrderID) {

    $removeSuccessful = $false  
    #UpdateMeterStatus("SC") Removed because this will occur automaticly
    GetRemoteData($global:meterEndpointID)
    $datetime = $(Get-Date -format 'yyyy-MM-ddTHH:MM:ss')
    $meterFound = 'false'
    $meterID = 0
    $meterToUpdate = $null
    $meterReadTypeID = $null
    $meterCallNumber = $null
    if (![string]::IsNullOrEmpty($global:accountID)) {
         
    
        $encodedCredentials = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($global:credPair))
 
        # Form the header and add the Authorization attribute to it
        $headers = @{ Authorization = "Basic $encodedCredentials" }
    
        $body = @{
            where = "account eq " + $global:accountID   
        } 

        try {
            $r = Invoke-WebRequest -Uri $global:cis_account_meter_uri -Method GET -Headers $headers -ContentType "application/json; charset=utf-8" -Body($body)
       

            $result = ConvertFrom-Json -InputObject $r.Content

            foreach ($accountmeter in $result._embedded.accountmeter) {
                     
                if ($global:current_installed_meter -eq $accountmeter.meter) {
                    $meterID = $accountmeter.meterId
                    $meterFound = 'true'
                    $meterToUpdate = $accountmeter
                    $global:oldRemoteID = $accountmeter.remoteId

                }
            
            }

            if ($meterFound -eq 'true') {
                   
                if (![string]::IsNullOrEmpty($meterToUpdate)) {                   
                       
                    if ([string]::IsNullOrEmpty($global:oldMeterReading)) {
                        #if (![string]::IsNullOrEmpty($global:remoteReplaceMeterReading)) {
                        #    $global:oldMeterReading = $global:remoteReplaceMeterReading
                        #}   
                    }

                    # Encode the pair to Base64 string
                    $encodedCredentials = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($global:credPair))
 
                    # Form the header and add the Authorization attribute to it
                    $headers = @{ Authorization = "Basic $encodedCredentials" }
                    $newURI = $global:cis_account_meter_uri + "/" + $meterID 

                       
                    $serviceID = GetServiceID
                       
                        
                    if (![string]::IsNullOrEmpty($meterToUpdate.remoteID)) {
                        $meterToUpdate.remoteID = $global:oldRemoteID
                        $meterToUpdate.remoteType = '01'# $global:remoteType
                        $meterToUpdate.remoteDateInstalled = $datetime
                        $meterToUpdate.dateRemoved = $null
                        $meterReadTypeID = $meterToUpdate.accountMeterReadTypeList.readTypeId
                        #$meterCallNumber = $meterToUpdate.accountMeterReadTypeList.callNumber
                    }
                    
                    # Encode the pair to Base64 string
                    $encodedCredentials = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($global:credPair))
                    $changeMeterJSON = $meterToUpdate | ConvertTo-Json -Depth 4 | % { [Regex]::Unescape($_) }
                    # Form the header and add the Authorization attribute to it
                    $headers = @{ Authorization = "Basic $encodedCredentials" }
                    $newURI = $global:cis_account_meter_uri + "/" + $meterID + "/change"    
                    $MyJsonHashTable = @{
                        'ActionType'                    = "R"
                        'ChangeDate'                    = $datetime
                        'ChangeCode'                    = "13" #PVWC Changed from 06
                        'RemovedMeterInventoryStatus'   = "SC"
                        'RemovedMeterInventoryLocation' = ""        
                        'RemoveAccountMeterChangeInfo'  = @(@{   
                                'account'       = $global:accountID  
                                'meter'         = $meterToUpdate.meter             
                                'DateRemoved'   = $datetime
                                'meterid'       = $meter
                                'readTypeId'    = $meterReadTypeID 
                                'removeReading' = $global:oldMeterReading
                            })          
                                
                    }
      
                    $changeMeterJSON = $MyJsonHashTable | ConvertTo-Json -Depth 4 | % { [Regex]::Unescape($_) }
                            
                    try {
                        
                        $r = Invoke-WebRequest -Uri $newURI -Method POST -Headers $headers -ContentType "application/json; charset=utf-8"  -Body ($changeMeterJSON) 
       
                        $removeSuccessful = $true                          
                            
                        #UpdateMeterRemoteStatus $global:remoteID "SC" PVWC Does not use Remote Meter
                            
                        
        
                    }
                    catch {
                        $strLog = "Unable to Remove the Meter from account:" + $accountID
                        LogWrite $strLog
                        $strLog = $_
                        LogWrite $strLog
                        $strComment = $_
                        checkComment($strComment)                            
                           
                    }
                       
                }

            }

       
        }
        catch {
            #throw $_
            $strLog = "Cannot Find Account Number: " + $accountID
            LogWrite $strLog
       
        }
      
    }
    else {
        $strLog = "Invalid Account ID"
        LogWrite $strLog
    }

    return $removeSuccessful
}



function GetServiceID() {
   
    $serviceID = 0   
   
    if (![string]::IsNullOrEmpty($global:accountID)) {
         
    
        $encodedCredentials = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($global:credPair))
 
        # Form the header and add the Authorization attribute to it
        $headers = @{ Authorization = "Basic $encodedCredentials" }
    
        $body = @{
            #account eq '069798' and service eq 30
            where = "account eq '" + $global:accountID + "' and service eq 30" 
        } 

        try {
            $r = Invoke-WebRequest -Uri $global:cis_account_service_uri -Method GET -Headers $headers -ContentType "application/json; charset=utf-8" -Body($body)
       

            $result = ConvertFrom-Json -InputObject $r.Content
            foreach ($accountservice in $result._embedded.accountservice) {
                $serviceID = $accountservice.serviceId
                break;
            }
            #$serviceID = 0
               
       
        }
        catch {
                
            $strLog = "Cannot Find Account Number: " + $accountID
            LogWrite $strLog
       
        }
      
    }
    else {
        $strLog = "Invalid Account ID"
        LogWrite $strLog
    }

    return $serviceID

}

function GetMeterReadTypeID($serviceID) {
   
    $readTypeID = 0   
   
    if (![string]::IsNullOrEmpty($global:accountID)) {
         
    
        $encodedCredentials = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($global:credPair))
 
        # Form the header and add the Authorization attribute to it
        $headers = @{ Authorization = "Basic $encodedCredentials" }
    
        $body = @{
                
            where = "account eq '" + $global:accountID + "' and serviceID eq " + $serviceID
        } 

        try {
            $r = Invoke-WebRequest -Uri $global:cis_account_meter_readtype_uri -Method GET -Headers $headers -ContentType "application/json; charset=utf-8" -Body($body)
       

            $result = ConvertFrom-Json -InputObject $r.Content
            foreach ($accountservice in $result._embedded.accountmeterreadtype) {
                #$meter.current_installed_meter
                if ($accountservice.meter -eq $meter.current_installed_meter) {
                    $readTypeID = $accountservice.readTypeId
                    break
                }
            }
               
               
       
        }
        catch {
            throw $_
                
            $strLog = "Cannot Find Account Number: " + $accountID + ' and Service ID' + $serviceID
            LogWrite $strLog
       
        }
      
    }
    else {
        $strLog = "Invalid Account ID"
        LogWrite $strLog
    }

    return $readTypeID

}


function GetRemoteDataNoEndpoint($meter) {
    
    #$global:remoteID = $null
    $global:remoteType = $null
    $tempAM_AR_ID = $null

    if (![string]::IsNullOrEmpty($meter)) {
        $sqlquery = 'Select * From WTAMR where AR_MD_ID = ' + "'" + $meter + "'"

        $command = new-object system.data.sqlclient.sqlcommand($sqlquery, $conn)
        $conn.Open()
  
        $adapter = New-Object System.Data.sqlclient.sqlDataAdapter $command
        $dataset = New-Object System.Data.DataSet
        $adapter.Fill($dataset) | Out-Null
        $global:conn.Close()
        $WKWOTSKItems = $dataset.tables[0]

    
        foreach ($WKWOTSKItem in $WKWOTSKItems) {          
            # $global:remoteID = $WKWOTSKItem.AR_NUMBER
            # $global:remoteType = '0' + $WKWOTSKItem.AR_TYPE_CD             
        }


        if (![string]::IsNullOrEmpty($tempAM_AR_ID)) {
            $sqlquery = 'Select * From WTMTAR where AM_AR_ID = ' + "'" + $tempAM_AR_ID + "'"

            $command = new-object system.data.sqlclient.sqlcommand($sqlquery, $conn)
            $conn.Open()
  
            $adapter = New-Object System.Data.sqlclient.sqlDataAdapter $command
            $dataset = New-Object System.Data.DataSet
            $adapter.Fill($dataset) | Out-Null
            $global:conn.Close()
            $WKWOTSKItems = $dataset.tables[0]

    
            foreach ($WKWOTSKItem in $WKWOTSKItems) {                 
                $global:remoteReplaceMeterReading = $WKWOTSKItem.AM_READ1
                if ($WKWOTSKItem.AM_PURP_CD -eq 2) {  
                    if ([string]::IsNullOrEmpty($global:oldMeterReading)) {
                        $global:oldMeterReading = $WKWOTSKItem.AM_READ1
                    }
                    
                }
                if ($WKWOTSKItem.AM_PURP_CD -eq 1) { 
                    if ([string]::IsNullOrEmpty($global:newMeterReading)) { 
                        $global:newMeterReading = $WKWOTSKItem.AM_READ1
                    }
                                   
                }              
            }
        }
    }

    

}




function GetRemoteData($meter) {
    
    #$global:remoteID = $null
    $global:remoteType = $null
    $tempAM_AR_ID = $null
    $foundNew = 'false'
    $foundOld = 'false'

    if (![string]::IsNullOrEmpty($meter)) {
        $sqlquery = 'Select * From WTAMR where AR_MD_ID = ' + "'" + $global:newMeterRecNumber + "'"

        $command = new-object system.data.sqlclient.sqlcommand($sqlquery, $conn)
        $conn.Open()
  
        $adapter = New-Object System.Data.sqlclient.sqlDataAdapter $command
        $dataset = New-Object System.Data.DataSet
        $adapter.Fill($dataset) | Out-Null
        $global:conn.Close()
        $WKWOTSKItems2 = $dataset.tables[0]
       
    
        foreach ($WKWOTSKItem2 in $WKWOTSKItems2) {          
          
            $tempAR_MT_ID = $WKWOTSKItem2.AR_MT_ID


            if (![string]::IsNullOrEmpty($tempAR_MT_ID)) {
                $sqlquery = 'Select * From WTMTAR where AM_MT_ID = ' + "'" + $tempAR_MT_ID + "'"

                $command = new-object system.data.sqlclient.sqlcommand($sqlquery, $conn)
                $conn.Open()
  
                $adapter = New-Object System.Data.sqlclient.sqlDataAdapter $command
                $dataset = New-Object System.Data.DataSet
                $adapter.Fill($dataset) | Out-Null
                $global:conn.Close()
                $WKWOTSKItems = $dataset.tables[0]

    
                foreach ($WKWOTSKItem in $WKWOTSKItems) {          
                    if ($WKWOTSKItem.AM_PURP_CD -eq 2) { 
                        $foundOld = 'true'
                        if ([string]::IsNullOrEmpty($global:oldMeterReading)) {
                            $global:oldMeterReading = $WKWOTSKItem.AM_READ1
                        }
                        $global:remoteReplaceMeterReading = $WKWOTSKItem.AM_READ1               
                    }
                    if ($WKWOTSKItem.AM_PURP_CD -eq 1) { 

                        $foundNew = 'true'
                        if ([string]::IsNullOrEmpty($global:newMeterReading)) { 
                            $global:newMeterReading = $WKWOTSKItem.AM_READ1
                        }
                        $global:remoteReplaceMeterReading = $WKWOTSKItem.AM_READ1               
                    }
                }
            }
            
            if ($foundNew -eq 'true') {
                if ($foundOld -eq 'true') {
                    #$global:remoteID = $WKWOTSKItem2.AR_NUMBER
                    #$global:remoteType = '0' + $WKWOTSKItem2.AR_TYPE_CD
                }
            }


        }

        


    }

    

}

function CheckTaskCode($taskCode) {
    
    $taskType = 'nothing'

    foreach ($task in $taskReplaceRemoteOnly) {
        if ($task -eq $taskCode) {
            $taskType = 'replaceRemote'
        }
    } 
    foreach ($task in $taskReplaceMeter) {
        if ($task -eq $taskCode) {
            $taskType = 'replaceMeter'
        }
    }

    foreach ($task in $taskAddMeter) {
        if ($task -eq $taskCode) {
            $taskType = 'addMeter'
        }
    }

    foreach ($task in $taskReplaceUME) {
        if ($task -eq $taskCode) {
            $taskType = 'replaceUME'
        }
    }

    
    foreach ($task in $taskRemoveMeter) {
        if ($task -eq $taskCode) {
            $taskType = 'removeMeter'
        }
    }

    foreach ($task in $taskPendingMeterReading) {
        if ($task -eq $taskCode) {
            $taskType = 'addPendingMeterReading'
        }
    }


    return $taskType
    
}


function UpdateMeterRemoteStatus($remoteID, $remoteStatus) {
    
   
   
    $datetime = $(Get-Date -format 'yyyy-MM-ddTHH:MM:ss')

    $meterFound = 'false'
    
    if (![string]::IsNullOrEmpty($remoteID)) { 
    
        $encodedCredentials = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($global:credPair))
 
        # Form the header and add the Authorization attribute to it
        $headers = @{ Authorization = "Basic $encodedCredentials" }
        $newURL = $global:cis_water_meter_remote_uri + '/' + $remoteID 
           

        try {
            $r = Invoke-WebRequest -Uri $newURL -Method GET -Headers $headers -ContentType "application/json; charset=utf-8"
       

            $result = ConvertFrom-Json -InputObject $r.Content

               
            $putWaterMeterRemote = $result

            $putWaterMeterRemote.status = $remoteStatus
            $putWaterMeterRemote.notes = ""
            #$putWaterMeterRemote.skipAccountStatusValidation = "true"
               
            if ($remoteStatus -eq "SC") {
                $putWaterMeterRemote.dateScrapped = $datetime 
            }
            else {
                $putWaterMeterRemote.dateScrapped = ""  
            }
              
                
       
            #The Un
            $newPutWaterMeterRemote = $putWaterMeterRemote | ConvertTo-Json | % { [Regex]::Unescape($_) }
            try {
                $r = Invoke-WebRequest -Uri $newURL -Method PUT -Headers $headers -ContentType "application/json; charset=utf-8" -Body ($newPutWaterMeterRemote) 

                $result = ConvertFrom-Json -InputObject $r.Content               

            }
            catch {
                # throw $_
                $strLog = "Cannot Find Meter Remote Number: " + $remoteID + " in WaterMeterRemote"
                LogWrite $strLog
       
            }
        }
        catch {
            #throw $_
            $strLog = "Cannot Find Meter Remote Number: " + $remoteID + " in WaterMeterRemote"
            LogWrite $strLog
       
        }
    }

    

}




function LogWrite {

      
    
    Param ([string]$logstring)
    $filePath = $LogfilePath + "\" + $LogFile
    $datetime = $(Get-Date -format 'yyyy-MM-dd 0:HH:mm:ss')
    $stringToAdd = $datetime + " WOID:" + $global:currentWOID + " WONumber:" + $global:currentWO_NUMBER + " " + $logstring
    Add-Content $filePath -value $stringToAdd

}

function CheckForActiveMeter($meterNumber) {
    $meterActive = $false
    $datetime = $(Get-Date -format 'yyyy-MM-ddTHH:MM:ss')
   
    
    if (![string]::IsNullOrEmpty($meterNumber)) { 
    
        $encodedCredentials = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($global:credPair))
 
        # Form the header and add the Authorization attribute to it
        $headers = @{ Authorization = "Basic $encodedCredentials" }
        $newURL = $global:cis_water_meter_uri + '/' + $meterNumber
           

        try {
            $r = Invoke-WebRequest -Uri $newURL -Method GET -Headers $headers -ContentType "application/json; charset=utf-8"
       

            $result = ConvertFrom-Json -InputObject $r.Content
           
               
            $waterMeter = $result
            if ($waterMeter.status -eq "AC") {
                $meterActive = $true
            }
          
        }
        catch {
            #throw $_
            $strLog = "Cannot Find Meter Remote Number: " + $remoteID + " in WaterMeterRemote"
            LogWrite $strLog
       
        }
    }

    return $meterActive
}

function TempUpdateWorkOrder() {

  
   
    $command = new-object system.data.sqlclient.sqlcommand
    $command.Connection = $global:conn
   
  
 

    $command.CommandText = 'Update WKORDER Set WO_EXTERNALID = @WO_EXTERNALID WHERE WO_ID = @WO_ID'
    $command.Parameters.AddWithValue("@WO_EXTERNALID", '');
    $command.Parameters.AddWithValue("@WO_ID", '3260');
 
    $global:conn.Open()

    $command.ExecuteNonQuery()  
    $global:conn.Close()
  
    Write-Host $WKWOTSKItems2

}
try {
    StartProcess
    #GetExistingServiceOrder(61)
          
}
catch {
    #throw $_
    $strLog = $_
    LogWrite $strLog
    $safeToRun = $true
    ClearRunDT
       
}

#UpdateMeterStatus ("SC")
#AddNewInstall
#GetRemoteData(10014422)
#CheckTaskCode('0011')
#TestChangeAccountMeter
#GetServiceID
#TestCreateAccountMeter
#TestChangeAccountMeter
#TempUpdateWorkOrder
#UpdateMeterRemoteStatus 2748617 IN