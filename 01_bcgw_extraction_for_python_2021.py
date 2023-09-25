'''
# ----------------------------------------------------------------------------------------------------------------------
# Name:        bcgw_extraction_for_python_2021
# Purpose:     Extract BCGW layers listed in an .xlsx, group and dissolve them into a single layer covering all of BC
# Author:      Robert Gowan, GeoBC, robert.gowan@gov.bc.ca
# Created:     31/05/2019
# Copyright:   (c) rgowan 2019
# Licence:     <For licence options see https://github.com/bcgov/BC-Policy-Framework-For-GitHub/blob/master/BC-Open-Source-Development-Employee-Guide/Licenses.md>
# Python Version:     2.7
# Python Interpretor: E:\sw_nt\Python27\ArcGISx6410.6\python.exe
# ArcGIS Version:     10.6.1
# ----------------------------------------------------------------------------------------------------------------------
# Update History:
#
#    23-July-2019  (salees)
#        -additional CEF fields added to xls to use for development type descriptors/grouping
#    13-Sept-2019 (salees)
#        -revised Source string to read as a raw string, to avoid strange character interpretation of \b
#        -added options to control running of each level
#        -added a dissolve for Level 4
#    7-Oct-2019  (salees)
#        -reference BCGW connection if you have an existing connection called 'Database Connections\BCGW4Scripting.sde' 
#         which uses embedded passwords
#        -added prompt for raw input of Oracle Username and Password
#        -added check for existence/creation of GDBs
#        -added Note re 64 bit processing
#        -added timer
#    27-Aug-2021 Rob Oostlander
#        -copied this script to the 2021 folder: \\spatialfiles.bcgov\work\srm\bcce\shared\data_library\disturbance\human_disturbance\2021\inputs\scripts
#        -added a few extra print messages, cleaned up file paths, but otherwise code works in GTS/ArcGIS version 10.6, in python 2.7.
#        -make sure to run with the 64-bit interpreter/processor if using GTS: E:\sw_nt\Python27\ArcGISx6410.6\python.exe
# ----------------------------------------------------------------------------------------------------------------------
# Dependencies:   Requires the 'bcgw_extraction_list_2021.xlsx' spreadsheet with its specific format & column order.  
#                 The script must be updated to refer to the appropriate columns if there are column order changes. 
# ----------------------------------------------------------------------------------------------------------------------
# Notes:
#
# 1: This script requires IDIR accces to the BC Geographic Warehouse (BCGW), an ArcInfo licence, 
#    and must be run from a BC government terminal server (in DTS/GTS).
#
# 2: You must use the 64 bit interpreter/processor when running this script, otherwise you risk incomplete data extractions! 
#    The script may run without errors, but there may be holes and/or missing features in the final data, with no error message from ArcGIS.
#
#    * The 64 bit interpreter can be found here in GTS 10.6:  E:\sw_nt\Python27\ArcGISx6410.6\python.exe
#    * In Eclipse, select --> Window --> Preferences --> PyDev --> Interpreters --> Python Interpreter --> New --> E:\sw_nt\Python27\ArcGISx6410.6\python.exe  
#    * Use the 'Up' button to move the 64 bit version to the top of the list.  Whatever is listed at the top will be used first by Eclipse.
#
# 3: If you are using the ArcToolbox tool version of this script, it will prompt you for your IDIR credentials.
#    If not, a Database Connection called 'BCGW4Scripting.sde' embedded with your IDIR password is required, 
#    or you could manually hard code your BCGW user name and password below, which is highly discouraged. 
# ----------------------------------------------------------------------------------------------------------------------
# Run Time in 2019:  
#
#     Level 1 and 2 - Data extraction and subgroup dissolve - approx 1.5 hrs
#     Level 3 - Group updates - approx  5 min
#     Level 4 - Group dissolve - approx 8 min
# ----------------------------------------------------------------------------------------------------------------------
'''

import arcpy, os, sys, datetime, time
import xlrd

print 'Starting...'
print '\n******** Notice: It is advisable to use the 64 bit Python interpreter, or otherwise risk incomplete data extractions! ********\n'

#    * The 64 bit interpreter can be found here in GTS 10.6:  E:\sw_nt\Python27\ArcGISx6410.6\python.exe
#    * In Eclipse, select --> Window --> Preferences --> PyDev --> Interpreters --> Python Interpreter --> New --> E:\sw_nt\Python27\ArcGISx6410.6\python.exe  
#    * Use the 'Up' button to move the 64 bit version to the top of the list.  Whatever is listed at the top will be used first by Eclipse.

# Start Time and date
startTime = time.time()
START_TIME = time.ctime(time.time())
print '   Starting : ', START_TIME

#print arcpy.env.overwriteOutput
#arcpy.env.overwriteOutput = True

sys.path.append(r'\\spatialfiles.bcgov\work\srm\bcce\shared\data_library\disturbance\human_disturbance\2021\inputs\scripts')

""" ADJUST OPTIONS TO RUN EACH LEVEL HERE  (Y = Run, N = Don't Run) """

run_lvl_1 = 'Y'     # Raw Data Extraction.  This is also run if LVL 2 is set to 'Y'
run_lvl_2 = 'Y'     # Dissolved Subgroups by CEF fields - if using this, LVL 1 will also be re-run.
run_lvl_3 = 'Y'     # Combined by Group - MAY HAVE self-overlaps!
run_lvl_4 = 'Y'     # Dissolved - should not have self-overlaps - does not have CEF Descriptor/type fields

# Reference BCGW4Scripting database connection.  This uses embedded passwords.  You must have connection set up with the exact name shown here, and with a saved password.
# if you don't have this connection, you can hard-code your username and password below.
bcgw_connection = r'Database Connections\BCGW4Scripting.sde'

# -----------------------------------
# Specify your output folder location
# -----------------------------------
output_folder = r'\\spatialfiles.bcgov\work\srm\bcce\shared\data_library\disturbance\human_disturbance\2021\inputs\data'

# Raw output gdb will contain the dataset extracted from the BCGW plus the fields added by this script (Level 1)
# output gdb will contain the sub group level datasets dissolved on the added fields ready for rolling up into the group layer  (Level 2)
# BCoutput gdb will contain datasets rolled up by group  (Level 3)
# BC_Dissolve gdb will contain group datasets dissolve to remove overlapping features.  (Level 4)

raw_output_gdb = os.path.join(output_folder, "Disturbance_1_Raw.gdb")
output_gdb = os.path.join(output_folder, "Disturbance_2_Subgroup.gdb")
BCoutput_gdb = os.path.join(output_folder, "Disturbance_3_Group.gdb")
BC_Dissolve_gdb = os.path.join(output_folder, "Disturbance_4_Dissolve.gdb")

# This lookup table provides all the information on data sources, attribute mapping, groups and sub groups
# The input list of datasets must be the first sheet in the Excel file
input_table = r'\\spatialfiles.bcgov\work\srm\bcce\shared\data_library\disturbance\human_disturbance\2021\inputs\scripts\01_bcgw_extraction_list_2021.xlsx'

CurrentGroup = ""
maxrank = 0

# This section extracts the raw datasets, adds and populates the new fields and dissolves to create the sub group level datasets
if run_lvl_1 == 'Y' or run_lvl_2 =='Y':
    print '\nRunning LEVEL 1 Raw data Extractions...'

    #Check For existence of Raw GDB
    if not arcpy.Exists(raw_output_gdb):
        folder_output, file_output = os.path.split(raw_output_gdb)
        if not os.path.exists(folder_output):
            os.makedirs(folder_output)
        arcpy.CreateFileGDB_management(folder_output, file_output)

    arcpy.env.workspace = raw_output_gdb
    
    #Check BCGW Connection
    if not arcpy.Exists(bcgw_connection):
        print 'No BCGW4Scripting.sde Database Connection - enter your username and password to connect to BCGW... '
        
        # ----------------------------------------------------
        # Import the GeoBC module (geobc.py)
        # Must be located in the same directory as this script
        # ----------------------------------------------------
        import module_geobc as geobc 
        
        # Enter your BCGW user name and password
        uname = raw_input('Enter your User Name:  ')  
        pword = raw_input('Enter your Oracle Password:  ')
    

        #BCGW Connect
        ACDC            = "DC"
        output_location = 'T:'
        bcgw_object = geobc.BCGWConnection()
        bcgw_object.create_bcgw_connection_file(uname, pword, output_location, ACDC)
        bcgw_connection = bcgw_object.bcgw_connection_file
    
    #set up memory workspace
    MEMORY = 'in_memory'

    #Get current date and assign to the variable Date_Stamp for use later
    datetimestamp =(datetime.datetime.now())
    year  = str(datetimestamp.strftime("%Y"))
    month = str(datetimestamp.strftime("%m"))
    day   = str(datetimestamp.strftime("%d"))
    Date_Stamp = year + month + day
    print Date_Stamp

    # Use xlrd library to read the input table from the first sheet of the Excel file
    # The input list of datasets must be located in the first sheet in the Excel file
    workbook_lu = xlrd.open_workbook(input_table)
    sheet_lu = workbook_lu.sheet_by_index(0)
    rows_lu = sheet_lu.nrows

    
    # This section works through the rows of the look up table. Each row is a new dataset.
    # Only rows marked with a value of 1 in the first column will be processed. Any value other than 1 will skip that row's dataset.
    for row_lu in range(rows_lu):
        lu_check = sheet_lu.cell_value(row_lu, 0)
        if lu_check == 1:  # Only run rows from the sheet with the check value of 1
            # Read values from the Spreadsheet
            Group = "'" + sheet_lu.cell_value(row_lu, 1) + "'"
            GroupRank = sheet_lu.cell_value(row_lu, 2)
            SubGroup = "'" + sheet_lu.cell_value(row_lu, 3) + "'"
            SubGroupRank = sheet_lu.cell_value(row_lu, 4)
            Descr1 = sheet_lu.cell_value(row_lu, 5)
            Descr2 = sheet_lu.cell_value(row_lu, 6)

            SourceName = "'" + sheet_lu.cell_value(row_lu, 7) + "'"
            #Source = '"' + sheet_lu.cell_value(row_lu, 8) + '"'
            Source = 'r"{0}"'.format(sheet_lu.cell_value(row_lu, 8))  #reads source path as a raw string
            # If the source comes from the BCGW add the BCGW connection. If not then use path straight from the spreadsheet
            source1 = Source.count('WHSE')
            if source1 == 1:
                bcgw_data = bcgw_connection + "\\" + sheet_lu.cell_value(row_lu, 8)

            if source1 <> 1:
                bcgw_data = sheet_lu.cell_value(row_lu, 8)

            sel_string = sheet_lu.cell_value(row_lu, 9)
            final_out_layer = sheet_lu.cell_value(row_lu, 11)
            raw_out_layer = final_out_layer + "_raw"
            buf = sheet_lu.cell_value(row_lu, 12)
            temp_layer = "temp"
            temp2_layer = "temp2"

            print '\n ----'+Group
            print SubGroup
            print bcgw_data
            print sel_string
            print final_out_layer

            if run_lvl_1 == 'Y':
                # If a selection is required make selection and extract
                # If a buffer is required, extract to a temp dataset and then buffer
                # Selection strings for the OGC layers don't work when read from the lookup table so option below to hard code
                #sel_string = '"'+ 'OG_SLU_DES' + '"' + "=" +"'GEOPHYSICAL'"
                arcpy.MakeFeatureLayer_management(bcgw_data, "bcgw_layer")
                count = int(arcpy.GetCount_management('bcgw_layer').getOutput(0)) 
                print ' Make Feature Layer count (all records):', count
                
                if sel_string <> 'All':     # If selection criteria field in the table (column 9) isn't set to 'All', create a selection string
                    arcpy.SelectLayerByAttribute_management("bcgw_layer", "NEW_SELECTION", sel_string)
                    count = int(arcpy.GetCount_management('bcgw_layer').getOutput(0)) 
                    print ' SelectLayerByAttribute count:', count
                            
                if buf == 0:
                    arcpy.FeatureClassToFeatureClass_conversion("bcgw_layer", raw_output_gdb, temp2_layer)
                    arcpy.MakeFeatureLayer_management(temp2_layer, "dev_data")
                    count = int(arcpy.GetCount_management('dev_data').getOutput(0)) 
                    print ' DEV DATA count:', count
                    
                if buf <> 0:
                    arcpy.FeatureClassToFeatureClass_conversion("bcgw_layer", raw_output_gdb, temp_layer)
                    buf_input = os.path.join(raw_output_gdb, temp_layer)
                    buf_output = os.path.join(raw_output_gdb, temp2_layer)
                    arcpy.Buffer_analysis(buf_input, buf_output, buf, "FULL", "ROUND")
                    arcpy.MakeFeatureLayer_management(buf_output, "dev_data")
                    arcpy.Delete_management(buf_input)
                
                arcpy.Delete_management("bcgw_layer")
    
                        
                # Add and calculate new fields. Most of the new fields are calculated from existing fields in the raw datasets
                arcpy.AddField_management("dev_data", "CEF_DISTURB_GROUP", "TEXT", "", "", "30")
                arcpy.CalculateField_management("dev_data", "CEF_DISTURB_GROUP", Group, "PYTHON")
    
                arcpy.AddField_management("dev_data", "CEF_DISTURB_GROUP_RANK", "SHORT")
                arcpy.CalculateField_management("dev_data", "CEF_DISTURB_GROUP_RANK", GroupRank, "PYTHON")
    
                arcpy.AddField_management("dev_data", "CEF_DISTURB_SUB_GROUP", "TEXT", "", "", "60")
                arcpy.CalculateField_management("dev_data", "CEF_DISTURB_SUB_GROUP", SubGroup, "PYTHON")
    
                arcpy.AddField_management("dev_data", "CEF_DISTURB_SUB_GROUP_RANK", "SHORT")
                arcpy.CalculateField_management("dev_data", "CEF_DISTURB_SUB_GROUP_RANK", SubGroupRank, "PYTHON")
    
                # In some cases DESCR_1 is populated directly from the spreadsheet as a text string. This section allows that to happen
                arcpy.AddField_management("dev_data", "CEF_DISTURB_DESCR_1", "TEXT", "", "", "60")
                type1 = Descr1.count('"')
                if type1 == 2:
                    arcpy.CalculateField_management("dev_data", "CEF_DISTURB_DESCR_1", Descr1, "PYTHON")
                if type1<> 2:
                    field_name = "!" + Descr1 + "!"
                    arcpy.CalculateField_management("dev_data", "CEF_DISTURB_DESCR_1", field_name.encode("ascii"), "PYTHON")
    
                # In some cases DESCR_2 is populated directly from the spreadsheet as a text string. This section allows that to happen
                arcpy.AddField_management("dev_data", "CEF_DISTURB_DESCR_2", "TEXT", "", "", "60")
                subtype1 = Descr2.count('"')
                if subtype1 == 2:
                    arcpy.CalculateField_management("dev_data", "CEF_DISTURB_DESCR_2", Descr2, "PYTHON")
                if subtype1 <> 2:
                    field_name = "!" + Descr2 + "!"
                    arcpy.CalculateField_management("dev_data", "CEF_DISTURB_DESCR_2", field_name.encode("ascii"), "PYTHON")
    
                arcpy.AddField_management("dev_data", "SOURCE_SHORT_NAME", "TEXT", "", "", "60")
                arcpy.CalculateField_management("dev_data", "SOURCE_SHORT_NAME", SourceName, "PYTHON")
    
                arcpy.AddField_management("dev_data", "SOURCE", "TEXT", "", "", "240")
                arcpy.CalculateField_management("dev_data", "SOURCE", Source, "PYTHON")
    
                arcpy.AddField_management("dev_data", "CEF_EXTRACTION_DATE", "TEXT", "", "", "10")
                arcpy.CalculateField_management("dev_data", "CEF_EXTRACTION_DATE", Date_Stamp, "PYTHON")
    
                #Export "Raw" layer
                raw_output = os.path.join(raw_output_gdb, raw_out_layer)
                if arcpy.Exists(raw_output):
                    arcpy.Delete_management(raw_output)
                arcpy.FeatureClassToFeatureClass_conversion("dev_data", raw_output_gdb, raw_out_layer)
    
                count = int(arcpy.GetCount_management(raw_out_layer).getOutput(0)) 
                print ' RAW OUTPUT count:', count
            
            if run_lvl_2 == 'Y':
                print '\nDissolving Raw Inputs to create Level 2 outputs... '
                if not arcpy.Exists(output_gdb):
                    folder_output, file_output = os.path.split(output_gdb)
                    if not os.path.exists(folder_output):
                        os.makedirs(folder_output)
                    arcpy.CreateFileGDB_management(folder_output, file_output)
        
                # Dissolve to create Sub Group layer
                raw_output = os.path.join(raw_output_gdb, raw_out_layer)
                result = os.path.join(output_gdb, final_out_layer)
                if arcpy.Exists(result):
                    arcpy.Delete_management(result)
                arcpy.Dissolve_management(raw_output, result, ["CEF_DISTURB_GROUP", "CEF_DISTURB_GROUP_RANK", "CEF_DISTURB_SUB_GROUP", "CEF_DISTURB_SUB_GROUP_RANK", "CEF_DISTURB_DESCR_1", "CEF_DISTURB_DESCR_2", "SOURCE_SHORT_NAME", "SOURCE",  "CEF_EXTRACTION_DATE"],"", "SINGLE_PART")
                count = int(arcpy.GetCount_management(result).getOutput(0))
                print ' Dissolve Lvl2 count:', count
                
            #File Cleanup
            arcpy.Delete_management("dev_data")
            arcpy.Delete_management("temp2")
            arcpy.Delete_management("temp")


# This section combines the sub group datasets to create Group level datasets
# Datasets are combined based on rank order established in the lookup table
# DO NOT RUN Cutblocks - these can only be run by tile
if run_lvl_3 == 'Y':
    print '\nRunning LEVEL 3 - Create Groups by Sub-Group Rank Order  ...\n'

    if not arcpy.Exists(BCoutput_gdb):
        folder_output, file_output = os.path.split(BCoutput_gdb)
        if not os.path.exists(folder_output):
            os.makedirs(folder_output)
        arcpy.CreateFileGDB_management(folder_output, file_output)
                    
    workbook_lu = xlrd.open_workbook(input_table)
    sheet_lu = workbook_lu.sheet_by_index(0)
    rows_lu = sheet_lu.nrows

    for row_lu in range(rows_lu):
        lu_check = sheet_lu.cell_value(row_lu, 0)
        if lu_check == 1:  # Only run rows with the check value of 1
            # NEVER RUN CUTBLOCKS BECAUSE IT BREAKS ESRI - Set Do/Do Not to 0 in the extraction xls
            # Cutblocks are run by Tile, and then merged to create the provincial extent
            
            ## Read values from the Spreadsheet
            GroupCheck = sheet_lu.cell_value(row_lu, 1)
            
            # Read through the look up table for one group and add the layer name to the appropriate sub group attribute
            if CurrentGroup == "":
                CurrentGroup = GroupCheck
            
            print "Current Group:  " + CurrentGroup
            if CurrentGroup == GroupCheck:
                SubGroupRank = sheet_lu.cell_value(row_lu, 4)
                
                if SubGroupRank > maxrank:
                    maxrank = int(SubGroupRank)
            
                print " Current rank " +str(int(SubGroupRank))
                if SubGroupRank == 1:
                    level_B_input1 = sheet_lu.cell_value(row_lu, 11)
                if SubGroupRank == 2:
                    level_B_input2 = sheet_lu.cell_value(row_lu, 11)
                if SubGroupRank == 3:
                    level_B_input3 = sheet_lu.cell_value(row_lu, 11)
                if SubGroupRank == 4:
                    level_B_input4 = sheet_lu.cell_value(row_lu, 11)
                if SubGroupRank == 5:
                    level_B_input5 = sheet_lu.cell_value(row_lu, 11)

            
            # Once all the sub group names are set for a given group move into this section to update a dataset with the next highest ranked dataset
            # Use the ESRI Update geoprocessing tool to merge the sub groups in rank order
            final = "BC_CEF_" + CurrentGroup
            final_output = os.path.join(BCoutput_gdb, final)
            
            if arcpy.Exists(final_output):
                arcpy.Delete_management(final_output)
            
            if CurrentGroup <> GroupCheck:
                print "Number of ranked layers: " + str(maxrank)
                # If there is only one sub group dataset it gets exported with no updates
                if maxrank == 1:
                    print "Only one layer so no updates required"
                    update_output = os.path.join(output_gdb, level_B_input1)
                    print "Writing final result"                    
                    arcpy.FeatureClassToFeatureClass_conversion(update_output, BCoutput_gdb, final)
                    
                if maxrank > 1:
                    print "Start updates now"
                    inputlayer = "level_B_input" + str(maxrank)
                    inputlayer = os.path.join(output_gdb, eval(inputlayer))
                    updatelayer = "level_B_input" + str(maxrank-1)
                    updatelayer = os.path.join(output_gdb, eval(updatelayer))
                    temp_layer = "result"+str(int(maxrank))
                    update_output = os.path.join(output_gdb, temp_layer)
                    if arcpy.Exists(update_output):
                        arcpy.Delete_management(update_output)
                    arcpy.Update_analysis(inputlayer, updatelayer, update_output)
                    
                    maxrank = maxrank - 2
                    while maxrank > 0:
                        inputlayer = update_output
                        updatelayer = "level_B_input" + str(maxrank)
                        updatelayer = os.path.join(output_gdb, eval(updatelayer))
                        temp_layer = "result"+str(int(maxrank))
                        update_output = os.path.join(output_gdb, temp_layer)
                        if arcpy.Exists(update_output):
                            arcpy.Delete_management(update_output)
                        arcpy.Update_analysis(inputlayer, updatelayer, update_output)
                        arcpy.Delete_management(inputlayer)
                        maxrank = maxrank - 1

                    print "Writing final result"                    
                    arcpy.FeatureClassToFeatureClass_conversion(update_output, BCoutput_gdb, final)
                    arcpy.Delete_management(update_output)
                
                
                maxrank = 0
                SubGroupRank = sheet_lu.cell_value(row_lu, 4)
                maxrank = int(SubGroupRank)

                # If the script has moved to the next row and it is a different group then update the attribute with the new group
                # and set the "GroupCheck" to the new group name
                CurrentGroup = GroupCheck
                SubGroupRank = sheet_lu.cell_value(row_lu, 4)
                if SubGroupRank == 1:
                    level_B_input1 = sheet_lu.cell_value(row_lu, 11)
                if SubGroupRank == 2:
                    level_B_input2 = sheet_lu.cell_value(row_lu, 11)
                if SubGroupRank == 3:
                    level_B_input3 = sheet_lu.cell_value(row_lu, 11)
                if SubGroupRank == 4:
                    level_B_input4 = sheet_lu.cell_value(row_lu, 11)
                if SubGroupRank == 5:
                    level_B_input5 = sheet_lu.cell_value(row_lu, 11)
                print "Move to next group\n"

    
    # This section processes the final set of sub groups once the end of the lookup table is reached in the above loop
    print 'FINAL SUB GROUP Updates'
    print "Number of ranked layers: " + str(maxrank)
    if maxrank == 1:
        print "Only one layer so no updates required"
        update_output = os.path.join(output_gdb, level_B_input1)
        print "Writing final result"                    
        arcpy.FeatureClassToFeatureClass_conversion(update_output, BCoutput_gdb, final)
        
    if maxrank > 1:
        print "Start updates now"
        inputlayer = "level_B_input" + str(maxrank)
        inputlayer = os.path.join(output_gdb, eval(inputlayer))
        updatelayer = "level_B_input" + str(maxrank-1)
        updatelayer = os.path.join(output_gdb, eval(updatelayer))
        temp_layer = "result"+str(int(maxrank))
        update_output = os.path.join(output_gdb, temp_layer)
        if arcpy.Exists(update_output):
            arcpy.Delete_management(update_output)
        arcpy.Update_analysis(inputlayer, updatelayer, update_output)

        maxrank = maxrank - 2
        while maxrank > 0:
            inputlayer = update_output
            updatelayer = "level_B_input" + str(maxrank)
            updatelayer = os.path.join(output_gdb, eval(updatelayer))
            temp_layer = "result"+str(int(maxrank))
            update_output = os.path.join(output_gdb, temp_layer)
            if arcpy.Exists(update_output):
                arcpy.Delete_management(update_output)
            arcpy.Update_analysis(inputlayer, updatelayer, update_output)
            arcpy.Delete_management(inputlayer)
            maxrank = maxrank - 1

        print "Writing final result"
        arcpy.FeatureClassToFeatureClass_conversion(update_output, BCoutput_gdb, final)
        arcpy.Delete_management(update_output)
    
                
# This section dissolves the Group datasets to eliminate overlapping features
# Do not run CUTBLOCKS or Reserves - Cutblocks are run by Tile, and then merged to create the provincial extent
#  Reserves are considered undisturbed and part of the Natural landbase - consolidated in the tile output
# The two DESCR field are dissolved out.
if run_lvl_4 == 'Y':
    print '\nRunning LEVEL 4 - Group and Sub-Group dissolve  ...'

    if not arcpy.Exists(BC_Dissolve_gdb):
        folder_output, file_output = os.path.split(BC_Dissolve_gdb)
        if not os.path.exists(folder_output):
            os.makedirs(folder_output)
        arcpy.CreateFileGDB_management(folder_output, file_output)
        
    #Dissolve for Level 4
    #read all features in level 3 to create list to dissolve on  - EXCEPT CUTBLOCKS - which need to be merged by tile (dissolve takes too long)
    arcpy.env.workspace = BCoutput_gdb

    
    fcList = arcpy.ListFeatureClasses('BC_CEF_*')
    # ---------------------------------------------------------------------
    # Dissolving cutblocks might crash, so remove it from the dissolve list
    # ---------------------------------------------------------------------
    #if 'BC_CEF_Cutblocks' in fcList:
    #    fcList.remove('BC_CEF_Cutblocks')
    print fcList

    for inFC in fcList:
        dissolveResult = os.path.join(BC_Dissolve_gdb, inFC+'_dissolve')
        #if arcpy.Exists(dissolveResult):
            #arcpy.Delete_management(dissolveResult)
        if not arcpy.Exists(dissolveResult):
            print 'Dissolving .....  '+inFC
            arcpy.Dissolve_management(inFC, dissolveResult, ["CEF_DISTURB_GROUP", "CEF_DISTURB_GROUP_RANK", "CEF_DISTURB_SUB_GROUP", "CEF_DISTURB_SUB_GROUP_RANK", "SOURCE_SHORT_NAME", "SOURCE", "CEF_EXTRACTION_DATE"],"", "SINGLE_PART")
        else:
            print dissolveResult + ' already Exists!'



print '\nSCRIPT COMPLETE'
totalTime = time.strftime("%H:%M:%S",time.gmtime(time.time() - startTime))
print '\n This script took ' + totalTime + ' to run.'


