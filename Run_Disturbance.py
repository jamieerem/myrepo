import arcpy
import os
import re
import json
import logging
import smtplib
import socket
import pandas as pd
import pandasql
## If you get a warning about pandas not exisitng/installed write this line of code in the termianl and run it
## python -m pip install "pandasql"

from arcpy import env
from disturbance_layer import disturbance_aoi, buffer_disturbance, intersect, delete, interim_clean_up, delete_layers, disturbance_flatten, disturbance_field_mapping, disturbance_cleanup, disturbance_buffer_flatten,disturbance_buffer_field_mapping, disturbance_buffer_cleanup, identity
from table_create import combine_loose_sheets, make_sheet_base, static_grouping
from protection_layer import protect_aoi, gather_protection, flatten_protection, field_mapping, clean_and_join, combine
from protection_table import tabletotable, combine_loose_herds, protection_grouping, protection_classes
from disturbance_protection_combine import combine_disturbance_and_protection, clean_up
configFile = r"\\spatialfiles2.bcgov\WORK\FOR\RNI\RNI\Restricted\Caribou_Recovery\projects\22_Chase_Core_disturbance\config_disturbance_Chase.json"

arcpy.env.overwriteOutput = True

def readConfig(configFile):#returns dictionary of parameters
    """
    reads the config file to dictionary
    """
    with open(configFile) as json_file:
        try:
            d = json.load(json_file)
        except:
            print ("failed to parse configuration")
        else:
            return d['params']
def layers():
    cfg = readConfig(configFile)
    ####
    connPath = cfg[0]['connPath']
    connFile = cfg[0]['connFile']
    username = cfg[0]['username']
    password = cfg[0]['password']
    aoi_location = cfg[0]["aoi_location"]
    layer_name = cfg[0]["layer_name"]
    unique_value = cfg[0]["unique_value"]
    dissolve_values = cfg[0]["dissolve_values"]

    disturbance_aoi(connPath, connFile, username, password, aoi_location, layer_name, unique_value, roads_file, bcce_file)
    buffer_disturbance()
    intersect(unique_value, aoi_location, layer_name, dissolve_values)
    delete()
    interim_clean_up(dissolve_values)
def spagh_meatball():

    aoi = (aoi_location + layer_name)
    search_word = "{}".format(unique_value)

    with arcpy.da.SearchCursor(aoi, [search_word]) as cursor:
        values_sorted = sorted({row[0] for row in cursor})
    print('Running disturbance on: {}'.format(values_sorted))
    
    for values in values_sorted:
        layer_query = """{0} = '{1}'""".format(unique_value, values)
        layer_select = arcpy.SelectLayerByAttribute_management(aoi, "NEW_SELECTION", layer_query)
        arcpy.CopyFeatures_management(layer_select, 'aoi')

        (print('Selected {}'.format(values)))

        value_update = values.replace(" ", "")
        value_update = value_update.replace("-", "") 
        value_update = value_update.replace(":", "") 
        value_update = value_update.replace("/", "") 


        disturbance_flatten(values, value_update)
        disturbance_field_mapping(values, value_update)
        disturbance_cleanup(values, value_update, keep_list)

        delete_layers()

        disturbance_buffer_flatten(values, value_update)
        disturbance_buffer_field_mapping(values, value_update)
        disturbance_buffer_cleanup(values, value_update, keep_list)

        delete_layers()

        identity(csv_dir, values, value_update, unique_value, intersect_layer, aoi_location)
def table():
    combine_loose_sheets(csv_dir, csv_output_name)
    make_sheet_base(intersect_layer, unique_value, aoi_location, csv_dir)
    static_grouping(csv_dir, csv_output_name, table_group, final_output)
def protection():
    protect_aoi(aoi_location, layer_name, unique_value)
    
    aoi = (aoi_location + layer_name)
    search_word = "{}".format(unique_value)

    with arcpy.da.SearchCursor(aoi, [search_word]) as cursor:
        values_sorted = sorted({row[0] for row in cursor})
    print('Running protection on: {}'.format(values_sorted))

    for values in values_sorted:
        layer_query = """{0} = '{1}'""".format(unique_value, values)
        layer_select = arcpy.SelectLayerByAttribute_management(aoi, "NEW_SELECTION", layer_query)
        arcpy.CopyFeatures_management(layer_select, 'aoi')

        (print('Selected {}'.format(values)))

        value_update = values.replace(" ", "")
        value_update = value_update.replace("-", "") 
        value_update = value_update.replace(":", "") 
        value_update = value_update.replace("/", "") 

        gather_protection(designated_lands, value_update)
        flatten_protection(value_update)
        field_mapping(value_update)
        clean_and_join(value_update, keep_list)
        combine(values, value_update, unique_value, intersect_layer, aoi_location)
def protection_table():
    aoi = (aoi_location + layer_name)
    search_word = "{}".format(unique_value)

    with arcpy.da.SearchCursor(aoi, [search_word]) as cursor:
        values_sorted = sorted({row[0] for row in cursor})
    print('Running protection on: {}'.format(values_sorted))
    
    for values in values_sorted:
        (print('Selected {}'.format(values)))

        value_update = values.replace(" ", "")
        value_update = value_update.replace("-", "") 
        value_update = value_update.replace(":", "") 
        value_update = value_update.replace("/", "") 

        tabletotable(value_update, csv_dir)
        combine_loose_herds(csv_dir, value_update, csv_protect_output)
        make_sheet_base(intersect_layer, unique_value, aoi_location, csv_dir)
        protection_grouping(csv_dir, csv_protect_output, table_group)
        protection_classes(csv_dir, csv_protect_output, table_group)
########
readConfig(configFile)
cfg = readConfig(configFile)
#####################################
workspace = cfg[0]['workspace']
connPath = cfg[0]['connPath']
connFile = cfg[0]['connFile']
username = cfg[0]['username']
password = cfg[0]['password']
aoi_location = cfg[0]["aoi_location"]
layer_name = cfg[0]["layer_name"]
unique_value = cfg[0]["unique_value"]
dissolve_values = cfg[0]["dissolve_values"]
keep_list = cfg[0]["keep_list"]
csv_dir = cfg[0]["csv_dir"]
intersect_layer = cfg[0]["intersect_layer"]
csv_output_name = cfg[0]["csv_output_name"]
table_group = cfg[0]["table_group"]
final_output = cfg[0]["final_output"]
csv_protect_output = cfg[0]["csv_protect_output"]
designated_lands = cfg[0]["designated_lands"]
roads_file = cfg[0]["roads_file"]
bcce_file = cfg[0]["bcce_file"]
######################################
arcpy.env.workspace = workspace
arcpy.env.overwriteOutput = True
######################################
layers()
spagh_meatball()
table()
protection()
protection_table()
