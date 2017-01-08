# -*- coding: utf-8 -*-

import boto3
from botocore.client import Config
import sys
import os
import subprocess
import string
import time
import simplejson as json
from pymediainfo import MediaInfo
import fnmatch

import logging
import logging.config

sys.path.insert(0, '/Assets/sharedLibraries/')
import parseHelper

def main(args):

    ARN = "arn:aws:states:us-east-1:497940546915:activity:extractMediaInfoMetadata"
    taskName = os.path.basename(__file__)[:-3]

    logging.config.fileConfig('/Assets/sharedLibraries/logging_config.ini')
    logging.debug("Creating SFN boto client")
    botoConfig = Config(connect_timeout=50, read_timeout=70) # suggestion is the read is higher than connect
    sfn = boto3.client('stepfunctions', config=botoConfig)
    logging.debug("Created SFN boto client: %s", sfn)
    

    while True:

        task = sfn.get_activity_task(
            activityArn=ARN,
            workerName='%s-01' %(taskName)
        )

        if 'taskToken' not in task:
            logging.info("%s - Poll timed out, no new task. Repoll", taskName)
        
        # Run the operation
        else:
            taskToken = task['taskToken']
            workID = task['ResponseMetadata']['RequestId']
            logging.info("[%s] New request for %s", workID, taskName)
            
            INPUT = json.loads(task['input'], use_decimal=True)

            DOC = processFile(INPUT['asset'], INPUT['metadata'], INPUT['assetClass'])
            OUTPUT = {
                'DOC' : DOC,
                'assetClass' : INPUT['assetClass'], 
                'asset' : INPUT['asset'],
            }

            sfn.send_task_success(
                taskToken=taskToken,
                output=json.dumps(OUTPUT)
            )
        
            logging.info("[%s] %s Complete", workID, taskName)

def processFile(asset, METADATA, assetClass):
    # For each asset we need
    # Account
    # User
    # DateTime Added
    # Sha1
    # Filename
    # General Metadata
    # Audio Track
    # Video Track

    sha1 = parseHelper.computeChecksum(asset)
    (filePath, fileName, fileExt) = parseHelper.splitFilename(asset)
    
    G,V,A = parseMediaToDict(asset)

    # Start Constructing Document
    DOC = {}
    DOC['Filename'] = fileName
    DOC['Extension'] = fileExt
    DOC['Checksum'] = sha1
    DOC['Imported_Time'] = time.strftime("%Y-%m-%dT%H:%M:%S+0000",time.gmtime())
    DOC['General'] = G
    DOC['Video'] = V
    DOC['Audio'] = A
    DOC['Asset_Class'] = assetClass
    DOC['File_Location'] = 'working'
    
    # Add other metadata to the track
    logging.debug("Loading user metadata")
    for m in METADATA:
        logging.debug("Key found: %s", m)
        DOC[m] = METADATA[m]

    # Run a small post processing for GPS. We need to turn lat/lon into a "location point" for elastic search indexing
    try:
        LOC = {'lat' : DOC['General']['Latitude'], 'lon' : DOC['General']['Longitude']}
        DOC['General']['deviceLocation'] = LOC
    except KeyError:
        pass
    
    return DOC

def initMapping():
    M = {}
    M['comapplequicktimemake'] = "make"
    M['comapplequicktimemodel'] = "model"
    M['comapplequicktimecreationdate'] = "recorded_date"
    M['comapplequicktimesoftware'] = "software"
    M['comapplequicktimelocationiso6709'] = "xyz"
    
    return M

    
def parseMediaToDict(asset):
    
    # Three dictionaries needed
    # General
    # Video
    # Audio
    
    G = {}
    V = {}
    A = {}

    DATES = ['encoded_date','tagged_date','file_last_modification_date']
    GPS = ['xyz']
    MAPPING = initMapping()

    # Object returned will be separated into tracks -- General, Video, Audio, Video #1...n, Audio #1...n
    # Using "to_data" loads the track information into a dictionary
    # We want to loop through the items and sanitize them
    # NOTE: If there are multiple audio or video tracks, all of them will be overwritten except the last
    # TODO: Identify the number of tracks ahead of time and add each track_id/type to a running dictionary
    
    MI = MediaInfo.parse(asset)
    for track in MI.tracks:
        #print "NEW TRACK %s" %(track.track_type)
        TRACKDATA = track.to_data()
        trackType =  track.track_type # General, Audio, Video
        for (key,value) in TRACKDATA.items():
            if key[:5] != 'other':
                # Run the key against the MAPPING Dictionary
                try:
                    key = MAPPING[key]
                except KeyError:
                    pass

                # GPS is an exception
                # If we get GPS, this is part of general and we need to add Lat/Long and Alt
                # If we find it, then SKIP
                if key in GPS:
                    latitude, longitude, altitude = parseHelper.parseGPS(value)
                
                    if 'Latitude' not in G:
                        G['Latitude']=latitude
                    
                    if 'Longitude' not in G:
                        G['Longitude']=longitude
                    
                    if altitude != '':
                        if 'Altitude' not in G:
                            G['Altitude']=altitude
                    
                    address = parseHelper.reverseLookup(latitude, longitude)
                    if 'Address' not in G:
                        G['Address']=address[0]['formatted_address']
                    continue

                # Date Change
                if key in DATES:
                    value = parseHelper.parseDate(value)
                    
                # Pick the dictionary to use
                if trackType == 'General':
                    G[key] = value
                elif trackType == 'Video':
                    V[key] = value
                elif trackType == 'Audio':
                    A[key] = value
                '''elif fnmatch.fnmatch(trackType,'Video #*'): # Captures Video1...n
                    try:
                        V[trackType].update({key: value}) # only works if we've added a key.
                    except KeyError:
                        V[trackType] = {key : value}
                elif fnmatch.fnmatch(trackType,'Audio #*'): # Captures Audio1...n
                    try:
                        A[trackType].update({key : value})
                    except KeyError:
                        A[trackType] = {key : value}'''

    return G,V,A


if __name__ == '__main__':
    
    main(sys.argv)
