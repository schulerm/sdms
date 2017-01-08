import boto3
from botocore.client import Config
import sys
import os
import simplejson as json
import subprocess
import time
import string

import logging
import logging.config

sys.path.insert(0, '/Assets/sharedLibraries/')
import parseHelper

def main(args):

    ARN = "arn:aws:states:us-east-1:497940546915:activity:extractExifMetadata"
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
        # Input to the function includes:
        # path to file
        # user entered Metadata
        # assetClass

        else:
            taskToken = task['taskToken']
            workID = task['ResponseMetadata']['RequestId']
            logging.info("[%s] New request for %s", workID, taskName)
            INPUT = json.loads(task['input'], use_decimal=True)
            
            # processFile will returned a combined metadata to be registered
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
    # -j is used for JSON
    # -d formats the dates into standard UTC
    # -c formats the GPS to decimal
    cmd = ['exiftool','-j','-d','"%Y-%m-%dT%H:%M:%S+0000"','-c','"%+.8f"',asset]
    output = subprocess.check_output(cmd)

    G,I  = parseMediaToDict(output)

    # Start Constructing Document
    DOC = {}
    DOC['Filename'] = fileName
    DOC['Extension'] = fileExt
    DOC['Asset_Class'] = assetClass
    DOC['Checksum'] = sha1
    DOC['Imported_Time'] = time.strftime("%Y-%m-%dT%H:%M:%S+0000",time.gmtime())
    DOC['General'] = G
    DOC['Image'] = I
    DOC['File_Location'] = 'working'

    # Run a small post processing for GPS. We need to turn lat/lon into a "location point" for elastic search indexing
    try:
        LOC = {'lat' : DOC['General']['Latitude'], 'lon' : DOC['General']['Longitude']}
        DOC['General']['deviceLocation'] = LOC
    except KeyError:
        pass
    
    
    for m in METADATA:
        DOC[m] = METADATA[m]

    return DOC

def initMapping():

    # Assume everything is in image with the exception of the following
    # Longtitude
    # Latitude
    # Altitude
    # Make
    # Model
    # Recorded date (DateTimeoriginal)
    # Filesize

    M = {}

    M['GPSLatitude'] = 'Latitude'
    M['GPSAltitude'] = 'Altitude'
    M['GPSLongitude'] = 'Longitude'
    M['Make'] = 'make'
    M['Model'] = 'model'
    M['Software'] = 'software'
    M['GPSDateTime'] = 'recorded_date'
    #M['DateTimeOriginal'] = 'Recorded date' # This field is not 100% accurate
    M['FileSize'] = 'file_size'

    return M

def parseMediaToDict(info):

    # Two dictionaries needed
    # General
    # Image

    # File is loaded as a JSON
    # We will take all entries with the exception of the embedded BINARY thumbnail
    # FILE CAN BE LOADED AS A JSON
    # Assume everything is in image with the exception of the following
   
    G = {}
    I = {}

    MAPPING = initMapping()

    I = json.loads(info, use_decimal=True)[0]

    # Altitude information comes back with string information
    # This will adjust the Altitude to a pure number
    # If no altitude is present, skip this
    try:
        I['GPSAltitude'] = string.split(I['GPSAltitude']," ")[0]
    except KeyError:
        pass

    # Remove Thumbail
    I.pop("ThumbnailImage",None)

    # The JSON returned data has quotes around keys. This will parse them out
    for (k,v) in I.items():
        try:
            I[k] = v.replace('"','')
        except AttributeError:
            continue

    # There are certain fields we want consitently mapped across images and videos that are defined in the MAPPING dict
    # This will assign those fields to the General track, and leave the reamaining fields
    for (k,v) in MAPPING.items():
        try:
            G[v] = I[k]
            I.pop(k)
        except KeyError:
            continue

   # Performs address lookup if we have Lat/Long
    try:
        G['Address'] = parseHelper.reverseLookup(G['Latitude'],G['Longitude'])[0]['formatted_address']
    except KeyError:
        pass

    # If we don't get a GPS date, use the Original date
    # Otherwise, we don't have any recorded date present
    try:
        if 'recorded_date' not in G:
            G['recorded_date'] = I['DateTimeOriginal']
    except KeyError:
        pass

    return G,I


if  __name__ == '__main__':
    
    main(sys.argv)
