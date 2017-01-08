""" This function accepts and asset, uploads it to S3, and adds a registration flag


author: Michael Schuler [mischuler@deloitte.com]

"""

import boto3
from botocore.client import Config
import botocore
import sys
import os
import string
import simplejson as json
import time

import logging
import logging.config

sys.path.insert(0, '/Assets/sharedLibraries')
import parseHelper
import databaseHelper

def main(args):

    DOMAIN = 'ITD'
    VERSION = '1'

    taskName = os.path.basename(__file__)[:-3]

    logging.config.fileConfig('/Assets/sharedLibraries/logging_config.ini')
    logging.debug("Creating SWF boto client")
    botoConfig = Config(connect_timeout=50, read_timeout=70) # suggestion is the read is higher than connect
    swf = boto3.client('swf', config=botoConfig)
    logging.debug("Created SWF boto client: %s", swf)

    BUCKETNAME = "schulerfiles"
    workingStorage = "/Assets/working/"

    while True:

        task = swf.poll_for_activity_task(
            domain=DOMAIN,
            taskList={'name': taskName},
            identity='%s-01' %(taskName)
        )

        if 'taskToken' not in task:
            logging.info("%s - Poll timed out, no new task. Repoll", taskName)
        
        # Run the operation
        else:
            taskToken = task['taskToken']
            workID = task['workflowExecution']['workflowId']
            logging.info("[%s] New request for %s", workID, taskName)

            INPUT = json.loads(task['input'])
            
            source = INPUT['locationSource']
            destination = INPUT['locationDestination']
            dbPrimaryKey = INPUT['dbPrimaryKey']
            fileKey = INPUT['fileKey'] + '/'
            
            # Bucket object is necessary in all cases
            logging.debug("[%s] Creating S3 bucket boto client", workID)
            s3 = boto3.resource('s3')
            bucket = s3.Bucket(BUCKETNAME)
            logging.debug("[%s] Created S3 bucket boto client", workID)
            
            # Setting the storage class to be used for later
            s3StorageClass = 'STANDARD'
            if destination == 'near_line':
                s3StorageClass = 'STANDARD_IA'
            
            logging.info("[%s] Moving %s from %s to %s", workID, fileKey, source, destination)
            # CDN and near_line are both S3 tiers, so all we are doing is changing the Storage Class with a PUT
            if (source == 'CDN' and destination == 'near_line') or (source == 'near_line' and destination == 'CDN'):
            
                logging.info("[%s] Moving objects between S3 and S3IA", workID)
                for obj in bucket.objects.filter(Prefix=fileKey):
                    logging.debug("[%s] Moving object %s from %s to %s object: ", workID, obj.key, source, destination)
                    
                    copy_source = {
                        'Bucket' : bucket.name,
                        'Key' : obj.key
                    }
                    
                    response = s3.meta.client.copy_object(
                        CopySource=copy_source, 
                        Bucket=bucket.name,
                        Key=obj.key, 
                        StorageClass=s3StorageClass
                        )
                    logging.debug("[%s] Object moved: ", workID, response)
                    
                OUTPUT = {
                    'result' : 'success',
                }
            
            # If we need to move to or restore from archive, we need to run the whole gamut
            elif 'archive' in [source, destination]: #Glacier
                
                # Create Glacier object
                
                
                # Create directory in working storage
                subDir = parseHelper.createDir(workingStorage, fileKey)
            
                # Pull down from glacier
                if source == 'archive':
                    logging.info("[%s] Moving asset from Glacier", workID)
                else:
                    logging.info("[%s] Begin moving objects to Glacier", workID)
                    logging.info("[%s] Begin object download", workID)
                    # Download object to the working storage subdirectory
                    # Upload files back up to the same fileKey (this takes Accounts into consideration as well)
                    for obj in bucket.objects.filter(Prefix=fileKey):
                        logging.info("[%s] Downloading %s to temporary storage", workID, obj.key)
                        fileName = os.path.join(workingStorage,obj.key)
                        if not os.path.exists(os.path.dirname(fileName)):
                            try:
                                os.makedirs(os.path.dirname(fileName))
                            except OSError as exc: # Guard against race condition
                                if exc.errno != errno.EEXIST:
                                    raise
                        
                        s3.Object(bucket.name, obj.key).download_file(fileName) # Create directories as needed here
                        
                    
                    logging.info("[%s] Begin object upload to glacier", workID)
                
                # Output needs the temporary storage location to clean up
                # cleanUpLandingPads expects an ASSET (e.g., /Assets/working/file.ext), and not just a path. We will provide a dummy asset
                OUTPUT = {
                    'result' : 'success',
                    'asset' : '%sdummy.file' % (subDir)
                }
            
            AUDIT = {}
            AUDIT['User'] = 'System'
            AUDIT['Timestamp'] = time.strftime("%Y-%m-%dT%H:%M:%S+0000",time.gmtime())
            AUDIT['Action'] = 'Asset moved from %s from %s' % (source, destination)
            AUDIT['Notes'] = workID
            
            # Add the Audit Dictionary to a list so that we can append it
            aLIST = []
            aLIST.append(AUDIT)
    
            updateExpression = 'set File_Location = :d, Audit = list_append(Audit, :a)'
            
            expressionValues = {
                ':d' : destination,
                ':a' : aLIST
            }
            # Call the update function
            logging.debug("[%s] Updating the asset location and history: %s", workID, destination)
            response = databaseHelper.updateEntry(dbPrimaryKey, updateExpression, expressionValues)       
            
            OUTPUT.update(INPUT)

            swf.respond_activity_task_completed(
                taskToken = taskToken,
                result = json.dumps(OUTPUT)
            )
                    
            logging.info("[%s] %s Complete", workID, taskName)

if __name__ == '__main__':
    
    main(sys.argv)
