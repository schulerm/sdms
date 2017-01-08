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

import logging
import logging.config

sys.path.insert(0, '/Assets/sharedLibraries')
import parseHelper
import databaseHelper

def main(args):

    DOMAIN = 'ITD'
    TASKLIST = 'default'
    VERSION = '1'

    taskName = os.path.basename(__file__)[:-3]

    logging.config.fileConfig('/Assets/sharedLibraries/logging_config.ini')
    logging.debug("Creating SWF boto client")
    botoConfig = Config(connect_timeout=50, read_timeout=70) # suggestion is the read is higher than connect
    swf = boto3.client('swf', config=botoConfig)
    logging.debug("Created SWF boto client: %s", swf)
    

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
            asset = INPUT['asset']
            dbPrimaryKey = INPUT['dbPrimaryKey']
            
            BUCKETNAME = "schulerfiles"

            logging.debug("[%s] Creating S3 bucket boto client", workID)
            s3 = boto3.resource('s3')
            bucket = s3.Bucket(BUCKETNAME)
            logging.debug("[%s] Creating S3 bucket boto client", workID)
            
            filePath, fileName, fileExt = parseHelper.splitFilename(asset)
            # fileName will serve as the FOLDERNAME
            # ext isn't not needed
            # filePath needs to be broken up if we want to organize things by account
            
            #b = bucket.new_key(folder)
            
            for dirName, dirList, fileList in os.walk(filePath):
                logging.info("[%s] Distributing file: %s", workID, asset)
                key = os.path.split(dirName)[1]

                for f in fileList:
                    upfile = os.path.join(dirName,f)
                    s3key = os.path.join(fileName, key, f)
                    logging.debug("[%s] Started uploading %s with key: %s", workID, upfile, s3key)
                    bucket.upload_file(upfile, s3key, {'ServerSideEncryption' :'AES256' })
                    logging.debug("[%s] Completed uploading %s with key: %s", workID, upfile, s3key)
                

            # Start setting the parameters needed to update the thumbnail
            updateExpression = 'set File_Location = :d'
            
            expressionValues = {
                ':d' : 'CDN'
            }
            # Call the update function
            logging.debug("[%s] Setting location to CDN for checksum: %s", workID, dbPrimaryKey)
            response = databaseHelper.updateEntry(dbPrimaryKey, updateExpression, expressionValues)       
            OUTPUT = {
                    'result' : 'success',
                    'dbPrimaryKey' : dbPrimaryKey,
                    'assetClass' : INPUT['assetClass'], 
                    'asset' : asset,
            }

            swf.respond_activity_task_completed(
                taskToken = taskToken,
                result = json.dumps(OUTPUT)
            )
                    
            logging.info("[%s] %s Complete", workID, taskName)

if __name__ == '__main__':
    
    main(sys.argv)
