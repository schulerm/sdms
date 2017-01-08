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

    ARN = "arn:aws:states:us-east-1:497940546915:activity:distributeToS3"
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

            # When two parallel tasks complete (like in the video section), we'll have two inputs
            # The try / except will handle this by assinging the input array the first of the series
            
            INPUT = json.loads(task['input'])

            try:
                asset = INPUT['asset']
            except TypeError:
                INPUT = INPUT[0]
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

            sfn.send_task_success(
                taskToken=taskToken,
                output=json.dumps(OUTPUT)
            )
                    
            logging.info("[%s] %s Complete", workID, taskName)

if __name__ == '__main__':
    
    main(sys.argv)
