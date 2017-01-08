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
            
            # 
            if source in ['CDN', 'near_line']:
                
                BUCKETNAME = "schulerfiles"

                logging.debug("[%s] Creating S3 bucket boto client", workID)
                s3 = boto3.resource('s3')
                bucket = s3.Bucket(BUCKETNAME)
                logging.debug("[%s] Created S3 bucket boto client", workID)
                
                for obj in bucket.objects.filter(Prefix=fileKey):
                    logging.debug("[%s] Deleting object: %s", workID, obj.key)
                    s3.Object(bucket.name, obj.key).delete()
            
            else: #Glacier
                continue # Add logic for glacier once supported
            
            AUDIT = {}
            AUDIT['User'] = 'System'
            AUDIT['Timestamp'] = time.strftime("%Y-%m-%dT%H:%M:%S+0000",time.gmtime())
            AUDIT['Action'] = 'Asset removed from %s' % (source)
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
            OUTPUT = {
                    'result' : 'success',
            }

            swf.respond_activity_task_completed(
                taskToken = taskToken,
                result = json.dumps(OUTPUT)
            )
                    
            logging.info("[%s] %s Complete", workID, taskName)

if __name__ == '__main__':
    
    main(sys.argv)
