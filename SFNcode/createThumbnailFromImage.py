""" This function creates a thumbnail from an image and registers the thumbnail with the DB

Function will take the following steps:
    Create a thumbnail directory
    Utilize ImageMagick to create a thumbnail
    Update the database entry with a Thumbnail section
    Return success or failure

author: Michael Schuler [mischuler@deloitte.com]

"""

import boto3
from botocore.client import Config
import botocore
import sys
import os
import subprocess
import string
import simplejson as json

import logging
import logging.config

sys.path.insert(0, '/Assets/sharedLibraries')
import parseHelper
import databaseHelper

def main(args):

    ARN = "arn:aws:states:us-east-1:497940546915:activity:createThumbnailFromImage"
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
            
            INPUT = json.loads(task['input'])
            asset = INPUT['asset']
            dbPrimaryKey = INPUT['dbPrimaryKey']
             # Take the thumbnail 25% through the video
            scale = "640x360"
            newDir = "thumbnails"
            (filePath, fileName, fileExt) = parseHelper.splitFilename(asset)
            subDir = parseHelper.createDir(filePath, newDir)
            outfile = '%s_thumbnail.jpg' % (fileName)

            cmd = ['convert'
                    , asset
                    ,'-format', 'jpg'
                    ,'-thumbnail', scale
                    ,'-auto-orient'
                    ,'%s/%s' %(subDir, outfile)
                  ]

            logging.debug("[%s] Execute image thumbnail creation: %s", workID, cmd)
            try:
                output = subprocess.check_output(cmd)
                
                # Start setting the parameters needed to update the thumbnail 
                key = dbPrimaryKey
                
                updateExpression = 'set thumbnail = :t'
                
                expressionValues = {
                    ':t' : '/%s/%s'%(newDir, outfile)
                }
                
                logging.debug("[%s] Update thumbnail value", workID)
                # Call the update function
                response = databaseHelper.updateEntry(key, updateExpression, expressionValues)
                
                OUTPUT = {
                    'tool' : output,
                    'dbPrimaryKey' : dbPrimaryKey,
                    'assetClass' : INPUT['assetClass'], 
                    'asset' : asset,
                }

                sfn.send_task_success(
                    taskToken=taskToken,
                    output=json.dumps(OUTPUT)
                )
            # We should catch other errors here
            except subprocess.CalledProcessError as err:
                
                result = { 
                    'reason' : 'THB-0001_Error in image thumbnail creation',
                    'detail' : str(err)
                }
                
                logging.error("%s", result)
                
                sfn.send_task_failure(
                    taskToken=taskToken,
                    error=json.dumps(result['reason']),
                    cause=json.dumps(result['detail'])
                )
                    
            logging.info("[%s] %s Complete", workID, taskName)

if __name__ == '__main__':
    
    main(sys.argv)
