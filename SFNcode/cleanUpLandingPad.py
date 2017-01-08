""" Deletes all files from the landing area


author: Michael Schuler [mischuler@deloitte.com]

"""

import boto3
from botocore.client import Config
import botocore
import sys
import os
import string
import simplejson as json
import shutil

import logging
import logging.config

sys.path.insert(0, '/Assets/sharedLibraries')
import parseHelper


def main(args):

    ARN = "arn:aws:states:us-east-1:497940546915:activity:cleanUpLandingPad"
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

            filePath, fileName, fileExt = parseHelper.splitFilename(asset)
            
            logging.debug("[%s] Begin file tree deletion: %s", workID, filePath)
            shutil.rmtree(filePath)
            logging.debug("[%s] Completed file tree deletion: %s", workID, filePath)
            
            OUTPUT = {
                'result' : 'success',
            }
            # As this activitiy is used by multiple workflows, we want to pass the INPUT parameters back
            OUTPUT.update(INPUT)

            sfn.send_task_success(
                taskToken=taskToken,
                output=json.dumps(OUTPUT)
            )
                    
            logging.info("[%s] %s Complete", workID, taskName)

if __name__ == '__main__':
    
    main(sys.argv)
