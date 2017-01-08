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

            filePath, fileName, fileExt = parseHelper.splitFilename(asset)
            
            logging.debug("[%s] Begin file tree deletion: %s", workID, filePath)
            shutil.rmtree(filePath)
            logging.debug("[%s] Completed file tree deletion: %s", workID, filePath)
            
            OUTPUT = {
                'result' : 'success',
            }
            # As this activitiy is used by multiple workflows, we want to pass the INPUT parameters back
            OUTPUT.update(INPUT)

            swf.respond_activity_task_completed(
                taskToken = taskToken,
                result = json.dumps(OUTPUT)
            )
                    
            logging.info("[%s] %s Complete", workID, taskName)

if __name__ == '__main__':
    
    main(sys.argv)
