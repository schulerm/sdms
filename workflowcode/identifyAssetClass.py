import boto3
from botocore.client import Config
import sys
import os
import simplejson as json
import logging
import logging.config

sys.path.insert(0, '/Assets/sharedLibraries/')
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
    
    # Should this be moved to the loop? Dyanmically change the item?
    EXT = loadExts()
    logging.info("Extension listing: %s", EXT)
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
            
            logging.info(task)
            taskToken = task['taskToken']
            workID = task['workflowExecution']['workflowId']
            
            logging.info("[%s] New request for %s", workID, taskName)
            logging.debug("[%s] Input: %s", workID, task['input'])
            parameters = json.loads(task['input'])
            asset = parameters['asset']

             # get the extensions
            filePath, fileName, fileExt = parseHelper.splitFilename(asset)
            # get the AssetClass
            # Extension will return with the period. We must nix this from the front
            try:
                assetClass = EXT[fileExt[1:].lower()]
            except KeyError:
                assetClass = "Other"
                logging.debug("[%s] File extension NOT found in list: %s", taskToken, fileExt)

            # metadata and asset are passthrough
            result = { 
                'assetClass' : assetClass, 
                'metadata' : parameters['metadata'], 
                'asset' : asset,
            }
            swf.respond_activity_task_completed(
                taskToken=task['taskToken'],
                result=json.dumps(result)
            )
        
            logging.info("[%s] %s Complete", workID, taskName)



def loadExts():

    # Configure files written outside of this directory
    # Read in all files
    # Remove the newline
    # Add them to a big dictionary so that the key provides the type

    EXTENSIONS = {}
    Types = ['Audio','Image','Video']

    logging.debug("Beginning loop to load extension configuration")
    for t in Types:
        fn = "Type%s" % t
        logging.debug("Opening filename: %s", fn)
        f = open(fn,'r')
        exts = f.readlines()
        for e in exts:
            logging.debug("Adding %s to %s type", e[:-1], t)
            EXTENSIONS[e[:-1]] = t

    return EXTENSIONS


if __name__ == '__main__':
    
    main(sys.argv)
