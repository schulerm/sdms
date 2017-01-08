""" This decider runs the default workflow

The workflow performs the following tasks:
    Identifies what the uploaded files class is (video, image, audio, other)
    Extracts the relevant metadata from the determined file type, and combines it with incoming metadata
    Registers the asset and metadata with the database
    Creates a thumbnail (if necessary)
    Transcode to a standard format (if necessary)
    Upload the file to the S3 store
    Clean up the temporary files

All information passed to and from the workflow should be a JSON object.
    Dictionaries will be passed to the "execute" function, where it will use json.dumps to serialize the object
    Activities will be responsible for using json.loads to retrieve the input or previous task results
    simplejson library will be used because it can convert using decimal instead of float. floats are not permitted by dynamoDB
    
Note: Information necessary for each objected MUST BE PASSED via workflow objects. We cannot use local variables because other workflow instances
will overwrite variable names. We will pass the following information to each workflow item as they become available:
    - Asset
    - Asset Class
    - Asset Database key
Other items may be passed as necessary

author: Michael Schuler [mischuler@deloitte.com]


Other notes:
    Future: Identify asset class by MIME type?
"""

import time
import boto3
import uuid
import simplejson as json
from botocore.client import Config
import sys
import logging
import logging.config

def main(args):

    logging.config.fileConfig('/Assets/sharedLibraries/logging_config.ini')
    # Set up the Config file and create a SWF object

    # Set up variables
    DOMAIN = 'ITD'
    TASKLIST = 'default'
    VERSION = '1'
    
    # Activity names
    ACTIVITY1 = 'identifyAssetClass'
    ACTIVITY2a = 'extractExifMetadata'
    ACTIVITY2b = 'extractMediainfoMetadata'
    ACTIVITY3 = 'registerAsset'
    ACTIVITY4a = 'createThumbnailFromImage'
    ACTIVITY4b = 'createThumbnailFromVideo'
    ACTIVITY5b = 'transcodeVideoDefault'
    ACTIVITY5c = 'transcodeAudioDefault'
    ACTIVITY6 = 'distributeToS3'
    ACTIVITY7 = 'cleanUpLandingPad'
    
    logging.debug("Creating SWF boto client")
    botoConfig = Config(connect_timeout=50, read_timeout=70) # suggestion is the read is higher than connect
    swf = boto3.client('swf', config=botoConfig)
    logging.debug("Created SWF boto client: %s", swf)

    # DEFINE VARIABLES THAT WILL BE TRACKED THROUGHOUT WORKFLOW
    # 
    fileName = ''
    assetClass = ''
    DOC = {} # Overall document to register
    METADATA = {} # This metadata contains any user input metadata, along with input about the upload
    dbPrimaryKey = '' # primary key of the file once it is registered

    # The decider polls for a minute
    # we need to continiously loop so that it re-polls
    while True:
        
        # Attempt to polll for a new task
        
        logging.debug("Begin poll for event")
        newTask = swf.poll_for_decision_task(
            domain=DOMAIN,
            taskList={'name': 'default' }, # This is just a string. I don't understand the purpose of this yet
            identity='decider-default-workflow-1', # This can be any item and is recorded in the history. Don't know if we need to change this yet
            reverseOrder=False) # Don't get this either

        
        # Look for a new task using "Task Token"
        if 'taskToken' not in newTask:
            logging.info("No new event found in poll. Repoll")

        # There is some sort of event in our history
        elif 'events' in newTask:
        
            # Debuging for knowledge
            # Get a list of non-decision events to see what event came in last.
            logging.debug("Recieved an event: %s", newTask['events'])
            workflow_events = [e for e in newTask['events']
                               if not e['eventType'].startswith('Decision')]

            # Record latest non-decision event.
            last_event = workflow_events[-1]
            last_event_type = last_event['eventType']
            workID = newTask['workflowExecution']['workflowId']
            
            logging.info("[%s] Last Event Type: %s, Last Event: %s", workID, last_event_type, last_event)
            
            # If we are starting a workflow, the first event is to identify the asset class
            # Input to the workflow event is:
                # fileName -- the full file path
                # metadata -- the input metadata
            # Output for the next result is:
                # assetClass
            if last_event_type == 'WorkflowExecutionStarted':
                # At the start, get the worker to fetch the first assignment.
                logging.info("[%s] Starting workflow execution for: %s", workID, newTask['workflowType'])
                
                # Workflow execution expects a filepath and metadata
                # The input is buried within workflowExecutionStartedEventAttributes
                parameters = json.loads(last_event['workflowExecutionStartedEventAttributes']['input'], use_decimal=True)
                logging.debug("[%s] Parameters received: %s",workID, parameters)

                taskName = ACTIVITY1
                taskVersion = '1'
                taskList = taskName

                taskInput = {
                    'asset' : parameters['fileName'],
                    'metadata' : parameters['metadata'],
                }
                
                # Initial Task only needs the fileName to determine the asset class
                executeNextTask(swf,newTask['taskToken'], taskName, taskVersion, taskInput, taskList)
            
            elif last_event_type == 'ActivityTaskCompleted':
                # Take decision based on the name of activity that has just completed.
                # 1) Get activity's event id.
                last_event_attrs = last_event['activityTaskCompletedEventAttributes']
                completed_activity_id = last_event_attrs['scheduledEventId'] - 1
            
                # 2) Extract its name.
                activity_data = newTask['events'][completed_activity_id]
                activity_attrs = activity_data['activityTaskScheduledEventAttributes']
                activity_name = activity_attrs['activityType']['name']
            
                # 3) Get the result from the activity. NOTE: Run checks for those functions without a result
                result = json.loads(last_event['activityTaskCompletedEventAttributes'].get('result'), use_decimal=True)

                logging.debug("[%s] Completed Activity Data: %s", workID, activity_data)
                logging.debug("[%s] Completed Activity Attributes: %s", workID, activity_attrs)
                logging.debug("[%s] Completed Activity Name: %s", workID, activity_name)
                logging.debug("[%s] Completed Activity Result: %s", workID, result)
                
                # Get the token
                taskToken = newTask['taskToken']
                
                # Extract metadata information
                if activity_name == ACTIVITY1:
                    
                    # Get the asset class result and assign it to the workflow variable
                    assetClass = result['assetClass']
                    
                    # Branch for image or everything else
                    if assetClass == 'Image':
                        taskName = ACTIVITY2a
                    else:
                        taskName = ACTIVITY2b
                    
                    taskVersion = '1'
                    taskList = taskName

                    taskInput = {
                        'asset' : result['asset'],
                        'METADATA' : result['metadata'],
                        'assetClass' : assetClass
                        }

                    executeNextTask(swf,taskToken, taskName, taskVersion, taskInput, taskList)


                # After the metadata is extracted, we will register the asset
                # Next function is register Asset
                # Input is:
                    # Metadata Document -- compiled metadata document from the previous activity
                # Output is:
                    # Primary Key of DB entry
                elif activity_name in (ACTIVITY2a, ACTIVITY2b):
                    
                    # Get the asset class result and assign it to the workflow variable
                    taskName = ACTIVITY3
                    
                    taskVersion = '1'
                    taskList = taskName

                    taskInput = {
                        'DOC' : result['DOC'],
                        'asset' : result['asset'],
                        'assetClass' : result['assetClass']
                        }

                    executeNextTask(swf,taskToken, taskName, taskVersion, taskInput, taskList)
                
                # Thumbnail Creation
                # Next function is to create a thumbnail:
                    # Asset
                # Output is:
                    # Success message
                # This is only relevant for Images and Videos
                
                elif activity_name == ACTIVITY3:
                    
                    # Get the primary key and assign it to the workflow variable
                    
                    assetClass = result['assetClass']
                    
                    # We need to skip this step for audio and other
                    if assetClass == 'Image':
                        taskName = ACTIVITY4a
                    elif assetClass == 'Video':
                        taskName = ACTIVITY4b
                    elif assetClass == 'Audio':
                        taskName = ACTIVITY5c
                    else:
                        taskName = ACTIVITY6
                    
                    taskVersion = '1'
                    taskList = taskName

                    taskInput = {
                        'asset' : result['asset'],
                        'assetClass': assetClass,
                        'dbPrimaryKey' : result['dbPrimaryKey'],
                    }

                    executeNextTask(swf,taskToken, taskName, taskVersion, taskInput, taskList)
                
                # Transcoding execution
                # This comes after Video Thumbnail creation OR Audio registration
                elif activity_name in (ACTIVITY4a, ACTIVITY4b):
                    
                    assetClass = result['assetClass']
                    # Other cases SHOULD NOT happen
                    if assetClass == 'Image':
                        taskName = ACTIVITY6
                    elif assetClass == 'Video':
                        taskName = ACTIVITY5b
                    
                    taskVersion = '1'
                    taskList = taskName

                    taskInput = {
                        'asset' : result['asset'],
                        'assetClass': assetClass,
                        'dbPrimaryKey' : result['dbPrimaryKey'],
                    }

                    executeNextTask(swf,taskToken, taskName, taskVersion, taskInput, taskList)

                # Ditribution
                elif activity_name in (ACTIVITY5b, ACTIVITY5c):

                    taskName = ACTIVITY6
                    taskVersion = '1'
                    taskList = taskName

                    taskInput = {
                        'asset' : result['asset'],
                        'assetClass': result['assetClass'],
                        'dbPrimaryKey' : result['dbPrimaryKey'],
                    }

                    executeNextTask(swf,taskToken, taskName, taskVersion, taskInput, taskList)
                
                # Cleanup
                elif activity_name == ACTIVITY6:

                    taskName = ACTIVITY7
                    taskVersion = '1'
                    taskList = taskName

                    taskInput = {
                        'asset' : result['asset'],
                        'assetClass': result['assetClass'],
                        'dbPrimaryKey' : result['dbPrimaryKey'],
                    }

                    executeNextTask(swf,taskToken, taskName, taskVersion, taskInput, taskList)
                    
                # Completion
                elif activity_name == ACTIVITY7:
                    swf.respond_decision_task_completed(
                    taskToken=taskToken,
                        decisions=[
                            {
                                'decisionType': 'CompleteWorkflowExecution',
                                'completeWorkflowExecutionDecisionAttributes': {
                                'result': 'success'
                                }
                            }
                        ]
                    )  
                    logging.info("Task has been completed: %s", taskToken)

# Function that will run the task completed call
# This is reused across all activities, minus the ending function

# Food for thought: Can we run two tasks from a single decision for parallel procsseing?
def executeNextTask(swf,taskToken, taskName, taskVersion, taskInput, taskList):

    swf.respond_decision_task_completed(
        taskToken=taskToken, # keeps the same token throughout the execution
        decisions = [
            {
            'decisionType': 'ScheduleActivityTask',
            'scheduleActivityTaskDecisionAttributes': {
                'activityType':{
                    'name': taskName, # string
                    'version': taskVersion # string
                    },
                'activityId': 'activityid-' + str(uuid.uuid4()),
                'input': json.dumps(taskInput, use_decimal=True),
                'scheduleToCloseTimeout': 'NONE',
                'scheduleToStartTimeout': 'NONE',
                'startToCloseTimeout': 'NONE',
                'heartbeatTimeout': 'NONE',
                'taskList': {'name': taskList}, # TASKLIST is a string
                }
            }
        ] # end bracket decision
    ) # end bracket respond

if __name__ == '__main__':

    main(sys.argv)

