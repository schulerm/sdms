""" fileManager is the workflow engine responsible for any file movement of uploaded files. File movements include:
    Move from S3 to S3 Infrequent Access (IA) (Safe delete)
    Move from S3 IA to Glacier Storage (Archive)
    Move from S3 to Glacier Storage (Archive)
    Delete from S3 (Note: Default "delete" behavior moves items to S3 IA for 30 days before removal)
    Delete from S3 IA (delete)
    Delete from Glacier Storage (delete)
    Restore to S3 from S3IA (un-delete)
    Restore to S3 from Glacier (un-archive)

The workflow contains the following tasks to accomplish the above:
    Move Files -- Copies files locally and re-uploads the location (If move or restore)
    Delete Files -- Removes the file from
    Clean Up -- Only if files have been successfully moved -- this will leverage the cleanupLandingPad activity used in ingest
    
Workflow expects the following input:
    Database Primary Key
    S3 initial directory key (This can be obtained from DyanmoDB but it prevents an extra query if passed in)
    Asset Class (This can be obtained from DynamoDB but it prevents an extra query if passed in)
    Source location (This can be obtained from DyanmoDB but it prevents an extra query if passed in)
    Destination Location [near_line, archive, CDN, delete]
    
Workflow logic is as follows:
    If end location is delete:
        Run deleteFile activity -- delete will identify the object storage type
            Update metadata location and add to history [UI should insert the request operation]
    If end location is not delete:
        If end OR source is in 'Archive':
            Download object to local EFS /Assets/working/ [Should Glacier run async? -- Should this be threaded?]
            Upload files to DESTINATION
            Clean up temporary files
            Delete files from source destination
                Update metadata location and add to history [UI should insert the request operation]
            Else
                Change objects location in S3
                Update metadata location and add to history [UID should insert the request operation]


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
    Future: TBD
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
    TASKLIST = 'fileManager'
    VERSION = '1'
    
    # Activity names
    ACTIVITY1 = 'moveFiles'
    ACTIVITY2 = 'cleanUpLandingPad'
    ACTIVITY3 = 'deleteFiles'
    
    logging.debug("Creating SWF boto client")
    botoConfig = Config(connect_timeout=50, read_timeout=70) # suggestion is the read is higher than connect
    swf = boto3.client('swf', config=botoConfig)
    logging.debug("Created SWF boto client: %s", swf)

    # The decider polls for a minute
    # we need to continiously loop so that it re-polls
    while True:
        
        # Attempt to polll for a new task
        
        logging.debug("Begin poll for event")
        newTask = swf.poll_for_decision_task(
            domain=DOMAIN,
            taskList={'name': TASKLIST }, # This is just a string. I don't understand the purpose of this yet
            identity='file-manager-tasks-1', # This can be any item and is recorded in the history. Don't know if we need to change this yet
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
            
            if last_event_type == 'WorkflowExecutionStarted':
                # At the start, get the worker to fetch the first assignment.
                logging.info("[%s] Starting workflow execution for: %s", workID, newTask['workflowType'])
                
                # Workflow execution expects a filepath and metadata
                # The input is buried within workflowExecutionStartedEventAttributes
                parameters = json.loads(last_event['workflowExecutionStartedEventAttributes']['input'], use_decimal=True)
                logging.debug("[%s] Parameters received: %s",workID, parameters)
                
                locationDestination = parameters['locationDestination']

                
                if locationDestination == 'delete':
                    taskName = ACTIVITY3
                else:
                    taskName = ACTIVITY1
                    
                taskVersion = '1'
                taskList = taskName

                # We can pass the same parameters back into each work unit
                taskInput = parameters
                                
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
                
                # If we are moving to/from Glacier, we need to download the files and clean them up, otherwise we skip to the end
                if activity_name == ACTIVITY1 and (result['locationDestination'] in 'archive' or result['locationSource'] in 'archive'):
                
                    # 
                    
                    taskName = ACTIVITY2
                    taskVersion = '1'
                    taskList = taskName

                    taskInput = result

                    executeNextTask(swf,taskToken, taskName, taskVersion, taskInput, taskList)


                elif activity_name == ACTIVITY2:
                    
                    # Get the asset class result and assign it to the workflow variable
                    taskName = ACTIVITY3
                    
                    taskVersion = '1'
                    taskList = taskName

                    taskInput = result

                    executeNextTask(swf,taskToken, taskName, taskVersion, taskInput, taskList)
                
                    
                # Completion
                elif activity_name == ACTIVITY3 or (activity_name == ACTIVITY1 and not(result['locationDestination'] in 'archive' or result['locationSource'] in 'archive')):
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

