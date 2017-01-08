""" This function will register the asset with the database

author: Michael Schuler [mischuler@deloitte.com]

"""

import boto3
from botocore.client import Config
import sys
import os
import simplejson as json
import botocore
import logging
import logging.config

sys.path.insert(0, '/Assets/sharedLibraries/')
import databaseHelper


def main(args):

    ARN = "arn:aws:states:us-east-1:497940546915:activity:registerAsset"
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
            INPUT = json.loads(task['input'] , use_decimal=True)
            DOC = INPUT['DOC']
            registered = True

            # The database helper class has all we need CRUD operations
            logging.debug("[%s] Writing database entry", workID)
            
            # This is where we write the entry. Note that we use the CHECKSUM as a primary key
            # Registration WILL fail if the same file exists
            # There are two options if the registration fails
            #   1. Someone uploaded a previously deleted file. If this is the case, use a contional update on:
            #       FILENAME 
            #       LOCATION
            #       USER
            #       Audit trail
            #   2. Else, fail
            # NOTE: Should we tie the index to account as well?
            
            dynamodb = boto3.resource('dynamodb')
            table = dynamodb.Table('Assets')
                
            try:
                table.put_item(
                    Item = DOC,
                    ConditionExpression = 'attribute_not_exists(Checksum)'
                )
            
            # ConditionalCheckFailedException
            except botocore.exceptions.ClientError as err:
            
                # FUTURE: Grep for ConditionalCheckFailedException
                # We need to separate out actual client errors from put item errors
                logging.debug("[%s] Entry already exists for: %s", workID, DOC['Checksum'])

                # Update the document. Set the PDL and thumbnail as NULL as well
                key = {
                    'Checksum' : DOC['Checksum'],
                    }
                
                updateExpression = 'SET File_Location = :d, Filename = :f, UserFields = :u, Audit = list_append(Audit, :a) REMOVE PDL, thumbnail, storyboard'
                conditionalExpression = 'File_Location = :l'
                
                expressionValues = {
                    ':d' : 'working',
                    ':a' : DOC['Audit'],
                    ':u' : DOC['UserFields'],
                    ':f' : DOC['Filename'],
                    ':l' : 'delete'
                }
                
                logging.debug("[%s] Attempting to update the item if it is deleted", workID)
                try:
                    result = table.update_item(
                        Key = key,
                        ConditionExpression = conditionalExpression,
                        UpdateExpression = updateExpression,
                        ExpressionAttributeValues = expressionValues,
                        ReturnValues = 'UPDATED_OLD'
                        )
                    
                    logging.debug("[%s] Update result: %s", workID, result )
                except botocore.exceptions.ClientError as err:
                    logging.debug("[%s] Update failed %s", workID, str(err) )
                    
                    registered = False
                    
                    result = { 
                        'reason' : 'REG-0001_Duplicate file entry: The file with ID %s already exists' %(DOC['Checksum']),
                        'detail' : str(err)
                    }


            # Registered being TRUE indicates success
            if registered:
                
                result = { 
                    'Checksum' : DOC['Checksum'] 
                }

                OUTPUT = {
                    'dbPrimaryKey' : result,
                    'assetClass' : INPUT['assetClass'], 
                    'asset' : INPUT['asset'],
                }

                
                sfn.send_task_success(
                    taskToken=taskToken,
                    output=json.dumps(OUTPUT)
                )
            else:
            
                logging.warning("[%s] Registration failure: %s", workID, result)
                sfn.send_task_failure(
                    taskToken=taskToken,
                    error=json.dumps(result['reason']),
                    cause=json.dumps(result['detail'])
                )

            logging.info("[%s] %s Complete", workID, taskName)

            
            
            
if __name__ == '__main__':
    
    main(sys.argv)