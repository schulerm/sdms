""" This function creates a thumbnail from an image and registers the thumbnail with the DB

Function will take the following steps:
    Create a thumbnail directory
    Utilize FFMPEG to create a thumbnail
        With a video, we want to take ~25% through a video just so it's not the beginning
    NOTE: Can we take full storyboard at some point?
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
import math
import fnmatch

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
             # Take the thumbnail 25% through the video
            
            #scale = "640x360"
            # Use the multipliers so that we don't distort vertical videos. This makes it generic. 
            scale = "iw/3:ih/3"  # 1/3 gives 1920 (HD) down to 640
            fps = 1 # Set the number of frames to be once per second
            newDir = "thumbnails"
            (filePath, fileName, fileExt) = parseHelper.splitFilename(asset)
            subDir = parseHelper.createDir(filePath, newDir)
            
            # We require the %d to keep the file names incremented
            # Note that we need to escape the percentage sign by using another %, hence the double %
            outfile = '%s_thumbnail_%%d.jpg' % (fileName)
            vtt = '%s.vtt' % (fileName)
            
            # Parameters are
            # -y for
            # -i for Input
            # -vf, fps=1,scale= for the video filter stating we want to take every one second
            cmd = ['ffmpeg'
                ,'-y'
                ,'-i', asset
                ,'-vf', 'fps=%s,scale=%s' %(fps, scale)
                ,'-loglevel', 'fatal'
                ,'%s/%s' %(subDir, outfile)
            ]

            logging.debug("[%s] Execute video thumbnail creation: %s", workID, cmd)
            try:
                output = subprocess.check_output(cmd)
                
                # Start setting the parameters needed to update the thumbnail
                
                # Comment block is staying for reference sake
                '''# Call the update function
                # The "thumbnails" map will need to be created if it doesn't exist (Note: It shouldn't at this point)
                # A validation exception will be thrown, and when this is thrown, we will create an empty map and try it again
                try:
                    response = databaseHelper.updateEntry(key, updateExpression, expressionValues) 
                
                except botocore.exceptions.ClientError as err:
                    if err.response['Error']['Code'] == 'ValidationException':
                        
                        
                        response = databaseHelper.updateEntry(key, 'set thumbnails = :t', {':t' : {}})
                        response = databaseHelper.updateEntry(key, updateExpression, expressionValues)
                '''
                
                # After the thumbnails are created, we need to do two things:
                # OLD # 1. Create the storyboard object which is [http://docs.brightcove.com/en/perform/brightcove-player/guides/thumbnails-plugin.html#collectimages]
                # 1. Create the storyboard VTT file (https://support.jwplayer.com/customer/portal/articles/1407439-adding-preview-thumbnails)
                # 2. We also need to identify the thumbnail for the video which we will take a percentage of the way through the video

                
                #STORYBOARD = {}
                thumbnailTime = .25 # Pick the thumbnail that's 25% of the way through the video
                counter = 0
                
                for thumb in os.listdir(subDir):
                    if fnmatch.fnmatch(thumb, '*_thumbnail_*.jpg'): # Match files in the directory that are the thumbnails
                        #sequenceNum = thumb[thumb.rfind('_')+1:-4] # filename_thumbnail_$frame.jpg
                        #STORYBOARD[sequenceNum] = {'src' : '/%s/%s' %(newDir, thumb) }
                        counter = counter + 1

                # Open the VTT file and write
                logging.debug("[%s] Writing VTT file: %s", workID, vtt)
                vttFile = open('%s/%s' %(subDir, vtt), 'w')
                vttFile.write("WEBVTT")
                # The counter represents how many files of FPS we have -- range is COUNTER*FPS --> (COUNTER+1)* fps
                # FPS references the frames per second so if we put (1/60), that means a frame EVERY MINUTE
                # Therefore, we need to invest the FPS
                # Use %02d to PAD the numbers 
                
                baseURL = "https://dnt4vq51jg2tj.cloudfront.net" # There needs to be a better way then the full URL
                for i in range(0,counter):
                    startSecond = i * (1/fps)
                    endSecond = (i + 1) * (1/fps)
                    startSpan = '%02d:%02d:%02d.000' % ( startSecond / 3600, startSecond / 60 % 60, startSecond % 60) 
                    endSpan =  '%02d:%02d:%02d.000' % ( endSecond / 3600, endSecond / 60 % 60, endSecond % 60)
                    
                    
                    thumbSpan =  '%s/%s/%s/%s_thumbnail_%d.jpg' % (baseURL, fileName, newDir, fileName,i + 1)
                    
                    vttFile.write("\n\n%s --> %s\n%s" % (startSpan, endSpan, thumbSpan))
                
                vttFile.close()
                logging.debug("[%s] Wrote VTT file: %s", workID, vtt)
                
                index = str(math.trunc(counter * thumbnailTime))
                logging.debug("[%s] Key frame identified in index: %s", workID, index)
                
                updateExpression = 'set thumbnail = :t, storyboard = :s'
                thumbnail = '/%s/%s_thumbnail_%s.jpg' % (newDir, fileName,index)
                
                # THERE MUST BE A DYNAMIC WAY TO DO THIS BUT I DONT KNOW YET
                storyboard = '/%s/%s' %(newDir, vtt)
                
                '''expressionValues = {
                    ':t' : STORYBOARD[index]['src'],
                    ':s' : STORYBOARD
                }'''
                
                expressionValues = {
                    ':t' : thumbnail,
                    ':s' : storyboard,
                }
                
                logging.debug("[%s] Update thumbnail value", workID)
                response = databaseHelper.updateEntry(dbPrimaryKey, updateExpression, expressionValues)

                OUTPUT = {
                    'tool' : output,
                    'dbPrimaryKey' : dbPrimaryKey,
                    'assetClass' : INPUT['assetClass'], 
                    'asset' : asset,
                }
                
                swf.respond_activity_task_completed(
                    taskToken = taskToken,
                    result = json.dumps(OUTPUT)
                )
            # We should catch other errors here
            except subprocess.CalledProcessError as err:
                
                result = { 
                    'reason' : 'THB-0002_Error in video thumbnail creation',
                    'detail' : str(err)
                }
                
                logging.error("%s", result)
                
                swf.respond_activity_task_failed(
                    taskToken=taskToken,
                    reason=json.dumps(result['reason']),
                    details=json.dumps(result['detail'])
                )
                    
            logging.info("[%s] %s Complete", workID, taskName)

if __name__ == '__main__':
    
    main(sys.argv)
