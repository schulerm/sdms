import boto3
import simplejson as json
import sys
import botocore
import subprocess
import shutil

sys.path.insert(0, '/Assets/sharedLibraries')
import databaseHelper
import parseHelper
import os
import fnmatch
import string
import math
import time

swf = boto3.client('swf')

DOMAIN = 'ITD'
WORKFLOW = 'defaultRun'
VERSION = '2'
TASKLIST = 'default'


MA = {}
AUDIT = {}

def main(args):

    runTest(args)
    #dbTest()
    #transcodeTest()
    #dbTest2()
    #delTest(args)
    
def runTest(args):
    testV = '/trim.6A7FE5FA-5321-4BF1-87D7-D26937142263_76ed9582-7da6-44ef-a0af-e22596297573/trim.6A7FE5FA-5321-4BF1-87D7-D26937142263_76ed9582-7da6-44ef-a0af-e22596297573.MOV'
    testI = '/image_509b2a45-de95-4757-9a4e-58d1e28b362a/image_509b2a45-de95-4757-9a4e-58d1e28b362a.jpeg'


    if args[1] == 'V':
        fileIn = testV
        sha1 = '6e508f0ed03da2b98ceb091827b569877037d1ef'
    else:
        fileIn = testI
        sha1 = 'afbf98c0070e5f048c21d7d17e24cd05401971f7'

    dir = os.path.split(fileIn)[0]
    shutil.copytree('/Assets/test' + dir ,'/Assets/upload' + dir)
    key = {
        'Checksum' : sha1,
    }
    response = databaseHelper.deleteEntry(key)

    AUDIT['User'] = 'System'
    AUDIT['Timestamp'] = time.strftime("%Y-%m-%dT%H:%M:%S+0000",time.gmtime())
    AUDIT['Action'] = 'File uploaded'
    AUDIT['Notes'] = 'test harness'
    
    
    A = []
    A.append(AUDIT)
    
    MA['Description'] = 'test harness run'
    MA['Audit'] = A

    
    INPUT = {
        'fileName' : '/Assets/upload' + fileIn,
        'metadata' : MA,
    }

    print json.dumps(INPUT)

    response = swf.start_workflow_execution(
      domain=DOMAIN, # string
      workflowId='test-%s' %(args[2]),
      workflowType={
        "name": WORKFLOW,# string
        "version": VERSION # string
      },
      taskList={
          'name': TASKLIST
      },
      input=json.dumps(INPUT)
    )

    print "Workflow requested: ", response
    
    
def delTest(args):


    if args[1] == 'V':
        #fileIn = testV
        sha1 = '6e508f0ed03da2b98ceb091827b569877037d1ef'
        fileKey = 'trim.6A7FE5FA-5321-4BF1-87D7-D26937142263_76ed9582-7da6-44ef-a0af-e22596297573'
    else:
        #fileIn = testI
        sha1 = 'afbf98c0070e5f048c21d7d17e24cd05401971f7'
        fileKey = '/image_509b2a45-de95-4757-9a4e-58d1e28b362a'

    sha1 = 'fc7a224388045ba3ffb1d6e647c331d747ca46c8'
    fileKey = 'trim.74CFE8B3-6DE3-406B-8A45-37B3BB9F9E69_850995df-b3eb-47b6-bb35-d6f9b453ce3e'
    key = {
        'Checksum' : sha1,
    }

    INPUT = {
        'dbPrimaryKey' : key,
        'fileKey' : fileKey,
        'assetClass' : 'Video',
        'locationSource' : 'CDN',
        'locationDestination' : 'delete'
    }

    print json.dumps(INPUT)

    response = swf.start_workflow_execution(
      domain=DOMAIN, # string
      workflowId='test-%s' %(args[2]),
      workflowType={
        "name": 'fileManager',# string
        "version": '1' # string
      },
      taskList={
          'name': 'fileManager'
      },
      input=json.dumps(INPUT)
    )

    print "Workflow requested: ", response


def transcodeTest():

    asset = '/Assets/test/trim.6A7FE5FA-5321-4BF1-87D7-D26937142263_76ed9582-7da6-44ef-a0af-e22596297573/trim.6A7FE5FA-5321-4BF1-87D7-D26937142263_76ed9582-7da6-44ef-a0af-e22596297573.MOV'
    scale = "640x360"
    newDir = "thumbnails"
    thumbnailTime = .25 # Create thumbnail 25% of the way through the video
    fps = 1
        
    (filePath, fileName, fileExt) = parseHelper.splitFilename(asset)
    subDir = parseHelper.createDir(filePath, newDir)
            
    # We require the %d to keep the file names incremented
    # Note that we need to escape the percentage sign by using another %, hence the double %
    outfile = '%s_thumbnail_%%d.jpg' % (fileName)
    vtt = '%s.vtt' % (fileName)

    cmd = ['ffmpeg'
        ,'-y'
        ,'-i', asset
        ,'-s', scale
        ,'-vf', 'fps=%s' % (fps)
        ,'%s/%s' %(subDir, outfile)
    ]
    
    output = subprocess.check_output(cmd)
    print output
    # After the thumbnails are created, we need to do two things:
    # 1. Find the thumbnail we want to display, and create the object
    # 2. Create the storyboard object which is [http://docs.brightcove.com/en/perform/brightcove-player/guides/thumbnails-plugin.html#collectimages]
    # "second" : { "src" : "thumbnail"}
    
    STORYBOARD = {}
    counter = 0
    
    for thumb in os.listdir(subDir):
        if fnmatch.fnmatch(thumb, '*_thumbnail_*.jpg'):
            counter = counter + 1


    # Open the VTT file and write
    vttFile = open('%s/%s' %(subDir, vtt), 'w')
    vttFile.write("WEBVTT")
    # The counter represents how many files of FPS we have -- range is COUNTER*FPS --> (COUNTER+1)* fps
    # FPS references the frames per second so if we put (1/60), that means a frame EVERY MINUTE
    # Therefore, we need to invest the FPS
    # Use %02d to PAD the numbers 
    for i in range(0,counter):
        startSecond = i * (1/fps)
        endSecond = (i + 1) * (1/fps)
        startSpan = '%02d:%02d:%02d.000' % ( startSecond / 3600, startSecond / 60 % 60, startSecond % 60) 
        endSpan =  '%02d:%02d:%02d.000' % ( endSecond / 3600, endSecond / 60 % 60, endSecond % 60)
        thumbSpan =  '%s/%s_thumbnail_%d.jpg' % (newDir, fileName,i + 1)
        
        vttFile.write("\n\n%s --> %s\n%s" % (startSpan, endSpan, thumbSpan))
    
    vttFile.close()
    
    index = str(math.trunc(counter * thumbnailTime))
    thumbnail = '%s/%s_thumbnail_%s.jpg' % (newDir, fileName,index)
    
    sha1 = '6e508f0ed03da2b98ceb091827b569877037d1ef'
    
    # Start setting the parameters needed to update the thumbnail
    key = {
        'Checksum' : sha1,
    }
    
    updateExpression = 'set thumbnail = :t, storyboard = :s'
    
    expressionValues = {
        ':t' : thumbnail,
        ':s' : '%s/%s' %(newDir, vtt)
    }
    
    #response = databaseHelper.updateEntry(key, updateExpression, expressionValues)

def dbTest():
    
    key = {'Checksum': u'afbf98c0070e5f048c21d7d17e24cd05401971f7'} 
    updateExp = 'set thumbnails.thumbnail = :t'
    expVals = {':t': u'thumbnails/image_509b2a45-de95-4757-9a4e-58d1e28b362a_thumbnail.jpg'}
    rv = 'NONE'
    
    
    #updateExp = 'set #k1 = if_not_exists( #k1, :v1 )'
    #updateExp = 'set #k1 = :v1'
    #expAttribute = {'#k1' : 'thumbnails'}
    #expVals = { ':v1' : { 'thumbnail' : 'thumbnails/image_509b2a45-de95-4757-9a4e-58d1e28b362a_thumbnail.jpg' } }

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Assets')
    try:
        response = table.update_item(
        Key = key,
        UpdateExpression = updateExp,
        ExpressionAttributeValues = expVals,
        ReturnValues = 'UPDATED_OLD'
        )
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'ValidationException':
            
            updateExp = 'set thumbnails = :t'
            expVals = {':t': M}
            
            response = table.update_item(
            Key = key,
            UpdateExpression = updateExp,
            ExpressionAttributeValues = expVals,
            ReturnValues = 'UPDATED_OLD'
            )
    #print key, updateExpression, expressionValue, returnValues
    '''response = table.update_item(
        Key = key,
        UpdateExpression = updateExp,
        ExpressionAttributeNames = expAttribute,
        ExpressionAttributeValues = expVals,
        ReturnValues = 'UPDATED_OLD'
        )'''
    
    print response
    
    return response
    
    
def dbTest2():
    
    key = {'Checksum': u'76b5f1bc9100b32447e6116b321c14e51cd59fe3'} 
    updateExp = "set #f = :t"
    expNames = {
        '#f': 'File Location' 
    }
    expVals = {
        ':t': 'CDN'
    }
    rv = 'NONE'
    
    
    #updateExp = 'set #k1 = if_not_exists( #k1, :v1 )'
    #updateExp = 'set #k1 = :v1'
    #expAttribute = {'#k1' : 'thumbnails'}
    #expVals = { ':v1' : { 'thumbnail' : 'thumbnails/image_509b2a45-de95-4757-9a4e-58d1e28b362a_thumbnail.jpg' } }

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Assets')
    
    response = table.update_item(
        Key = key,
        UpdateExpression = updateExp,
        ExpressionAttributeNames = expNames,
        ExpressionAttributeValues = expVals,
        ReturnValues = 'UPDATED_OLD'
        )
    
if __name__ == '__main__':
    
    main(sys.argv)