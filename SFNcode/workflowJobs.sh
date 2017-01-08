#!/bin/sh

SCRIPTS=()

SCRIPTS+=('identifyAssetClass.py')
SCRIPTS+=('registerAsset.py')
SCRIPTS+=('extractExifMetadata.py')
SCRIPTS+=('extractMediainfoMetadata.py')
SCRIPTS+=('createThumbnailFromImage.py')
SCRIPTS+=('createThumbnailFromVideo.py')
SCRIPTS+=('transcodeVideoDefault.py')
SCRIPTS+=('distributeToS3.py')
SCRIPTS+=('cleanUpLandingPad.py')


for i in "${SCRIPTS[@]}"
do
	if [ "$1" == "start" ]; then 
		python $i &
	else
		id=`ps u | grep "[p]ython ${i}"`
		idA=($id)
		`kill ${idA[1]}`
	fi
done



