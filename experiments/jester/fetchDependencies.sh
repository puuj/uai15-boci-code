#!/bin/bash

DATAURL=https://linqs-data.soe.ucsc.edu/public/pujara_uai15/psl-1.3.1-SNAPSHOT-jars.tgz
LOCALFILE=jars.tgz
CURL=`which curl`
WGET=`which wget`
TAR=`which tar`
CMD=''
if [  $CURL ]; then
    echo "curl is $CURL"
    CMD="$CURL -o $LOCALFILE $DATAURL";
elif [ $WGET ]; then
    echo "wget is $WGET"
    CMD="$WGET -O $LOCALFILE $DATAURL";
else
    echo "Need either wget or curl to download a dataset"
    exit 1;
fi

echo "Downloading the dataset with command: $CMD";
`$CMD`;

if [ $TAR ]; then
    CMD="$TAR -xvzf $LOCALFILE";
else 
    echo "Need tar to extract dataset";
    exit 2;
fi

echo "Extracting the dataset with command: $CMD";
`$CMD`


