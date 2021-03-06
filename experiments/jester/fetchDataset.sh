#!/bin/bash

DATAURL=https://linqs-data.soe.ucsc.edu/public/pujara_uai15/data-jester.tgz
CURL=`which curl`
WGET=`which wget`
TAR=`which tar`
CMD=''
if [  $CURL ]; then
    echo "curl is $CURL"
    CMD="$CURL -o data.tgz $DATAURL";
elif [ $WGET ]; then
    echo "wget is $WGET"
    CMD="$WGET -O data.tgz $DATAURL";
else
    echo "Need either wget or curl to download a dataset"
    exit 1;
fi

echo "Downloading the dataset with command: $CMD";
`$CMD`;

if [ $TAR ]; then
    CMD="$TAR -xvzf data.tgz";
else 
    echo "Need tar to extract dataset";
    exit 2;
fi

echo "Extracting the dataset with command: $CMD";
`$CMD`


