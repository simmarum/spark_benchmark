#!/bin/bash

# Simple bash script to recreate env for this repository
# Activate env by typing
#
# source ./v-env/bin/activate
#

set -e
SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"

rm -rf $SCRIPTPATH/v-env

python3 -m venv $SCRIPTPATH/v-env
. $SCRIPTPATH/v-env/bin/activate

pip3 install --compile \
pandas==0.25.3 \
more-itertools==8.0.2 \
pyarrow==0.15.1 \
pyspark==2.4.4 \
psutil==5.6.7 \
jupyter==1.0.0 \
matplotlib==3.1.2
