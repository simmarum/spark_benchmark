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
pandas \
more-itertools
