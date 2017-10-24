#!/bin/sh

cd ~/
biosFilename=currentbios.xml

chmod 755 conrep
./conrep -s -f $biosFilename
errCode=$?

exit $errCode
