#!/bin/sh

cd ~/
biosFilename=newbios.xml

echo Applying BIOS from $biosFilename

chmod 755 conrep
echo executing ./conrep -l -f $biosFilename
./conrep -l -f $biosFilename
errCode=$?
echo conrep returned $errCode

exit $errCode
