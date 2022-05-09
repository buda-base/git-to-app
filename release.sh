#!/bin/bash
 
d=`date +%Y%m%d%H%M%S`
den=`date +"%x"`
dbo=`date +"སྤྱི་ལོ%Yཟླ%Mཚེས%d"`
dzh=`date +"%Y年%M月%d日"`
d2=`date -Iseconds`

JSON="{
  \"mirrorId\":\"us-east-1\",
  \"siteUrl\":\"https://www.bdrc.io\",
  \"libraryUrl\":\"https://library.bdrc.io\",
  \"viewerUrlPrefix\":\"https://library.bdrc.io/scripts/embed-iframe.html\",
  \"releaseSerialNumber\": XXX,
  \"releaseDate\":\"$d2\",
  \"releaseZipUrl\":\"http://staticfiles.bdrc.io/BDRCLibApp/1.2/$d.zip\",
  \"releaseDescription\":[
    {
      \"value\":\"$den\",
      \"language\":\"en\"
    },
    {
      \"value\":\"$dbo\",
      \"language\":\"bo\"
    },
    {
      \"value\":\"$dzh\",
      \"language\":\"zh\"
    }
  ]
}"

rm -rf BDRCLIB
mkdir -p BDRCLIB
python3 gittoapp.py ../bdrc-git-repos/ BDRCLIB/

mkdir -p releases

zip -q -r releases/$d.zip BDRCLIB

aws s3 --profile appdata cp releases/$d.zip s3://data.tbrc.org/app-data/$d.zip

echo "posible json saved as $d.json"
echo $JSON > $d.json
