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
  \"viewerUrlPrefix\":\"http://library.bdrc.io/view/\",
  \"releaseSerialNumber\": XXX,
  \"releaseDate\":\"$d2\",
  \"releaseZipUrl":"http://library.bdrc.io/mobile-data/$d.zip\",
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

python3 gittoapp.py ../bdrc-git-repos/

mkdir -p releases

cd output/
zip -q -r ../releases/$d.zip *
cd ../

aws s3 cp releases/$d.zip s3://data.tbrc.org/app-data/$d.zip

echo "posible json saved as output.json"
echo $JSON > output.json

echo "complete and then upload to s3:"
echo "aws s3 cp output.json s3://data.tbrc.org/app-data/$d.json"