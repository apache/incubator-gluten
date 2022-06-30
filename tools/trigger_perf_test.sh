#! /bin/bash

if [ -n "$1" ]; then
    echo "begin to build job"
else
    echo "need pull request number"
    exit 1
fi

branchname=origin/pr/$1/head
echo $branchname

curl --noproxy vsr168 -L -X POST http://vsr168:8080/job/Gluten_Perf_ICX/build --user jenkins:11fd1b5a82bfd638bd9b3749c96b324ff2 --data-urlencode json='{"parameter": [{"name":"sha1", "value":"'$branchname'"}]}'
