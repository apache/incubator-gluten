#!/bin/bash

if [ ! -d spark331 ];
then  
  git clone --depth 1 --branch v3.3.1 https://github.com/apache/spark.git spark331 && cd /tmp/spark33/spark331 && ./build/mvn -Pyarn -DskipTests install 
else  
  echo "Skip Spark331 build" 
fi
