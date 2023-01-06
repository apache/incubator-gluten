# Build Dev Environment in Mac OSX

This document describes how to set up a development environment in Mac OSX (for Velox backend). The development includes
both coding and runtime environment. For coding part, we follow the command practice to load
gluten project as a maven project in IntelliJ. For runtime part, as Gluten has not supported building and running 
in Mac OSX, we leverage docker container here. 

1. build docker image

```bash
cd $GLUTEN_HOME/dev;

docker build -t gluten_dev:latest . 
```

2. start a container. The following command start a container based on the image we
 built in the last step and always keep it running. It also maps port 4040 to host, so that you
 can access Spark application started within the container with http://localhost:4040. Additionally, 
the gluten source code directory is loaded as as shared volume in container at `/src`

```bash
cd $GLUTEN_HOME;

docker run -itd -p 4040:4040 -v $(pwd):/src/ gluten_dev:latest tail -f /dev/null

```


3. login to the container (assuming your docker container name is 6bd08d27a5d7) 
```docker exec -it 6bd08d27a5d7 /bin/bash```

4. build gluten. In theory, you could build gluten directly in `/src`. However, it is usually very slow to 
access a shared volume in host from container. We recommend to sync code to container native file system with
 the following command

```bash
rsync -av --exclude '*build' --exclude '*target' /src /root/gluten 
```

and then you could build gluten with the corresponding instructions in `/root/gluten/src`. (Please remember
 to run the above command when you want to rebuild gluten after some changes)

5. run Spark within the container with Gluten. The docker image in the first step has spark installed already so that 
you can run the following command 

```export gluten_jvm_jar = /PATH/TO/GLUTEN/backends-velox/target/gluten-spark3.2_2.12-1.0.0-snapshot-jar-with-dependencies.jar 
spark-shell 
  --master yarn --deploy-mode client \
  --conf spark.plugins=io.glutenproject.GlutenPlugin \
  --conf spark.gluten.sql.columnar.backend.lib=velox \
  --conf spark.driver.extraClassPath=${gluten_jvm_jar} \
  --conf spark.executor.extraClassPath=${gluten_jvm_jar} \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=2g
  ...
  ```