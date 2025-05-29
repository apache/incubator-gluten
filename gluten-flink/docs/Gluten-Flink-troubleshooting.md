---
layout: page
title: Gluten for Flink Troubleshooting
---
## Troubleshooting

### How to make sure gluten packages loaded first in Flink?

Gluten packages need to be loaded first in Flink, you can edit the `bin/config.sh` in Flink dir,
and change the `constructFlinkClassPath` function like this:

```
GLUTEN_JAR="/path/gluten-flink-loader-1.4.0.jar:/path/velox4j-0.1.0-SNAPSHOT.jar:/path/gluten-flink-runtime-1.4.0.jar:"
echo "$GLUTEN_JAR""$FLINK_CLASSPATH""$FLINK_DIST"
```
