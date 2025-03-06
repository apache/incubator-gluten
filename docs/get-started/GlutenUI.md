---
layout: page
title: Gluten UI
nav_order: 1
parent: Getting-Started
---
# Gluten UI

## Gluten event

Gluten provides two events `GlutenBuildInfoEvent` and `GlutenPlanFallbackEvent`:

- GlutenBuildInfoEvent, it contains the Gluten build information so that we are able to be aware of the environment when doing some debug.
  It includes `Java Version`, `Scala Version`, `GCC Version`, `Gluten Version`, `Spark Version`, `Hadoop Version`, `Gluten Revision`, `Backend`, `Backend Revision`, etc.

- GlutenPlanFallbackEvent, it contains the fallback information for each query execution.
  Note, if the query execution is in AQE, then Gluten will post it for each stage.

Developers can register `SparkListener` to handle these two Gluten events.

## SQL tab

Gluten provides a tab based on Spark UI, named `Gluten SQL / DataFrame`

![Gluten-UI](../image/gluten-ui.png)

This tab contains two parts:

1. The Gluten build information.
2. SQL/Dataframe queries fallback information.

If you want to disable Gluten UI, add a config when submitting `--conf spark.gluten.ui.enabled=false`.

## History server

Gluten UI also supports Spark history server. Add gluten-ui jar into the history server classpath, e.g., $SPARK_HOME/jars, then restart history server.
