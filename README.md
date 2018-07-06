# ClimateProject

Assumptions:

If humidity > 100 it will precipitate, if temp > 0 it will rain and if temp < 0 it will snow
Jar file can be executed via spark submit command: spark-submit --class com.ravz.spark.Project Weather.jar

To execute the job on cluster we can create a bundled jar file using sbt and execute that jar on cluster.
