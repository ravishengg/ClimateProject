# ClimateProject

Assumptions:

If humidity > 100 it will precipitate, if temp > 0 it will rain and if temp < 0 it will snow

Process:
1. Weather folder has source files from Weather Stations
2. Airport folder has airports.dat file for IATA Code
3. Weather and Airport data is joined on Longitude and Latitude to get IATA Code.
4. Error Handling: Sanity check on Columns Avg Temp and Pressure, to check whether source value is Float or not.
   Rejected records are in folder: https://github.com/ravishengg/ClimateProject/tree/master/result/error
   
5. Output can be found at below path: https://github.com/ravishengg/ClimateProject/tree/master/result
6. build dependancies can be seen in sbt folder: https://github.com/ravishengg/ClimateProject/tree/master/sbt

7. Executable jar file is there in main folder: Filename: Climate.jar

8. jar file can be executed via spark submit command:
spark-submit Climate.jar

9. Dependancies used: Spark-core
