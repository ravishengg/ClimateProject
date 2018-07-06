package com.ravz.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.util.Try

object Climate {

  //isFloat: Test whether given string is float- returns true or false
  def isFloat(aString: String): Boolean = Try(aString.toFloat).isSuccess

  //parse line in given RDD and return flag for isFloat test
  def parseLine(line: String) = {
    val fields = line.split(",")
    val flag1 = isFloat(fields(0))
    val flag2 = isFloat(fields(1))
    val flag = flag1 && flag2

    (flag, fields(4), fields(7), fields(9), fields(10), fields(11), fields(23), fields(27))
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    //Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

    //read airports.dat file
    val airportData = spark.read.format("csv").option("header", "false").option("ignoreLeadingWhiteSpace", "true").load("/user/ravishengg/airport/")

    //Register Temp table with airport Data
    airportData.createOrReplaceTempView("airport")

    //read airports.dat file
    val weatherStDataUnparsed = spark.read.format("csv").option("header", "true").option("ignoreLeadingWhiteSpace", "true").load("/user/ravishengg/weather/csv/")
    //Parse the weather File: Sanity check Avg Temp and Pressure- IsFloat

    val weatherStRddUnparsed = weatherStDataUnparsed.rdd
    val weatherStRddParsed = weatherStRddUnparsed.map(x => x(9) + "," + x(21) + "," + x).map(parseLine)

    //Filter the row if Avg Temp or Pressure column have non FLoat value
    val erroRdd = weatherStRddParsed.filter(x => x._1 != true)

    //filteredWeatherRdd with filtered data
    val filteredWeatherRdd = weatherStRddParsed.filter(x => x._1 == true).map(x => (x._2, x._3, x._4, x._5, x._6, x._7, x._8))

    //Convert RDD to DS
    import spark.implicits._
    val weatherStData = filteredWeatherRdd.toDS

    //convert datetime from yyyyMMddHHmmss to y-MM-dd'T'hh:mm:ss'Z'
    val weatherDFTstamp = weatherStData.select(col("_1").as("name"), date_format(unix_timestamp(col("_2"), "yyyyMMddHHmmss").cast(TimestampType).as("timestamp"), "y-MM-dd'T'hh:mm:ss'Z'").as("local_date_time_full"), col("_3").as("lat"), col("_4").as("lon"), col("_5").as("apparent_t"), col("_6").as("press_qnh"), col("_7").as("rel_hum"))

    //Register Temp View with weather data and required timestamp format
    weatherDFTstamp.createOrReplaceTempView("weatherTblTstamp")

    //Convert String to Decimal in weather data
    val weatherDFcast = spark.sqlContext.sql("select name,local_date_time_full,cast(lat as decimal(10,1)) as lat,cast(lon as decimal(10,1)) as lon," +
      " cast(apparent_t as decimal(10,2)) as avg_temp,cast(press_qnh as decimal(10,2)) as pressure ,cast(rel_hum as decimal(10,2)) as humidity from  weatherTblTstamp")

    //Register Temp View with Decimal data
    weatherDFcast.createOrReplaceTempView("weather")

    //If humidity > 100 it will precipitate, if temp > 0 it will rain and if temp < 0 it will snow
    val climateDF = spark.sqlContext.sql("select name,local_date_time_full,lat,lon," +
      " case when (humidity > 100 and avg_temp > 0) then \"Rain\" " +
      "when (humidity > 100 and avg_temp <= 0) then \"Snow\" " +
      "else \"Sunny\" end as Climate, " +
      " avg_temp,pressure ,humidity  from  weather")

    //Register Temp View with Climate Column
    climateDF.createOrReplaceTempView("Climate")

    //Convert String to Decimal in Airport data
    val airportCast = spark.sqlContext.sql("Select _c4 as Code,cast(_c6 as decimal(10,1)) as lat,cast(_c7 as decimal(10,1)) as lon from airport")

    //Register Airport Temp View with IATACode,Latitude,longitude
    airportCast.createOrReplaceTempView("IATACOde")

    //Join Climate with airport data on Latitude and Longitude to get IATA Code
    val resultdf = spark.sqlContext.sql("Select l.code,concat(cast(c.lat as char(6)),\",\",cast(c.lon as char(6))),c.local_date_time_full,c.climate,cast(c.avg_temp as char(8)), " +
      " cast(c.pressure as char(8)),cast(c.humidity as char(8)) from " +
      " Climate c left outer join IATACOde l ON " +
      " l.lat = c.lat and l.lon = c.lon ")

    //Convert DF to RDD
    val resultrdd = resultdf.rdd

    //Create a single column row with String Data Type and save as Text File
    resultrdd.map(x => x(0) + "|" + x(1) + "|" + x(2) + "|" + x(3) + "|" + x(4) + "|" + x(5) + "|" + x(6)).coalesce(1, shuffle = true).saveAsTextFile("/user/ravishengg/result/")
    erroRdd.coalesce(1, shuffle = true).saveAsTextFile("/user/ravishengg/result/error")
  }
}
