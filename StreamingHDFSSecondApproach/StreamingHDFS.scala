//package com.testing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object StreamingHDFS extends App {
  val spark = SparkSession.builder()
    //.master("spark://127.0.0.1:7077")
    .master("local[*]")
    .appName("StreamingHDFS").getOrCreate()
  //spark.sparkContext.setLogLevel("WARN")
  val sc = spark.sparkContext
  //val ssc = new StreamingContext(sc, Seconds(2))

  var id = Array[String]()
  var vendorId = Array[String]()
  var pickupDate = Array[String]()
  var dropoffDate = Array[String]()
  var passengerCount = Array[String]()
  var pickupLon = Array[String]()
  var pickupLat = Array[String]()
  var dropoffLon = Array[String]()
  var dropoffLat = Array[String]()
  var store = Array[String]()
  var tripDur = Array[String]()
  var lines = Array[String]()

  val myschema = StructType(Array(
    StructField("id", StringType),
    StructField("vendor_id", IntegerType),
    StructField("pickup_datetime", StringType),
    StructField("dropoff_datetime", StringType),
    StructField("passenger_count", IntegerType),
    StructField("pickup_longitude", DoubleType),
    StructField("pickup_latitude", DoubleType),
    StructField("dropoff_longitude", DoubleType),
    StructField("dropoff_latitude", DoubleType),
    StructField("store_and_fwd_flag", StringType),
    StructField("trip_duration", IntegerType)
  ))

  println("Choose a query(3, 4, 6):")
  val input = scala.io.StdIn.readLine()

  case class CsvRow(id: String, vendor_id: String, pickup_datetime: String, dropoff_datetime: String,
                    passenger_count: String, pickup_longitude: String, pickup_latitude: String, dropoff_longitude: String,
                    dropoff_latitude: String, store_and_fwd_flag: String, trip_duration: String)

  while(true){
    val myFile = sc.textFile("hdfs://namenode:8020/user/alex/dataset/").collect()
    val fileSize = myFile.length
    println(fileSize)

    myFile.foreach(line => {
      val field = line.split(",")
      results(spark, field)
    })
  }

  def results(spark: SparkSession, field: Array[String]) {
    id = id :+ field(0)
    vendorId = vendorId :+ field(1)
    pickupDate = pickupDate :+ field(2)
    dropoffDate = dropoffDate :+ field(3)
    passengerCount = passengerCount :+ field(4)
    pickupLon = pickupLon :+ field(5)
    pickupLat = pickupLat :+ field(6)
    dropoffLon = dropoffLon :+ field(7)
    dropoffLat = dropoffLat :+ field(8)
    store = store :+ field(9)
    tripDur = tripDur :+ field(10)

    val columns = Array(id, vendorId, pickupDate, dropoffDate, passengerCount, pickupLon, pickupLat,
      dropoffLon, dropoffLat, store, tripDur).transpose

    val rdd = spark.sparkContext.parallelize(columns).map(col => CsvRow(col(0), col(1),
      col(2), col(3), col(4), col(5), col(6), col(7),
      col(8), col(9), col(10)))

    import spark.sqlContext.implicits._
    val df = rdd.toDF("id", "vendor_id", "pickup_datetime", "dropoff_datetime",
      "passenger_count", "pickup_longitude", "pickup_latitude",
      "dropoff_longitude", "dropoff_latitude", "store_and_fwd_flag",
      "trip_duration")

    df.createOrReplaceTempView("fares")

    //cases for stream queries
    input match {
      case "3" =>
        val query3 = spark.sql("select id, distance, trip_duration, passengers from(select 111.111 * degrees(acos(least(1.0, cos(radians(pickup_latitude)) * " +
          "cos(radians(dropoff_latitude)) * " +
          "cos(radians(pickup_longitude - dropoff_longitude)) " +
          "+ sin(radians(pickup_latitude)) " +
          "* sin(radians(dropoff_latitude))))) as distance, trip_duration as trip_duration, passenger_count as passengers, id as id from query)" +
          "where distance >= 10 and trip_duration >= 600 and passengers >= 2")

        query3.show()

        query3.repartition(1)
          .write
          .option("header", "true")
          .format("com.databricks.spark.csv")
          .mode("overwrite")
          .save("hdfs://namenode:8020/user/alex/dataset/query3")
      //            .save("file:///home/alex/Downloads/data/sql4/")
      case "4" =>
        val query4 = spark.sql("select time, id from (select date_format(pickup_datetime, 'HH:00:00') " +
          "as time, count(id) as id from fares group by time) order by time")

        query4.show()
        query4.repartition(1)
          .write
          .option("header", "true")
          .format("com.databricks.spark.csv")
          .mode("overwrite")
          .save("hdfs://namenode:8020/user/alex/stream/sql4")
      //.save("file:///home/alex/Downloads/data/sql4/")
      case "6" =>
        //vendor id -> 1
        val query6_1 = spark.sql("SELECT TOP 1 CAST(pickup_datetime as date) AS 'ForDate', " +
          "DATEPART(hh, pickup_datetime) AS 'OnHour', COUNT (*) AS 'Courses' FROM query " +
          "WHERE pickup_datetime BETWEEN '2016-01-01 00:00:00.000' AND '2016-01-01 23:59:59.999' " +
          "and vendor_id=1 GROUP BY CAST(pickup_datetime as date), DATEPART(hh, pickup_datetime) " +
          "ORDER BY Courses DESC")

        query6_1.show()

        query6_1.repartition(1)
          .write
          .option("header", "true")
          .format("com.databricks.spark.csv")
          .mode("overwrite")
          .save("hdfs://namenode:8020/user/alex/dataset/query6_1")

        //vendor id -> 2
        val query6_2 = spark.sql("SELECT TOP 1 CAST(pickup_datetime as date) AS 'ForDate', " +
          "DATEPART(hh, pickup_datetime) AS 'OnHour', COUNT (*) AS 'Courses' FROM query " +
          "WHERE pickup_datetime BETWEEN '2016-01-01 00:00:00.000' AND '2016-01-01 23:59:59.999' " +
          "and vendor_id=2 GROUP BY CAST(pickup_datetime as date), DATEPART(hh, pickup_datetime) " +
          "ORDER BY Courses DESC")

        query6_2.show()

        query6_2.repartition(1)
          .write
          .option("header", "true")
          .format("com.databricks.spark.csv")
          .mode("overwrite")
          .save("hdfs://namenode:8020/user/alex/dataset/query6_2")
      case _ => println("Wrong input!")
    }
  }
}
