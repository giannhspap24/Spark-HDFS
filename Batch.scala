import org.apache.hadoop.fs.FileSystem
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
//import org.apache.spark.implicits._

object Batch {


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("BatchProject")

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

    //val spark = SparkSession.builder().master("local[*]").appName("Batch").getOrCreate()
    val spark = SparkSession.builder().master("spark://127.0.0.1:7077").appName("Batch").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    println("Choose a query(1, 2, 3, 4, 5, 6, 7):")
    val input = scala.io.StdIn.readLine()

    //Load data
    val tempDF = spark.read.schema(myschema).option("dateFormat", "yyyy-MM-ddThh:mm:ss").option("header",true).csv("hdfs://namenode:8020/user/alex/dataset/fares.00.csv",
                                                                                                                          "hdfs://namenode:8020/user/alex/dataset/fares.01.csv",
                                                                                                                          "hdfs://namenode:8020/user/alex/dataset/fares.02.csv",
                                                                                                                          "hdfs://namenode:8020/user/alex/dataset/fares.03.csv")
//        val tempDF = spark.read.schema(myschema).option("dateFormat", "yyyy-MM-ddThh:mm:ss").option("header",true).csv("file:///home/alex/Downloads/dataset/fares.00.csv",
//                                                                                                                              "file:///home/alex/Downloads/dataset/fares.01.csv",
//                                                                                                                              "file:///home/alex/Downloads/dataset/fares.02.csv",
//                                                                                                                              "file:///home/alex/Downloads/dataset/fares.03.csv")
//val tempDF = spark.read.schema(myschema).option("dateFormat", "yyyy-MM-ddThh:mm:ss").option("header",true).csv("file:///home/alex/Downloads/dataset/fares.csv")
    tempDF.createOrReplaceTempView("fares")
    //tempDF.show()

    val avg_lon = spark.sql("select avg(pickup_longitude) as avg_lon from fares")
    val avg_lat = spark.sql("select avg(pickup_latitude) as avg_lat from fares")

    avg_lon.createOrReplaceTempView("avg_lon")
    avg_lat.createOrReplaceTempView("avg_lat")

    val quartiles = spark.sql("select *, " +
                                      "case " +
                                      "when pickup_longitude > avg_lon and pickup_latitude > avg_lat then '1' " +
                                      "when pickup_longitude < avg_lon and pickup_latitude > avg_lat then '2' " +
                                      "when pickup_longitude < avg_lon and pickup_latitude < avg_lat then '3' " +
                                      "else '4' " +
                                      "end as quartiles from fares, avg_lon, avg_lat")

    quartiles.createOrReplaceTempView("query")
    //quartiles.show()
    println(input)
    input match {
      case "1" =>
        val query1 = spark.sql("select date_format(pickup_datetime, 'yyyy-MM-dd') as date, quartiles as quartiles, count(id) as id from query " +
          "group by date, quartiles order by date, quartiles")

        query1.show()

        query1.repartition(1)
          .write
          .option("header", "true")
          .format("com.databricks.spark.csv")
          .mode("overwrite")
          .save("hdfs://namenode:8020/user/alex/dataset/query1")
      case "2" =>
        val query2_a = spark.sql("select avg(trip_duration) as trip_duration, quartiles as quartile from query group by quartiles " +
          "order by trip_duration desc limit 1")

        query2_a.show()

        query2_a.repartition(1)
          .write
          .option("header", "true")
          .format("com.databricks.spark.csv")
          .mode("overwrite")
          .save("hdfs://namenode:8020/user/alex/dataset/query2_a")

        val query2_b = spark.sql("select avg(distance), quartile " +
          "from (select 111.111 * degrees(acos(least(1.0, cos(radians(pickup_latitude)) * " +
          "cos(radians(dropoff_latitude)) * " +
          "cos(radians(pickup_longitude - dropoff_longitude)) " +
          "+ sin(radians(pickup_latitude)) " +
          "* sin(radians(dropoff_latitude))))) as distance, quartiles as quartile from query)" +
          "group by quartile " +
          "order by avg(distance) desc limit 1")

        query2_b.show()

        query2_b.repartition(1)
          .write
          .option("header", "true")
          .format("com.databricks.spark.csv")
          .mode("overwrite")
          .save("hdfs://namenode:8020/user/alex/dataset/query2_b")
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
      case "4" =>
        val query4 = spark.sql("select time, id from " +
          "(select date_format(pickup_datetime, 'HH:00:00') as time, count(id) as id " +
          "from query group by time) order by time")

        query4.show()

        query4.repartition(1)
          .write
          .option("header", "true")
          .format("com.databricks.spark.csv")
          .mode("overwrite")
          .save("hdfs://namenode:8020/user/alex/dataset/query4")
      case "5" =>
        println("Give pickup longitude:")
        //val pickup_lon = "-73.98"
        val pickup_lon = scala.io.StdIn.readLine()
        //val pickup_lat = "40.76"
        println("Give pickup latitude:")
        val pickup_lat = scala.io.StdIn.readLine()

        println("Give dropoff longitude:")
        //val dropoff_lon = "-73.96"
        val dropoff_lon = scala.io.StdIn.readLine()
        println("Give dropoff latitude:")
        //val dropoff_lat = "40.76"
        val dropoff_lat = scala.io.StdIn.readLine()

        println("Give time:")
        //val time = "17:24"
        val time = scala.io.StdIn.readLine()

        val pickup_lon_double = pickup_lon.toDouble + 1.0
        val pickup_lat_double = pickup_lat.toDouble + 1.0

        val dropoff_lon_double = dropoff_lon.toDouble + 1.0
        val dropoff_lat_double = dropoff_lat.toDouble + 1.0

        val query5 = spark.sql("select date_format('" + time + "', 'HH:00:00') as time, count(id) from query where(" +
          "(date_format(pickup_datetime, 'HH:00:00')=date_format('" + time + "', 'HH:00:00') or " +
          "date_format(pickup_datetime, 'HH:00:00')=date_format('" + time + "', 'HH:00:00')) and " +
          "id in (select id from query where((pickup_longitude between " + pickup_lon + " and "+ pickup_lon_double.toString +") and " +
          "(pickup_latitude between " + pickup_lat + " and "+ pickup_lat_double.toString +") and " +
          "(dropoff_longitude between " + dropoff_lon + " and "+ dropoff_lon_double.toString +") and " +
          "(dropoff_latitude between " + dropoff_lat + " and "+ dropoff_lat_double.toString +"))))")

        query5.show()

        query5.repartition(1)
          .write
          .option("header", "true")
          .format("com.databricks.spark.csv")
          .mode("overwrite")
          .save("hdfs://namenode:8020/user/alex/dataset/query5")
      case "6" =>
//        val query6 = spark.sql("select vendor, date, time, max(drives) as max from (select date_format(pickup_datetime, 'yyyy-MM-dd') " +
//          "as date, date_format(pickup_datetime, 'HH:00:00') " +
//          "as time, vendor_id as vendor, count(id) as drives " +
//          "from query group by date, time, vendor order by date)")

        val query6 = spark.sql("select date, time, vendor, max(drives) from (select date_format(pickup_datetime, 'yyyy-MM-dd') as date, " +
          "date_format(pickup_datetime, 'HH:00:00') as time, vendor_id as vendor, count(id) as drives " +
          "from query group by date, time, vendor order by date, time, vendor) group by date, time, vendor, drives order by date, time, vendor")

        query6.show()

        query6.repartition(1)
          .write
          .option("header", "true")
          .format("com.databricks.spark.csv")
          .mode("overwrite")
          .save("hdfs://namenode:8020/user/alex/dataset/query6")
      case "7" =>
        val query7 = spark.sql("select date, time, id " +
          "from(select date_format(pickup_datetime, 'yyyy-MM-dd') as date, " +
          "date_format(pickup_datetime, 'HH:00:00') as time, count(id) as id from query group by date, time order by date, time ) " +
          "where weekday(date) = 5 or weekday(date) = 6")

        query7.show()

        query7.repartition(1)
          .write
          .option("header", "true")
          .format("com.databricks.spark.csv")
          .mode("overwrite")
          .save("hdfs://namenode:8020/user/alex/dataset/query7")
      case _ => println("Wrong input!")
    }
  }
}
