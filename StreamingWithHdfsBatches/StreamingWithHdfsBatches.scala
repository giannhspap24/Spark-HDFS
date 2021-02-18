import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.hadoop.fs.{FileSystem, Path}

import java.nio.file.Paths
import scala.collection.mutable.ListBuffer

object StreamingWithHdfsBatches {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .master("spark://127.0.0.1:7077")
      .appName("Spark Project").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")


    val sc = spark.sparkContext
    //val ssc = new StreamingContext(sc, Seconds(2))

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

    val fs = FileSystem.get(new java.net.URI("hdfs://namenode:8020"), sc.hadoopConfiguration)
    val status = fs.listStatus(new Path("hdfs://namenode:8020/user/pap"))
    val sqlContext = spark.sqlContext
    var start=0;
    import sqlContext.implicits._
    def addAutakia(a:String) :String ={
      var result:String =a;
      result = "\""+a ;
      result = result.concat("\"");
      return result;
    }
    var inputpath1="";
    var inputpath2="";
    val filePaths= new ListBuffer[String];
    var a=0;
    var b=0;
    var aa=0;
    var bb=0;
    var cc=0;
    var dd=0;
    var temp1="";
    var temp2="";
    var x="0";

    def toStr(input:Int) : String={
        if(input<10){
          return x.concat(input.toString);
        }
         else{
          return input.toString;
        }
    }

    status.foreach(x => {
      filePaths +=x.getPath.toString;
      val tempDF = spark.read.schema(myschema).option("dateFormat", "yyyy-MM-ddThh:mm:ss").option("header", true).csv(filePaths: _*);
      //val tempDF = spark.read.schema(myschema).option("dateFormat", "yyyy-MM-ddThh:mm:ss").option("header", true).csv("hdfs://namenode:8020/user/pap/fares.00.csv",hdfs://namenode:8020/user/pap/fares.01.csv)

      tempDF.createOrReplaceTempView("fares")

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

      println("Choose a query(4, 5, 6, 7):")
      val input = scala.io.StdIn.readLine()
      println(input)
      input match {
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
            .save("hdfs://namenode:8020/user/pap/dataset/query4")
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
            .save("hdfs://namenode:8020/user/pap/dataset/query5")
        case "6" =>
          //        val query6 = spark.sql("select vendor, date, time, max(drives) as max from (select date_format(pickup_datetime, 'yyyy-MM-dd') " +
          //          "as date, date_format(pickup_datetime, 'HH:00:00') " +
          //          "as time, vendor_id as vendor, count(id) as drives " +
          //          "from query group by date, time, vendor order by date)")


          for(a<-1 to 12){
            for(b<-1 to 31){
              temp1=toStr(a);
              temp2=toStr(b);
              println("FOr vendor 1:\n");
              println(temp1,temp2);
              //          val query6 = spark.sql("SELECT  TOP 1 CAST(pickup_datetime as date) AS 'ForDate', DATEPART(hh, pickup_datetime) AS 'OnHour', COUNT (*) AS 'Courses' FROM fares WHERE pickup_datetime BETWEEN '2016-"+temp1+"-"+temp2 + " 00:00:00.000' AND '2016-"+temp1+ "-"+temp2 + " 23:59:59.999' and vendor_id=1 GROUP BY CAST(pickup_datetime as date), DATEPART(hh, pickup_datetime) ORDER BY Courses DESC;");
              //          query6.show(5, false)
              val  query6=spark.sql("SELECT  TOP 1 CAST(pickup_datetime as date) AS 'ForDate', DATEPART(hh, pickup_datetime) AS 'OnHour', COUNT (*) AS 'Courses' FROM fares WHERE pickup_datetime BETWEEN '2016-01-01 00:00:00.000' AND '2016-01-01 23:59:59.999' and vendor_id=1 GROUP BY CAST(pickup_datetime as date), DATEPART(hh, pickup_datetime) ORDER BY Courses DESC;")
              query6.show(5, false)
              query6.repartition(1)
                .write
                .option("header", "true")
                .format("com.databricks.spark.csv")
                .mode("append")
                .save("hdfs://namenode:8020/user/alex/dataset/query6")
              println("-------- \n" );
            }
          }
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
            .save("hdfs://namenode:8020/user/pap/dataset/query7")
        case _ => println("Wrong input!")
      }


      Thread.sleep(1000)
    }
    )
    //val myFile = sc.textFile(x.getPath.toString)
  }
}

