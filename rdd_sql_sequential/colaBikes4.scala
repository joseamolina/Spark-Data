// Databricks notebook source
import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
// --------------------------------------------------------
//           SCALA PROGRAM
// Here is where we are going to define our set of...
// - Imports
// - Global Variables
// - Functions
// ...to achieve the functionality required.
// --------------------------------------------------------
import java.text.SimpleDateFormat

class MyProgram extends Serializable {

  def ex1(sc:SparkContext, myDataSetFile:String): Unit = {
    
    val file = sc.textFile(myDataSetFile)
    val rows = file.map(_.split(";")).map{case Array(a,b,c,d,e,f,g) => 
(a.toInt,b,c.toDouble,d.toDouble, e, f.toInt,g.toInt)}.toDF("status", "name", "longitude", "latitude", "dateStatus", "bikesAvailable", "docksAvailable")
    
    val n_bikes_cero = rows.filter($"status" === 0 && $"bikesAvailable" === 0)
    n_bikes_cero.groupBy("name").count().orderBy($"count".desc).show()
    
  }
  
  def ex2(sc:SparkContext, myDataSetFile:String): Unit = {
    
    // Date format transformation
    val inputFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss")
    val outputFormat = new SimpleDateFormat("dd-MM-yyyy")
    
    val file = sc.textFile(myDataSetFile)
    val rows = file.map(_.split(";")).map{case Array(a,b,c,d,e,f,g) => 
(a.toInt,b,c.toDouble,d.toDouble, outputFormat.format(inputFormat.parse(e)),f.toInt,g.toInt)}.toDF("status", "name", "longitude", "latitude", "dateStatus", "bikesAvailable", "docksAvailable")
  
     rows.filter($"status" === 0 && $"dateStatus" === "27-08-2017").groupBy("name").sum("bikesAvailable").orderBy(desc("sum(bikesAvailable)")).show()
  }
  
  def ex3(sc:SparkContext, myDataSetFile:String): Unit = {
    val inputFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss")
    val outputFormat = new SimpleDateFormat("HH dd-MM-yyyy")
    
    val file = sc.textFile(myDataSetFile)
    val rows = file.map(_.split(";")).map{case Array(a,b,c,d,e,f,g) => 
(a.toInt,b,c.toDouble,d.toDouble, outputFormat.format(inputFormat.parse(e)),f.toInt,g.toInt)}.toDF("status", "name", "longitude", "latitude", "dateStatus", "bikesAvailable", "docksAvailable").filter($"status" === 0 && $"dateStatus".like("%27-08-2017"))
    
    val final_stat = rows.groupBy("name", "dateStatus").mean("bikesAvailable").orderBy("name", "dateStatus")
    final_stat.show(final_stat.count.toInt)
  }
  
  def ex4(sc:SparkContext, myDataSetFile:String): Unit = {
    
    val file = sc.textFile(myDataSetFile)
    val rows = file.map(_.split(";")).map{case Array(a,b,c,d,e,f,g) => 
(a.toInt,b,c.toDouble,d.toDouble, e,f.toInt,g.toInt)}.toDF("status", "name", "longitude", "latitude", "dateStatus", "bikesAvailable", "docksAvailable").filter($"status" === 0 && $"dateStatus".like("27-08-2017%") && $"bikesAvailable" === 0)
    
    val part1 = Window.partitionBy("name").orderBy("dateStatus")
    
    val ret = rows.withColumn("group_stats", lag($"dateStatus", 1, 0).over(part1)).withColumn("group_stats", unix_timestamp(col("dateStatus"), "dd-MM-yyyy HH:mm:ss") - unix_timestamp(col("group_stats"), "dd-MM-yyyy HH:mm:ss")).where($"group_stats" =!= 300 || $"group_stats".isNull).orderBy($"dateStatus").select($"dateStatus", $"name").show()
  } 
  
  // Improved version: For each ranout shows the stations that share the most common number of bikes available
  def ex5(sc:SparkContext, myDataSetFile:String, ran_out_times:List[String]): Unit = {
    
    val date_day = "27-08-2017"
    val inputFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss")
    val outputFormat = new SimpleDateFormat("dd-MM-yyyy")
    val out_time = new SimpleDateFormat("HH:mm:ss")
    
    val df_time = ran_out_times.map(t => date_day + " " + t)
    
    val file = sc.textFile(myDataSetFile)
    val rows = file.map(_.split(";")).map{case Array(a,b,c,d,e,f,g) => 
(a.toInt,b,c.toDouble,d.toDouble, e,f.toInt,g.toInt)}.toDF("status", "name", "longitude", "latitude", "dateStatus", "bikesAvailable", "docksAvailable").filter($"status" === 0 && $"dateStatus".isin(df_time: _*))
    
    val window_rows = Window.partitionBy("dateStatus").orderBy($"bikesAvailable".desc)
    
    val act = rows.withColumn("max_num", max($"bikesAvailable") over (window_rows)).filter($"bikesAvailable" === $"max_num" && $"docksAvailable" =!=  0).orderBy("dateStatus").select($"dateStatus", $"name", $"max_num")
    
    act.show(act.count.toInt)
    
  } 

  // ------------------------------------------
  // FUNCTION main
  // ------------------------------------------  
  def myMain(sc:SparkContext, myDataSetDir:String, option:Int, ran_out_times:List[String]): Unit = {
    
      option match {
      case 1  => ex1(sc, myDataSetDir)
      case 2  => ex2(sc, myDataSetDir)
      case 3  => ex3(sc, myDataSetDir)
      case 4  => ex4(sc, myDataSetDir)
      case 5  => ex5(sc, myDataSetDir, ran_out_times)
      case whoa  => println("Unexpected case: " + whoa.toString)
    }
    
  }
} 

// ---------------------------------------------------------------
//                      SCALA EXECUTION
// This is the main entry point to the execution of our program.
// It provides a call to the 'main function' defined in our
// Scala program.
// ---------------------------------------------------------------
val option = 5

val ran_out_times: List[String] = List("06:03:00", "06:03:00", "08:58:00", "09:28:00", "10:58:00", "12:18:00", "12:43:00", "12:43:00", "13:03:00", "13:53:00", "14:28:00", "14:28:00", "15:48:00", "16:23:00", "16:33:00", "16:38:00", "17:09:00", "17:29:00", "18:24:00", "19:34:00", "20:04:00", "20:14:00", "20:24:00", "20:49:00", "20:59:00", "22:19:00", "22:59:00", "23:14:00", "23:44:00")

val localFalseDatabricksTrue = false

val my_local_path = "/home/nacho/CIT/Tools/MyCode/Spark/"
val my_databricks_path = "/"
var myDataSetDir = "/FileStore/tables/tiny_dataset"

new MyProgram().myMain(sc, myDataSetDir, option, ran_out_times)



