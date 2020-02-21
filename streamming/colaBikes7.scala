// --------------------------------------------------------
//           SCALA PROGRAM
// Here is where we are going to define our set of...
// - Imports
// - Global Variables
// - Functions
// ...to achieve the functionality required.
// --------------------------------------------------------
import sqlContext.implicits._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.mean
import java.text.SimpleDateFormat

class MyProgram extends Serializable {
  
  def ex1(myDataSetFile:String): Unit = {
    
    val file = sc.textFile(myDataSetFile)
    val rows = file.map(_.split(";")).map{case Array(a,b,c,d,e,f,g,h) => 
(a,b,c,d,e,f,g,h)}.toDF("station_number", "station_name", "direction", "day_of_week", "date", "query_time", "scheduled_time", "expected_arrival_time")
    println(rows.count())
  }
  
  def ex2(myDataSetFile:String, station_num:Int): Unit = {
    
    val file = sc.textFile(myDataSetFile)
    val rows = file.map(_.split(";")).map{case Array(a,b,c,d,e,f,g,h) => 
(a,b,c,d,e,f,g,h)}.toDF("station_number", "station_name", "direction", "day_of_week", "date", "query_time", "scheduled_time", "expected_arrival_time")
    val tal = rows.select("date").filter($"station_number" === station_num).distinct().count()
    println(tal)
  }
  
  def ex3(myDataSetFile:String, station_num:Int): Unit = {
     
    val file = sc.textFile(myDataSetFile)
    val rows = file.map(_.split(";")).map{case Array(a,b,c,d,e,f,g,h) => 
(a,b,c,d,e,f,g,h)}.toDF("station_number", "station_name", "direction", "day_of_week", "date", "query_time", "scheduled_time", "expected_arrival_time")
    val tal = rows.select("date").filter($"station_number" === station_num)
    
    val time_n = tal.count()
    val ret_n = tal.filter($"scheduled_time" < $"expected_arrival_time").count()
    
    println("Ahead: " + (time_n - ret_n).toString + ", behind: " + ret_n)
    
  }
  
  def ex4(myDataSetFile:String, station_num:Int): Unit = {
    // Convert from text file directory to a dataframe sql with name renamed
    val file = sc.textFile(myDataSetFile)
    val rows = file.map(_.split(";")).map{case Array(a,b,c,d,e,f,g,h) => 
(a,b,c,d,e,f,g,h)}.toDF("station_number", "station_name", "direction", "day_of_week", "date", "query_time", "scheduled_time", "expected_arrival_time").filter($"station_number" === station_num)
    val new_l = rows.select("day_of_week", "scheduled_time").distinct().groupBy("day_of_week").agg(collect_list("scheduled_time").as("Times")).show(false)
    
  } 
  
  def ex5(myDataSetFile:String, station_num:Int, monthList:Seq[String]): Unit = {
    
    //format.parse(t._6).getTime()
    val format = new SimpleDateFormat("hh:mm:ss")
    // Convert from text file directory to a dataframe sql with name renamed
    val file = sc.textFile(myDataSetFile)
    val rows = file.map(_.split(";")).map{case Array(a,b,c,d,e,f,g,h) => 
(a,b,c,d,e.slice(3, 5),format.parse(f).getTime(),g,format.parse(h).getTime())}.toDF("station_number", "station_name", "direction", "day_of_week", "date", "query_time", "scheduled_time", "expected_arrival_time").filter($"station_number" === station_num && $"date" <= "11" && $"date" >= "09")
    
    rows.withColumn("new_date", concat($"day_of_week", lit(" "), $"date")).groupBy("new_date").agg(mean($"expected_arrival_time" - $"query_time").as("average (millisecs)")).orderBy(asc("average (millisecs)")).show()
    
  } 

  // ------------------------------------------
  // FUNCTION main
  // ------------------------------------------  
  def myMain(myDataSetDir:String, option:Int): Unit = {
    
      option match {
      case 1  => ex1(myDataSetDir)
      case 2  => ex2(myDataSetDir, 240101)
      case 3  => ex3(myDataSetDir, 240561)
      case 4  => ex4(myDataSetDir, 241111)
      case 5  => ex5(myDataSetDir, 240491, Seq("09", "10", "11"))
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
val myDataSetDir = "/FileStore/tables/my_data_single_file"
val option = 5
new MyProgram().myMain(myDataSetDir, option)