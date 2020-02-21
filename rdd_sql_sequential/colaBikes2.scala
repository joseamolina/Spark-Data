// Databricks notebook source
// --------------------------------------------------------
//           SCALA PROGRAM
// Here is where we are going to define our set of...
// - Imports
// - Global Variables
// - Functions
// ...to achieve the functionality required.
// --------------------------------------------------------
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Arrays;

class MyProgram extends Serializable {
  
  def processLineN(line: String): Tuple7[Int, String, Float, Float, String, Int, Int] = {
      val ese: String = line.replace("\n", "")
      val params:Array[String] = ese.split(";")
      val tuple_new = (params(0).toInt, params(1), params(2).toFloat, params(3).toFloat, params(4), params(5).toInt, params(6).toInt)
      return tuple_new
  }
  
  def summerTuple(acc:Int, summer:Tuple6[Int, Float, Float, String, Int, Int]): Int = {
      return acc + summer._5
  }
  
  def processLine(line: String): Tuple2[String, Tuple6[Int, Float, Float, String, Int, Int]] = {
      val ese: String = line.replace("\n", "")
      val params:Array[String] = ese.split(";")
      val tuple_new = (params(1), (params(0).toInt, params(2).toFloat, params(3).toFloat, params(4), params(5).toInt, params(6).toInt))
      return tuple_new
  }
    
  def processLine4(line: String): Tuple2[String, Tuple7[Int, String, Float, Float, String, Int, Int]] = {
      val ese: String = line.replace("\n", "")
      val params:Array[String] = ese.split(";")
      val tuple_new = (params(1), (params(0).toInt, params(1), params(2).toFloat, params(3).toFloat, params(4), params(5).toInt, params(6).toInt))
      return tuple_new
  }
  
  def ex1(sc:SparkContext, myDataSetFile:String): Unit = {
    
    val inputRDD = sc.textFile(myDataSetFile)
    val n_bikes = inputRDD.map(t => processLine(t))
    
    val n_bikes_cero = n_bikes.filter(t => (t._2._1 == 0 && t._2._5 == 0))
    val res_bikes = n_bikes_cero.countByKey()
    
    res_bikes.toSeq.sortWith(_._2 > _._2).foreach(println)
    println("\n")
    
  }
  
  def ex2(sc:SparkContext, myDataSetFile:String): Unit = {
    
    // Initial preprocessing operations
    val inputRDD = sc.textFile(myDataSetFile)
    val n_bikes = inputRDD.map(t => processLine(t))
    
    // Date format transformation
    val inputFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss")
    val outputFormat = new SimpleDateFormat("dd-MM-yyyy")
    
    // Filter the RDD by being operative and date 27-08-2017
    val n_bikes_cero = n_bikes.filter(t => (t._2._1 == 0 && outputFormat.format(inputFormat.parse(t._2._4)) == "27-08-2017"))
    
    // Higher order functions applied to aggregate
    val initial_value = 0
    val addOpp = (acc: Int, value: Tuple6[Int, Float, Float, String, Int, Int]) => acc + value._5
    val mergeOpp = (p1: Int, p2: Int) => p1 + p2 
    
    // Aggregate function applied to the sum of the interested value -bikes available- in the tuple V
    val y = n_bikes_cero.aggregateByKey(initial_value)(addOpp, mergeOpp).sortBy(_._2, false)
    
    // Print value
    y.collect().foreach(println)
    println("\n")
  }
  
  def ex3(sc:SparkContext, myDataSetFile:String): Unit = {
    
    // Initial preprocessing operations
    val inputRDD = sc.textFile(myDataSetFile)
    val n_bikes = inputRDD.map(t => processLine(t))
    
    // Date format transformation
    val inputFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss")
    val outputFormat = new SimpleDateFormat("dd-MM-yyyy")
    val out_hour = new SimpleDateFormat("HH")
  
    // Filter the RDD by being operative and date 27-08-2017
    val n_bikes_cero = n_bikes.filter(t => (t._2._1 == 0 && outputFormat.format(inputFormat.parse(t._2._4)) == "27-08-2017"))
    
    // Map the values to get the Key (Name_station, time_slot) and the Value Number of available bikes
    val obj_hour = n_bikes_cero.map(t => ((t._1, out_hour.format(inputFormat.parse(t._2._4))), t._2._5))
    
    // Group by the key. After that calculate the mean
    val grouped_time_stations = obj_hour.groupByKey().map{case (x, y) => (x, y.sum.toFloat / y.size)}
    
    // Print the results and sort by the key
    for (name <- grouped_time_stations.sortByKey().collect()) println(name)
  }
  
  // This method takes all ranouts and filters by non consequtive
  def getRanOuts(cb: Array[Tuple7[Int, String, Float, Float, String, Int, Int]]): Seq[Tuple2[String, String]] = {
    
    val pattern_date = "dd-MM-yyyy HH:mm:ss";
    val out_time =  new SimpleDateFormat("HH:mm:ss")
    val formatter = new SimpleDateFormat(pattern_date);
    
    var list_ret = Seq[Tuple2[String, String]]()
    val sdf = new SimpleDateFormat(pattern_date)
    
    var initial = sdf.parse("01-01-1970 00:00:00")
    var rest:Long = 0
    
    for ( form <- cb) {
      val current = sdf.parse(form._5)
      rest = current.getTime() - initial.getTime()
      if (rest != 300000) {
        list_ret = list_ret :+ (out_time.format(formatter.parse(form._5)), form._2)
      }
      initial = current
    }
    return list_ret
  }
  
  def ex4(sc:SparkContext, myDataSetFile:String): Unit = {
    
    // Initial preprocessing operations
    val inputRDD = sc.textFile(myDataSetFile)
    val n_bikes = inputRDD.map(t => processLine4(t))
    
    // Date format transformation
    val inputFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss")
    val time_out = new SimpleDateFormat("HH:mm:ss")
    val outputFormat = new SimpleDateFormat("dd-MM-yyyy")
    
    // Filter the RDD by being operative and date 27-08-2017
    val n_bikes_cero = n_bikes.filter(t => t._2._1 == 0 && outputFormat.format(inputFormat.parse(t._2._5)) == "27-08-2017" && t._2._6 == 0)
    
    val grouped_stations = n_bikes_cero.groupByKey().mapValues(t => getRanOuts(t.toArray)).flatMap(t => t._2)
    
    for (name <- grouped_stations.collect().sortBy(_._1)) println(name)
    
  } 
  
  def myUpdateAccum(sp:Iterable[((String, (Int, Float, Float, String, Int, Int)), Int)]): Tuple2[String, Int] = {
     val rev = sp.maxBy(_._1._2._5)
     return (rev._1._1, rev._1._2._5)
  }
  
  def ex5(sc:SparkContext, myDataSetFile:String, ran_out_times:List[String]): Unit = {
    // Initial preprocessing operations
    val inputRDD = sc.textFile(myDataSetFile)
    val n_bikes = inputRDD.map(t => processLine(t))
    
    // Date format transformation
    val inputFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss")
    val outputFormat = new SimpleDateFormat("dd-MM-yyyy")
    val timeoutput = new SimpleDateFormat("HH:mm:ss")
    
    // Filter the RDD by being operative and date 27-08-2017
    val n_bikes_cero = n_bikes.filter(t => (t._2._1 == 0 && outputFormat.format(inputFormat.parse(t._2._4)) == "27-08-2017")).map(n => (timeoutput.format(inputFormat.parse(n._2._4)), (n._1, n._2)))
    
    
    val grupA = sc.parallelize(ran_out_times).map(n => (n, 0))
    val res = n_bikes_cero.join(grupA).groupByKey().mapValues(t => myUpdateAccum(t)).sortByKey()
    
    for (name <- res.collect()) println(name)
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



