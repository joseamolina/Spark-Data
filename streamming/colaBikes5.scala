Rimport java.text.SimpleDateFormat
import java.util.Date
// --------------------------------------------------------
//           SCALA PROGRAM
// Here is where we are going to define our set of...
// - Imports
// - Global Variables
// - Functions
// ...to achieve the functionality required.
// --------------------------------------------------------

class MyProgram extends Serializable {
  
  def processLine(line: String): Tuple8[Int, String, String, String, String, String, String, String] = {
      val ese: String = line.replace("\n", "")
      val params:Array[String] = ese.split(";")
      val tuple_new = (params(0).toInt, params(1), params(2), params(3), params(4), params(5), params(6), params(7))
      return tuple_new
  } 
  
  def processTime(line:Tuple8[Int, String, String, String, String, String, String, String]): Tuple2[String, String] = {
    val tuple_new = (line._4, line._7)
    return tuple_new
  }
  
  def ex1(myDataSetFile:String): Unit = {
    val inputRDD = sc.textFile(myDataSetFile)
    println(inputRDD.count())
  }
  
  def ex2(myDataSetFile:String, stationNumber:Int): Unit = {
  
    val inputRDD = sc.textFile(myDataSetFile)
    val n = inputRDD.map(t => processLine(t)).filter(t => t._1 == stationNumber).map(t => t._5).distinct().count()
    println(n)
  }
  
  def ex3(myDataSetFile:String, stationNumber:Int): Unit = {
    
    val inputRDD = sc.textFile(myDataSetFile)
    val n = inputRDD.map(t => processLine(t)).filter(t => t._1 == stationNumber)
    
    val time_in = n.count()
    val retarded = n.filter(t => t._7 < t._8).count()
    
    println(((time_in - retarded).toString, retarded))
  }
  
  def ex4(myDataSetFile:String, stationNumber:Int): Unit = {
    
    val inputRDD = sc.textFile(myDataSetFile)
    val n = inputRDD.map(t => processLine(t))
    
    val stations = n.filter(t => t._1 == stationNumber).map(t => processTime(t))
    val stations_colected = stations.groupByKey().map(t => (t._1, t._2.toList.distinct)).collect()
    
    stations_colected.foreach(println)
  } 
  
  def ex5(myDataSetFile:String, stationNumber:Int, monthList:Seq[String]): Unit = {  
    
    // Date format transformation
    val inputFormat = new SimpleDateFormat("dd/MM/yyyy")
    val outputFormat = new SimpleDateFormat("MM")
  
    val format = new SimpleDateFormat("hh:mm:ss")
    
    val inputRDD = sc.textFile(myDataSetFile)
    
   val n = inputRDD.map(t => processLine(t)).filter(t => t._1 == stationNumber && outputFormat.format(inputFormat.parse(t._5)) <= "11" && outputFormat.format(inputFormat.parse(t._5)) >= "09").map(t => ((t._4, outputFormat.format(inputFormat.parse(t._5))), format.parse(t._8).getTime() - format.parse(t._6).getTime()))
    
    val final_n = n.aggregateByKey((0, 0))((k, v) => (k._1 + v.toInt, k._2 + 1), (v1, v2) => (v1._1 + v2._1, v1._2 + v2._2)).mapValues(sum => 1.0 * sum._1 / sum._2).sortBy(_._2).collect()
    
   final_n.foreach(println)
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
val option = 4
new MyProgram().myMain(myDataSetDir, option)


