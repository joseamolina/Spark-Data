// Databricks notebook source
// --------------------------------------------------------
//           SCALA PROGRAM
// Here is where we are going to define our set of...
// - Imports
// - Global Variables
// - Functions
// ...to achieve the functionality required.
// --------------------------------------------------------

class MyProgram extends Serializable {

  
  def processLine(line: String): Tuple7[String, String, String, String, String, String, String] = {
      val ese: String = line.replace("\n", "")
      val params:Array[String] = ese.split(";")
      val tuple_new = (params(0), params(1), params(2), params(3), params(4), params(5), params(6))
      return tuple_new
  }
  
  def processLine5(line: String): Tuple2[String, Double] = {
      val ese: String = line.replace("\n", "")
      val params:Array[String] = ese.split(";")
      val tuple_new = (params(1), params(6).toDouble)
      return tuple_new
  }
  
  def ex1(myDataSetFile:String): Unit = {
    val inputRDD = sc.textFile(myDataSetFile)
    println(inputRDD.count())
  }
  
  def ex2(myDataSetFile:String): Unit = {
    val inputRDD = sc.textFile(myDataSetFile)
    val n = inputRDD.map(t => processLine(t))
    val stations = n.map(s => s._2).distinct().count()
    
    println(stations)
  }
  
  def ex3(myDataSetFile:String): Unit = {
    val inputRDD = sc.textFile(myDataSetFile)
    val n = inputRDD.map(t => processLine(t))
    val stations = n.map(s => s._2).distinct()
    val stations_colected = stations.collect()
 
    for (name <- stations_colected) println(name)
  }
  
  def ex4(myDataSetFile:String): Unit = {
    val inputRDD = sc.textFile(myDataSetFile)
    val n = inputRDD.map(t => processLine(t))
    val stations = n.map(s => (s._2, s._3)).distinct().sortBy(_._2)
    val stations_colected = stations.collect()
 
    for (name <- stations_colected) println(name)
  } 
  
  def ex5(myDataSetFile:String): Unit = {
    val inputRDD = sc.textFile(myDataSetFile)
    val n = inputRDD.map(t => processLine5(t))
    val kent_bikes = n.filter(s => s._1 == "Kent Station").map(s => s._2).mean()
    println(kent_bikes)                         
  } 

  // ------------------------------------------
  // FUNCTION main
  // ------------------------------------------  
  def myMain(myDataSetDir:String, option:Int): Unit = {
    
      option match {
      case 1  => ex1(myDataSetDir)
      case 2  => ex2(myDataSetDir)
      case 3  => ex3(myDataSetDir)
      case 4  => ex4(myDataSetDir)
      case 5  => ex5(myDataSetDir)
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
val myDataSetDir = "/FileStore/tables/tiny_dataset"
val option = 5
new MyProgram().myMain(myDataSetDir, option)