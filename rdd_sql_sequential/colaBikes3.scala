// Databricks notebook source
// --------------------------------------------------------
//           SCALA PROGRAM
// Here is where we are going to define our set of...
// - Imports
// - Global Variables
// - Functions
// ...to achieve the functionality required.
// --------------------------------------------------------
import sqlContext.implicits._
import org.apache.spark.sql.functions.mean

class MyProgram extends Serializable {
  
  def ex1(myDataSetFile:String): Unit = {
    
    val rows = spark.read.textFile(myDataSetFile)
    println(rows.count())
  }
  
  def ex2(myDataSetFile:String): Unit = {
    
    // Convert from text file directory to a dataframe sql with name renamed
    val file = sc.textFile(myDataSetFile)
    val rows = file.map(_.split(";")).map{case Array(a,b,c,d,e,f,g) => 
(a.toInt,b,c.toDouble,d.toDouble,e,f.toInt,g.toInt)}.toDF("status", "name", "longitude", "latitude", "dateStatus", "bikesAvailable", "docksAvailable")
    val tal = rows.select("name").distinct().count()
    println(tal)
  }
  
  def ex3(myDataSetFile:String): Unit = {
     // Convert from text file directory to a dataframe sql with name renamed
    val file = sc.textFile(myDataSetFile)
    val rows = file.map(_.split(";")).map{case Array(a,b,c,d,e,f,g) => 
(a.toInt,b,c.toDouble,d.toDouble,e,f.toInt,g.toInt)}.toDF("status", "name", "longitude", "latitude", "dateStatus", "bikesAvailable", "docksAvailable")
   rows.select("name").distinct().show(rows.distinct.count.toInt, false)
  }
  
  def ex4(myDataSetFile:String): Unit = {
    // Convert from text file directory to a dataframe sql with name renamed
    val file = sc.textFile(myDataSetFile)
    val rows = file.map(_.split(";")).map{case Array(a,b,c,d,e,f,g) => 
(a.toInt,b,c.toDouble,d.toDouble,e,f.toInt,g.toInt)}.toDF("status", "name", "longitude", "latitude", "dateStatus", "bikesAvailable", "docksAvailable")
    
    rows.select("name", "longitude").distinct.orderBy($"longitude".desc).show(rows.distinct.count.toInt, false)
  } 
  
  def ex5(myDataSetFile:String): Unit = {
    // Convert from text file directory to a dataframe sql with name renamed
    val file = sc.textFile(myDataSetFile)
    val rows = file.map(_.split(";")).map{case Array(a,b,c,d,e,f,g) => 
(a.toInt,b,c.toDouble,d.toDouble,e,f.toInt,g.toInt)}.toDF("status", "name", "longitude", "latitude", "dateStatus", "bikesAvailable", "docksAvailable")
    
    rows.filter($"name" === "Kent Station").groupBy("name").agg(mean("bikesAvailable")).show(rows.distinct.count.toInt, false)
    
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
val option = 1
new MyProgram().myMain(myDataSetDir, option)



