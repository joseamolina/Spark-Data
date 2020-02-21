import org.apache.spark.streaming._
import Math._
import java.text.SimpleDateFormat
import java.util.Date

class MyProgram extends Serializable {
  
  def processTime(line:Tuple8[Int, String, String, String, String, String, String, String]): Tuple2[String, String] = {
    val tuple_new = (line._4, line._7)
    return tuple_new
  }
  
  def processLine(line: String): Tuple8[Int, String, String, String, String, String, String, String] = {
      val ese: String = line.replace("\n", "")
      val params:Array[String] = ese.split(";")
      val tuple_new = (params(0).toInt, params(1), params(2), params(3), params(4), params(5), params(6), params(7))
      return tuple_new
  }
  
  def processLine5(line: String): Tuple2[String, Double] = {
      val ese: String = line.replace("\n", "")
      val params:Array[String] = ese.split(";")
      val tuple_new = (params(1), params(6).toDouble)
      return tuple_new
  }
  
  def ex1(myDataSetFile:String, ssc:StreamingContext, resultDir:String): Unit = {
    val inputDStream = ssc.textFileStream(myDataSetFile).count()
    inputDStream.cache()
    inputDStream.print()
    inputDStream.saveAsTextFiles(resultDir)
  }
  
  def ex2(myDataSetFile:String, ssc:StreamingContext, resultDir:String, stationNumber:Int): Unit = {
    
    val inputRDD = ssc.textFileStream(myDataSetFile)
    val n = inputRDD.map(t => processLine(t)).filter(t => t._1 == stationNumber).map(t => t._5)
    val new_n = n.transform( rdd => rdd.distinct()).count()
    
    new_n.cache()
    
    new_n.print()
    new_n.saveAsTextFiles(resultDir)
  }
  
  def ex3(myDataSetFile:String, ssc:StreamingContext, resultDir:String, stationNumber:Int): Unit = {
    val inputRDD = ssc.textFileStream(myDataSetFile)
    
    val n = inputRDD.map(t => processLine(t)).filter(t => t._1 == stationNumber).map(t => if (t._7 < t._8){ (0, 1) }else {(1, 0)} )
    val result = n.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    
    result.cache()
    result.print()
    
    result.saveAsTextFiles(resultDir)
  }
   
  def ex4(myDataSetFile:String, ssc:StreamingContext, resultDir:String, stationNumber:Int): Unit = {
    val inputRDD = ssc.textFileStream(myDataSetFile)
    val n = inputRDD.map(t => processLine(t))
    
    val stations = n.filter(t => t._1 == stationNumber).map(t => processTime(t))
    val stations_colected = stations.groupByKey().map(t => (t._1, t._2.toList.distinct))
    
    stations_colected.cache()
    stations_colected.print()
    
    stations_colected.saveAsTextFiles(resultDir)
  } 
  
  def ex5(myDataSetFile:String, ssc:StreamingContext, resultDir:String, stationNumber:Int, sequence:Seq[String]): Unit = {  
    
    val inputRDD = ssc.textFileStream(myDataSetFile)
    
    // Date format transformation
    val inputFormat = new SimpleDateFormat("dd/MM/yyyy")
    val outputFormat = new SimpleDateFormat("MM")
  
    val format = new SimpleDateFormat("hh:mm:ss")
    
    val n = inputRDD.map(t => processLine(t)).filter(t => t._1 == stationNumber && outputFormat.format(inputFormat.parse(t._5)) <= "11" && outputFormat.format(inputFormat.parse(t._5)) >= "09").map(t => ((t._4, outputFormat.format(inputFormat.parse(t._5))), format.parse(t._8).getTime() - format.parse(t._6).getTime()))
    
    val final_n = n.transform( t => t.aggregateByKey((0, 0))((k, v) => (k._1 + v.toInt, k._2 + 1), (v1, v2) => (v1._1 + v2._1, v1._2 + v2._2)) .mapValues(sum => 1.0 * sum._1 / sum._2).sortBy(_._2))
    
    final_n.cache()
    final_n.print()
    
    final_n.saveAsTextFiles(resultDir)
  } 
  
  def functionToCreateContext(myDataSetDir:String, option:Int, monitoringDir:String, resultDir:String, maxMicroBatches:Int, timeStepInterval:Int): StreamingContext = {
    
    // Create the new Spark Streaming context.
    val ssc = new StreamingContext(sc, Seconds(timeStepInterval))
    
    // Set this to the maximum amount of micro-batches we allow before 
    // considering data
    ssc.remember(Duration(maxMicroBatches * timeStepInterval))
    
    /* To model the ssc.
     Specify what do we want the SparkStreaming context to do once it receives 
     data for instance, the full set of transformations and ouptut operations we        want it to perform.
    */
    
    option match {
      case 1  => ex1(monitoringDir, ssc, resultDir)
      case 2  => ex2(monitoringDir, ssc, resultDir, 240101)
      case 3  => ex3(monitoringDir, ssc, resultDir, 240561)
      case 4  => ex4(monitoringDir, ssc, resultDir, 241111)
      case 5  => ex5(monitoringDir, ssc, resultDir, 240491, Seq[String]("09", "10", "11"))
      case whoa  => println("Unexpected case: " + whoa.toString)
    }
    
    ssc
}
  
  def getSourceDirFileNames(sourceDir: String, verbose: Boolean): Seq[String] = {
    
    var listRes = Seq[String]()
    val fileInfoObjects = dbutils.fs.ls(sourceDir)
    for (item <- fileInfoObjects) {
      var fileName = item.toString.split(",")(1)
      var newName = fileName.slice(1, fileName.length)
      listRes = listRes :+ newName
      
      if (verbose) println(fileName)
    }
    
    return listRes.sorted
  }
  
  def streamingSimulation(sourceDir:String, monitoringDir:String, timeStepInterval:Int, verbose:Boolean): Unit = {
    
    val files = getSourceDirFileNames(sourceDir, verbose)

    val timeStepMil = timeStepInterval * 1000
    
    Thread.sleep(timeStepMil)
    val start = System.currentTimeMillis()

    if (verbose) println("Start time = " + start.toString)
    
    var count = 0

    for (file <- files) {
      
        //dbfs cp sourceDir + file monitoringDir + file
        dbutils.fs.cp(sourceDir + file, monitoringDir + file, true)
        count+=1

        if (verbose) println("File " + count.toString + " transferred. Time since start = " + (System.currentTimeMillis() - start).toString)
        
        val aux = (start + (count * timeStepMil)) - System.currentTimeMillis()
        Thread.sleep((start + (count * timeStepMil)) - System.currentTimeMillis())
      
        //Thread.sleep(timeStepMil * 1000)
    
    }
    
  }

  // ------------------------------------------
  // FUNCTION main
  // ------------------------------------------  
  def myMain(myDataSetDir:String, option:Int, monitoringDir:String, checkpointDir:String, resultDir:String, maxMicroBatches:Int, timeStepInterval:Int, verbose:Boolean): Unit = {
    
    val ssc = StreamingContext.getActiveOrCreate(checkpointDir, () => functionToCreateContext(myDataSetDir, option, monitoringDir, resultDir, maxMicroBatches, timeStepInterval))
    
    ssc.start()
    ssc.awaitTerminationOrTimeout(timeStepInterval)
    
    streamingSimulation(myDataSetDir, monitoringDir, timeStepInterval, verbose)

    ssc.stop(stopSparkContext=false)
  }
} 

// ---------------------------------------------------------------
//                      SCALA EXECUTION
// This is the main entry point to the execution of our program.
// It provides a call to the 'main function' defined in our
// Scala program.
// ---------------------------------------------------------------
val myDataSetDir = "/FileStore/tables/streaming/"
val monitoringDir = "/FileStore/tables/2_Spark_Streaming/my_monitoring/"
val checkpointDir = "/FileStore/tables/2_Spark_Streaming/my_checkpoint/"
val resultDir = "/FileStore/tables/2_Spark_Streaming/my_result/"

val datasetMicroBatches = 12
val timeStepInterval = 10
val maxMicroBatches = datasetMicroBatches + 1
val verbose = true

dbutils.fs.rm(monitoringDir, true)
dbutils.fs.rm(resultDir, true)
dbutils.fs.rm(checkpointDir, true)

dbutils.fs.mkdirs(monitoringDir)
dbutils.fs.mkdirs(resultDir)
dbutils.fs.mkdirs(checkpointDir)

val option = 5
new MyProgram().myMain(myDataSetDir, option, monitoringDir, checkpointDir, resultDir, maxMicroBatches, timeStepInterval, verbose)