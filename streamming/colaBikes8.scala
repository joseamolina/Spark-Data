import org.apache.spark.streaming._
import org.apache.spark.sql.streaming._
import Math._
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.StructType

class MyProgram extends Serializable {
  
  def ex1(monitoringDir:String, checkpointDir:String, resultDir:String, timeStepInterval:Int): DataStreamWriter[Row] = {
    
    val rowsSchema = new StructType().add("station_number", "string").add("station_name", "string").add("direction", "string").add("day_of_week", "integer").add("date", "string").add("query_time", "string").add("scheduled_time", "string").add("expected_arrival_time", "string")
    
    var rows = spark.readStream.option("delimiter", ";").schema(rowsSchema).csv(monitoringDir)
    
    // 4. Operation T1: We add the current timestamp
    val timeInputSDF = rows.withColumn("myTime", current_timestamp()).withWatermark("myTime", "0 seconds")

    // 5. We set the frequency for the trigger
    val myFrequency = timeStepInterval.toString + " seconds"

    // 6. Operation T2: We add the watermark on my_time
    val windowSDF = timeInputSDF.groupBy("myTime").count()
    
    // 7. We create the DataStreamWritter
    val myDSW = windowSDF.writeStream.format("csv").option("delimiter", ";").option("path", resultDir).option("checkpointLocation", checkpointDir).trigger(Trigger.ProcessingTime(myFrequency)).outputMode("append")
    
    return myDSW
  }
  
  def ex2(monitoringDir:String, checkpointDir:String, resultDir:String, timeStepInterval:Int, num:Int): DataStreamWriter[Row] = {
    
    val rowsSchema = new StructType().add("station_number", "string").add("station_name", "string").add("direction", "string").add("day_of_week", "integer").add("date", "string").add("query_time", "string").add("scheduled_time", "string").add("expected_arrival_time", "string")
    
    var rows = spark.readStream.option("delimiter", ";").schema(rowsSchema).csv(monitoringDir)
    
    var timeInputSDF = rows.withColumn("myTime", current_timestamp()).withWatermark("myTime", "0 seconds")

    var myFrequency = timeStepInterval.toString + " seconds"

    timeInputSDF = timeInputSDF.filter($"station_number" === num)

    var windowSDF = timeInputSDF.groupBy("date", "myTime")
    val new_opr = windowSDF.count()
    
    val myDSW = new_opr.writeStream.format("csv").option("delimiter", ";").option("path", resultDir).option("checkpointLocation", checkpointDir).trigger(Trigger.ProcessingTime(myFrequency)).outputMode("append")
    
    return myDSW
  }
  
  def ex3(monitoringDir:String, checkpointDir:String, resultDir:String, timeStepInterval:Int, num:Int): DataStreamWriter[Row] = {
    
    val rowsSchema = new StructType().add("station_number", "string").add("station_name", "string").add("direction", "string").add("day_of_week", "integer").add("date", "string").add("query_time", "string").add("scheduled_time", "string").add("expected_arrival_time", "string")
    
    var rows = spark.readStream.option("delimiter", ";").schema(rowsSchema).csv(monitoringDir)
    
    var timeInputSDF = rows.withColumn("myTime", current_timestamp()).withWatermark("myTime", "0 seconds")

    var myFrequency = timeStepInterval.toString + " seconds"

    timeInputSDF = timeInputSDF.filter($"station_number" === num)
    
    timeInputSDF = timeInputSDF.withColumn("ahead", when($"scheduled_time".lt($"expected_arrival_time"), 1).otherwise(0)).withColumn("behind", when($"scheduled_time".geq($"expected_arrival_time"), 1).otherwise(0))
    
    var windowSDF = timeInputSDF.groupBy("ahead", "behind", "myTime")
    val new_opr = windowSDF.count()
    
    val myDSW = new_opr.writeStream.format("csv").option("delimiter", ";").option("path", resultDir).option("checkpointLocation", checkpointDir).trigger(Trigger.ProcessingTime(myFrequency)).outputMode("append")
    
    return myDSW
  }
  
  def ex4(monitoringDir:String, checkpointDir:String, resultDir:String, timeStepInterval:Int, num:Int): DataStreamWriter[Row] = {
    
    val rowsSchema = new StructType().add("station_number", "string").add("station_name", "string").add("direction", "string").add("day_of_week", "integer").add("date", "string").add("query_time", "string").add("scheduled_time", "string").add("expected_arrival_time", "string")
    
    var rows = spark.readStream.option("delimiter", ";").schema(rowsSchema).csv(monitoringDir)
    
    var timeInputSDF = rows.withColumn("myTime", current_timestamp()).withWatermark("myTime", "0 seconds")
    
    var myFrequency = timeStepInterval.toString + " seconds"
    
    timeInputSDF = timeInputSDF.filter($"station_number" === num)
    
    timeInputSDF = timeInputSDF.withColumn("group_main", struct(timeInputSDF("day_of_week"), timeInputSDF("scheduled_time")))
    
    var windowSDF = timeInputSDF.groupBy("group_main", "myTime").count()
    
    val myDSW = windowSDF.writeStream.format("csv").option("delimiter", ";").option("path", resultDir).option("checkpointLocation", checkpointDir).trigger(Trigger.ProcessingTime(myFrequency)).outputMode("append")
    
    return myDSW
  } 
  
  def ex5(monitoringDir:String, checkpointDir:String, resultDir:String, timeStepInterval:Int, num:Int, sequence:Seq[String]): DataStreamWriter[Row] = {
    
    val rowsSchema = new StructType().add("station_number", "string").add("station_name", "string").add("direction", "string").add("day_of_week", "integer").add("date", "string").add("query_time", "string").add("scheduled_time", "string").add("expected_arrival_time", "string")
    
    var rows = spark.readStream.option("delimiter", ";").schema(rowsSchema).csv(monitoringDir)
    
    // 4. Operation T1: We add the current timestamp
    var timeInputSDF = rows.withColumn("myTime", current_timestamp()).withWatermark("myTime", "0 seconds")

    // 5. We set the frequency for the trigger
    var myFrequency = timeStepInterval.toString + " seconds"

    // 6. Operation T2: We filter the items that belong to station_number.
    timeInputSDF = timeInputSDF.filter($"station_number" === num && $"date" <= "11" && $"date" >= "09")

    // 9. Operation T5: We put together the day of the week and the month
    val together = timeInputSDF.withColumn("week_month", concat($"day_of_week", lit(" "), $"date"))

    // 10. Operation T6: We compute the waiting time
    val grouped_together = together.groupBy("week_month")
    
    val myDSW = grouped_together.writeStream.format("csv").option("delimiter", ";").option("path", resultDir).option("checkpointLocation", checkpointDir).trigger(Trigger.ProcessingTime(myFrequency)).outputMode("append")
    
    return myDSW
    
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
        
        Thread.sleep((start + (count * timeStepMil)) - System.currentTimeMillis())
    
    }
    Thread.sleep(timeStepMil)
    
  }

  // ------------------------------------------
  // FUNCTION main
  // ------------------------------------------  
  def myMain(myDataSetDir:String, option:Int, monitoringDir:String, checkpointDir:String, resultDir:String, timeStepInterval:Int, verbose:Boolean): Unit = {
    
    var dsw: DataStreamWriter[Row] = null
    if (option == 1) {
      dsw = ex1(monitoringDir, checkpointDir, resultDir, timeStepInterval)
    }else{
      if (option == 2){
        dsw = ex2(monitoringDir, checkpointDir, resultDir, timeStepInterval, 240101)
      }else{
        if (option == 3){
         dsw = ex3(monitoringDir, checkpointDir, resultDir, timeStepInterval, 240561)
        }else{
          if (option == 4){
          dsw = ex4(monitoringDir, checkpointDir, resultDir, timeStepInterval, 241111)
          }else{
           dsw = ex5(monitoringDir, checkpointDir, resultDir, timeStepInterval, 240491, Seq[String]("09", "10", "11"))
          }
        }
      }
    }
    println(dsw)
    
    val ssq = dsw.start()
    ssq.awaitTermination(timeStepInterval)
    streamingSimulation(myDataSetDir, monitoringDir, timeStepInterval, verbose)
    
    ssq.stop()
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

val timeStepInterval = 20
val verbose = true

dbutils.fs.rm(monitoringDir, true)
dbutils.fs.rm(resultDir, true)
dbutils.fs.rm(checkpointDir, true)

dbutils.fs.mkdirs(monitoringDir)
dbutils.fs.mkdirs(resultDir)
dbutils.fs.mkdirs(checkpointDir)

val option = 1
new MyProgram().myMain(myDataSetDir, option, monitoringDir, checkpointDir, resultDir, timeStepInterval, verbose)