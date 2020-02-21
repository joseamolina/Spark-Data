import java.text.SimpleDateFormat
import java.util.Date
import java.util.Arrays;

class MyProgram extends Serializable {
  
    val N_EXP: Int = 0
    val DATE: Int = 1
    val TIME: Int = 2
    val STREET: Int = 3
    val NUMBER: Int = 4
    val DISTRICT: Int = 5
    val AC_TYPE: Int = 6
    val WEATHER: Int = 7
    val WEHICLE: Int = 8
    val TYPE_PERSON: Int = 9
    val AGE_RANGE: Int = 10
    val SEX: Int = 11
    val LESIVITY: Int = 12
  
    // Data cleaning
    def processLine(line: String): Tuple2[String, Array[String]]= {
          val ese: String = line.replace("\n", "")
          val params:Array[String] = ese.split(";")
          val final_tuple = (params(N_EXP), params)
          return final_tuple
      }
  
    def data_analysis(myDataSetFile:String): Unit = {
      
      // Load data
      val inputRDD = sc.textFile(myDataSetFile)
      
      val junk = inputRDD.first()
      val accidents = inputRDD.filter(x => x != junk).map(t => processLine(t))
      
      // Print streets in Madrid affected
      val streets = accidents.map(s => s._2(STREET)).distinct().collect()
      for (name <- streets) println(name)
      
      // Climathe conditions
      val res_weather = accidents.map(l => (l._2(WEATHER), l._1 )).countByKey()
      res_weather.toSeq.sortWith(_._2 > _._2).foreach(println)
      
      // Month with more accidents
      // Date format transformation
      val inputFormat = new SimpleDateFormat("dd/MM/yyyy")
      val outputFormat = new SimpleDateFormat("MM")
      val new_ad = accidents.map(l => (outputFormat.format(inputFormat.parse(l._2(DATE))), l._1 )).countByKey()
      new_ad.toSeq.sortWith(_._2 > _._2).foreach(println)
      
      // Age ranges with more accidents
      val age = accidents.map(l => (l._2(AGE_RANGE), l._1 )).countByKey()
      age.toSeq.sortWith(_._2 > _._2).foreach(println)
      
      // District with more accidents
      val district = accidents.map(l => (l._2(DISTRICT), l._1 )).countByKey()
      district.toSeq.sortWith(_._2 > _._2).foreach(println)
      
      // Streets with more accidents
      val street = accidents.map(l => (l._2(STREET), l._1 )).countByKey()
      street.toSeq.sortWith(_._2 > _._2).foreach(println)
      
    }
  
    def myMain(myDataSetDir:String): Unit = {
        data_analysis(myDataSetDir)
    }
}

val myDataSetDir = "/FileStore/tables/accidents.csv"
new MyProgram().myMain(myDataSetDir)
