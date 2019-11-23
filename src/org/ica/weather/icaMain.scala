package  org.ica.weather

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import java.io._

/**
 * @author sabari
 * @written 23 Nov, 2019
 * @description
 * 		Object is driving program of the application.
 */

object icaMain extends fileConfig with Context{
  
  
  def main (args: Array[String]) :Unit ={    
     
    try{   
    
    var barometerPath = ""
    var temperaturePath= ""      
    var targetTable_Barometer = ""      
    var targetTable_Temperature = ""
    
    if(args.length == 4 ){
       barometerPath= args(0)
       temperaturePath = args(1)
       targetTable_Barometer= args(2)
        targetTable_Temperature= args(3)  
    }
    else{
        println ("Incorrect number of arguments")
        System.exit(0)
    }      
   
    var barometerDir = new File(barometerPath)     
    var temperatureDir = new File(temperaturePath)
    
    //Reading Temperature files from the source folder   
    var temperatureFileList=list_files(temperatureDir)
    println(temperatureFileList)
    
    //Drop the existing Temperature table if any
    drop_table_if_exists(targetTable_Temperature)
    
    //Loading the files to Temperature table
    for (temperature_src_file_name <-temperatureFileList) {
      temperature_loading (temperaturePath,temperature_src_file_name,targetTable_Temperature)      
    }   
    
    //Reading Barometer files from the source folder
    
    var barometerFileList=list_files(barometerDir)
    println(barometerFileList)
    
    //Drop the existing Barometer table if any
    drop_table_if_exists(targetTable_Barometer)
    
    //Loading the files to Barometer table
    for (bar_src_file_name <-barometerFileList) {
       barometer_loading (barometerPath,bar_src_file_name,targetTable_Barometer)
    }
    
    // Prints the table count
    print_table_count()
    
    // Close the Spark Session
    close_context()
     
    }
    catch{      
      case x:RuntimeException => {
        println("Run Time Exception : " + x)
      }
      case x:Exception => {
        println("Exception : " + x)
      }
    }
  }
}