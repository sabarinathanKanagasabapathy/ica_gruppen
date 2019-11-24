package  org.ica.invoker

import java.io._
import org.ica.utils._

/**
 * @author sabari
 * @written 23 Nov, 2019
 * @description
 * 		Object is driving program of the application.
 */

object PredictWeatherMain extends WeatherConfig with TemperatureTableLoad with BarometerTableLoad {
    
  def main (args: Array[String]) :Unit ={  
    try{   
    
    if(args.length != 4 ){  
        println ("Incorrect number of arguments")
        System.exit(0)
    }    
    val barometerPath = args(0)
    val temperaturePath= args(1)
    val targetBarometerTable = args(2)
    val targetTemperatureTable = args(3) 
   
    val barometerDir = new File(barometerPath)     
    val temperatureDir = new File(temperaturePath)
    
    //Reading Temperature files from the source folder   
    var temperatureFileList=listFiles(temperatureDir)
    println(temperatureFileList)
    
    //Drop the existing Temperature table if any
    dropTableIfExists(targetTemperatureTable)
    
    //Loading the files to Temperature table
    for (temperature_src_file_name <-temperatureFileList) {
      dataIngestionTemperature (temperaturePath,temperature_src_file_name,targetTemperatureTable)      
    }   
    
    //Reading Barometer files from the source folder
    
    var barometerFileList=listFiles(barometerDir)
    println(barometerFileList)
    
    //Drop the existing Barometer table if any
    dropTableIfExists(targetBarometerTable)
    
    //Loading the files to Barometer table
    for (barSrcFileName <-barometerFileList) {
       dataIngestionBarometer (barometerPath,barSrcFileName,targetBarometerTable)
    }
    
    // Prints the table count
    printTableCount()
    
    // Close the Spark Session
    closeContext()
     
    }
    catch{           
      case x:Exception => {
        println("Exception : " + x)
      }
    }
  }
}