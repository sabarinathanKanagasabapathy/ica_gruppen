package org.ica.test

import org.junit.Assert._
import org.junit.Test
import junit.framework.TestCase
import org.ica.utils._

/**
 * @author sabari
 * @written 23 Nov, 2019
 * @description
 * 		Assertion Test to ensure the functionality is working as expected
 */

class WeatherAssertionTest {
  
  var baromeberObject= new BarometerTableLoad{}
  var temperatureObject = new TemperatureTableLoad{}  
  var filePath = ""
  var fileName = ""
  var fileType = "" 
  
    
   /*
   * Initialize the test case argument
   *  
   * 
   */
  
  def initValue (args : Array[String]) : Unit = {
    filePath = args(0)
    fileName = args(1)
    fileType = args(2)
  }
  
    /*
   * Fetch Unit test case result  
   * eg: If we pass correct argument then it will return succes, if we pass empty path then it will throw error  
   * 
   */

  @Test def verifyRun {    
    if (fileType == "Barometer") {       
       assertNull(baromeberObject.dataIngestionBarometer(filePath, fileName,  "ica_barometer_output"))
    }
    else{
       assertNull(temperatureObject.dataIngestionTemperature(filePath, fileName,  "ica_temperature_output"))  
    }  
        
  }
  
 object WeatherTest {
    
  def main(args: Array[String]): Unit = {
    
     if(args.length != 3 ){  
        println ("Incorrect number of arguments")
        System.exit(0)
    }
     
    /*
   * Invoke the Unit test case
   * 
   */ 
    val weatherObj = new WeatherAssertionTest()
    weatherObj.initValue(args)    
   }
  }  
}