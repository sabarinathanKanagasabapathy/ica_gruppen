package org.ica.utils

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.spark.sql.types.{StructType, StructField, StringType}


/**
 * @author sabari
 * @written 23 Nov, 2019
 * @description
 * 		trait to preprocess the barometer and temperature data and loading to target table of the application.
 */

trait TemperatureTableLoad extends WeatherConfig{
  
   /*
   * Function to download weather data for the given input path and align the template into common format 
   * eg: 2013 Dec 2018 temperature measurements on min, max, mean degree  
   * 
   */
   
  def dataIngestionTemperature (path : String, fileName : String, targetTable : String ) : Unit = {
    
     var filePath=path +"\\"+fileName
        var tempViewName = fileName.substring(0, fileName.lastIndexOf("."))
        
       val header =  tempViewName match {
          case x if x.contains("daily_temp_obs") => 
		         "year,month,day,morning_deg_c,noon_deg_c,evening_deg_c,tmax_deg_c,tmin_deg_c,est_diurnal_mean_deg_c"
          case _ => ""
       }
     
       val partitionKey =  tempViewName match {
          case x if x.contains("stockholmA_") => 
		         "-A"
          case _ => ""
       }      
        
       val fields = header.split(",").map(fieldName => StructField(fieldName, StringType, nullable=true))
       val schema = StructType(fields)
       val dataFrame =  sparkSession.read       
       .option("header","false")
       .option("delimiter"," ")
       .schema(schema)
       .csv(filePath)             
       dataFrame.createOrReplaceTempView(tempViewName)
       println ( " Table Name : " + tempViewName + "  Count : "+ dataFrame.count())
            
       val targetDF = sparkSession.sql(s""" select 
             concat(Year,'${partitionKey}') as Year,
             concat (Year,'-',month,'-', day) as date,		
             morning_deg_c,
             noon_deg_c,
             evening_deg_c,
             tmax_deg_c,
             tmin_deg_c,
             est_diurnal_mean_deg_c
         from ${tempViewName}
           """ .stripMargin)   
       targetDF.write.mode("append").partitionBy("Year").saveAsTable(targetTable)
       println ( " Table Name : " + targetTable + "  Count : "+ targetDF.count())
  }  
  
}