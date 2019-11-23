package org.ica.weather

import java.io._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.SparkSession


/**
 * @author sabari
 * @written 23 Nov, 2019
 * @description
 * 		trait to preprocess the barometer and temperature data and loading to target table of the application.
 */

trait fileConfig extends Context{
  
 
   /** Listing of the files
  *
  *  Read the files from the source folder
  * 
  */
  
  def list_files(dir: File) : List[String] ={
    
    val dir_list = dir.listFiles.map(_.getName).toList
   
    return dir_list
  }
  
  
  /** Drop the Existing table.
  *
  * Existing will be dropped it will recreate as part of the loading
  * 
  */
  
  def drop_table_if_exists (tableName : String ) : Unit = {
    sparkSession.sql(s""" DROP TABLE IF EXISTS ${tableName} """.stripMargin)    
  }
  
  
   /*
   * Function to download weather data for the given input path and align the template into common format 
   * eg: 2013 Dec 2018 barometer measurements on air pressure  
   * 
   */
  
  def barometer_loading (path : String, fileName : String, targetTable : String ) : Unit = {
    
     var file_name=path +"\\"+fileName
        var tempViewName = fileName.substring(0, fileName.lastIndexOf("."))
        
        val header =  tempViewName match {
          case x if x.contains("1756") && x.contains("1858") => 
                     "year,month,day,morning_swc_inc,morning_deg_c,noon_swc_inc,noon_deg_c,evening_swc_inc,evening_deg_c,morning_air_pressure,noon_air_pressure,evening_air_pressure,morning_0deg_C,noon_0deg_c,evening_0deg_c"
          case x if x.contains("1859") && x.contains("1861") => 
                     "year,month,day,morning_swc_inc,morning_deg_c,morning_0deg_c,noon_swc_inc,noon_deg_c,noon_0deg_c,evening_swc_inc,evening_deg_c,evening_0deg_C,morning_air_pressure,noon_air_pressure,evening_air_pressure"
          case x if x.contains("1862") && x.contains("1937") => 
                     "year,month,day,morning_air_pressure,noon_air_pressure,evening_air_pressure,morning_swc_inc,morning_deg_c,morning_0deg_c,noon_swc_inc,noon_deg_C,noon_0deg_c,evening_swc_inc,evening_deg_c,evening_0deg_c"
          case x if x.contains("1938") && x.contains("1960") => 
                     "year,month,day,morning_air_pressure,noon_air_pressure,evening_air_pressure,morning_swc_inc,morning_deg_c,morning_0deg_c,noon_swc_inc,noon_deg_C,noon_0deg_c,evening_swc_inc,evening_deg_c,evening_0deg_c"
          case x if x.contains("1961") && x.contains("2012") => 
                     "year,month,day,morning_air_pressure,noon_air_pressure,evening_air_pressure,morning_swc_inc,morning_deg_c,morning_0deg_c,noon_swc_inc,noon_deg_C,noon_0deg_c,evening_swc_inc,evening_deg_c,evening_0deg_c" 
          case x if x.contains("2013") && x.contains("2017") && x.contains("stockholm_")  => 
                     "year,month,day,morning_air_pressure,noon_air_pressure,evening_air_pressure,morning_swc_inc,morning_deg_c,morning_0deg_c,noon_swc_inc,noon_deg_C,noon_0deg_c,evening_swc_inc,evening_deg_c,evening_0deg_c"
          case x if x.contains("2013") && x.contains("2017") && x.contains("stockholmA_")  =>  
                    "year,month,day,morning_air_pressure,noon_air_pressure,evening_air_pressure,morning_swc_inc,morning_deg_c,morning_0deg_c,noon_swc_inc,noon_deg_C,noon_0deg_c,evening_swc_inc,evening_deg_c,evening_0deg_c"
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
       .option("inferSchema", "true")
       .schema(schema)
       .csv(file_name)             
       dataFrame.createOrReplaceTempView(tempViewName)
     
       
       println ( " Table Name : " + tempViewName + "  Count : "+ dataFrame.count())                
       val targetDF = sparkSession.sql(s""" select 
               concat(Year,'${partitionKey}') as Year,
               concat (Year,'-',month,'-', day) as date,		
               morning_air_pressure,		
               noon_air_pressure,		
               evening_air_pressure,		
               morning_swc_inc,		
               morning_deg_C,	
               morning_0deg_C,		
               noon_swc_inc,		
               noon_deg_C,		
               noon_0deg_C,		
               evening_swc_inc,		
               evening_deg_C,		
               evening_0deg_C  
           from ${tempViewName}
           """ .stripMargin)   
       targetDF.write.mode("append").partitionBy("Year").saveAsTable(targetTable)
       println ( " Table Name : " + targetTable + "  Count : "+ targetDF.count())
  }
  
   /*
   * Function to download weather data for the given input path and align the template into common format 
   * eg: 2013 Dec 2018 temperature measurements on min, max, mean degree  
   * 
   */
   
  def temperature_loading (path : String, fileName : String, targetTable : String ) : Unit = {
    
     var file_name=path +"\\"+fileName
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
       .option("inferSchema", "true")
       .schema(schema)
       .csv(file_name)             
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
  
  /** Recon of data.
  *
  * This is verify the target and source table count details.
  * 
  */
  
  def print_table_count():Unit ={    
    val tableDf = sparkSession.sql("show tables").collect()
    var sqlBuilder = ""
    var i = 1
   
    tableDf.foreach { row =>
      {
        sqlBuilder = sqlBuilder + (s"""select '""") + row.getString(1) + (s"""' TABLE_NAME,count(*) COUNT from """) + row.getString(1)
        if (i < tableDf.size) {
          sqlBuilder = sqlBuilder + "\nunion all \n"
        }
        i = i + 1
      }
    }
    sparkSession.sql(sqlBuilder).show  
  }
  
}