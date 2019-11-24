package  org.ica.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.io._

/**
 * @author sabari
 * @written 23 Nov, 2019
 * @description
 * 		trait to hold config file of the application.
 */

trait WeatherConfig {

  /** Initialize the configuration
  *
  * Application name and other properties will set via spark submit
  * 
  */
  lazy val sparkConf = new SparkConf()
    .setAppName("Weather_Monitoring")
    .setMaster("local[*]")
    .set("spark.cores.max", "2")
 
  //Initialize the Spark Session
  lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()
    
    
   /** Closing the connection.
  *
  * Open session will be closed.
  * 
  */
  def closeContext():Unit ={
    sparkSession.close()
  }
  
   
   /** Listing of the files
  *
  *  Read the files from the source folder
  * 
  */
  
  val listFiles = (dir: File) =>  dir.listFiles.map(_.getName).toList
      
  /** Drop the Existing table.
  *
  * Existing will be dropped it will recreate as part of the loading
  * 
  */
  
  def dropTableIfExists (tableName : String ) : Unit = {
    sparkSession.sql(s""" DROP TABLE IF EXISTS ${tableName} """.stripMargin)    
  }
  
    def printTableCount():Unit ={    
    val tableDetails = sparkSession.sql("show tables").collect()
    var sqlBuilder = ""
    var i = 1
   
    tableDetails.foreach { row =>
      {
        sqlBuilder = sqlBuilder + (s"""select '""") + row.getString(1) + (s"""' TABLE_NAME,count(*) COUNT from """) + row.getString(1)
        if (i < tableDetails.size) {
          sqlBuilder = sqlBuilder + "\nunion all \n"
        }
        i = i + 1
      }
    }
    sparkSession.sql(sqlBuilder).show  
  }
    
}