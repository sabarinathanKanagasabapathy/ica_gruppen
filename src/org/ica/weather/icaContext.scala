package  org.ica.weather
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession


trait Context {

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
  def close_context():Unit ={
    sparkSession.close()
  }
}