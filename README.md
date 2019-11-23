Weather Monitoring - Sabari Nathan

The project is trying to do in "big data processing" fashion for the stockholm temperature/barometer data

Procured input data from stockholm case studies as a raw text file. 

After analysing the data, decided to choose following platform.

3.1 Apache Spark 1.4.1, Spark SQL 1.4.1

3.2 Scala 2.10.4

3.3 java version "1.8.0_77"

3.5 Apache Hadoop: Hadoop 2.5.0-cdh5.3.0

3.6 SBT: 0.13.5

3.6 Hadoop Distribution: Cloudera/Horton Works

Project Artifacts:

4.1 icaContext.scala to intialize the spark session

4.2 icaConfig.scala for:

  4.2.1 Loading barometer/temperature data into Spark from local directory.

  4.2.2 Align the different format of file into common temporary view
  
  4.2.3 Load the temporary view into final table

  4.2.4 Print the counts to ensure the data is loaded successfuly without any data drop

4.3 icaMain.scala is the Main class which will invoke above trait and process the input files
    4.3.1 it takes input path and output path as an argument
    4.3.2 it takes target output tables as an argument

Assumption :

1) Input file will have space delimitted file
2) Currently code configured to run locally need to change it to yarn

Compile :

Assuming you already have Eclipse Scala IDE with sbt, spark Jar installed:

$ git https://github.com/sabarinathanKanagasabapathy/weather_monitoring.git
$ Create the project with WEATHER_MONITORING in the the eclipse 
$ Import the package into project
$ Build the jar

target/weather_monitoring.jar

Unit Test :

Recon counts will print at end of the job run to ensure the data is loaded in all stages

Test case 1 : select count(*) from ica_barometer_output
              select count (*) from ica_temperature_output
              Vs
              Number of count in the file
Expected result : Count should match
Actual result  :  Count matched

Individual File count :

Barometer :

|stockholm_baromet...|37620|
|stockholm_baromet...| 1096|
|stockholm_baromet...|27758|
|stockholm_baromet...| 8401|
|stockholm_baromet...|18993|
|stockholm_baromet...| 1826|
|stockholma_barome...| 1826|

Temperatre :

|stockholm_daily_t...|37620|
|stockholm_daily_t...|37255|
|stockholm_daily_t...|18993|
|stockholm_daily_t...| 1826|
|stockholma_daily_...| 1826|

Final Table count :
--------------------+-----+
|          TABLE_NAME|COUNT|
+--------------------+-----+
|ica_barometer_output|97520|
|ica_temperature_o...|97520|
+--------------------+-----+

Submit job to Spark on YARN :

spark-submit --conf spark.port.maxRetries=50 --driver-memory 3G --executor-memory 14G --num-executors 15 --master yarn --class org.ica.weather.icaMain weather_monitoring.jar <input_path> <output_path> ica_barometer_output ica_temperature_output
