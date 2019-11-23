**Weather Monitoring - Sabari Nathan**

The project is trying to do in "big data processing" fashion for the stockholm temperature/barometer data

Procured input data from stockholm case studies as a raw text file. 

After analysing the data, decided to choose following platform.

3.1 Apache Spark 1.4.1, Spark SQL 1.4.1

3.2 Scala 2.10.4

3.3 java version "1.8.0_77"

3.5 Apache Hadoop: Hadoop 2.5.0-cdh5.3.0

3.6 SBT: 0.13.5

3.6 Hadoop Distribution: Cloudera/Horton Works

**Project Artifacts:**

4.1 icaContext.scala to intialize the spark session

4.2 icaConfig.scala for:

  - Loading barometer/temperature data into Spark from local directory.
  - Align the different format of file into common temporary view  
  - Load the temporary view into final table
  - Print the counts to ensure the data is loaded successfuly without any data drop

4.3 icaMain.scala is the Main class which will invoke above trait and process the input files
  - it takes input path and output path as an argument
  - it takes target output tables as an argument

**Assumption :**

1) Input file will have space delimitted file
2) Currently code configured to run locally need to change it to yarn

**Compile :**

Assuming you already have **Eclipse Scala IDE with sbt, spark Jar installed**:

1. Create the project with WEATHER_MONITORING in the the eclipse 
2. git clone https://github.com/sabarinathanKanagasabapathy/weather_monitoring.git to local project folder
3. Import the package into project
4. Build the jar - target/weather_monitoring.jar

**Unit Test :**

Recon counts will print at end of the job run to ensure the data is loaded in all stages

**Test case :**

1.Query :
  - select count(*) from ica_barometer_output
  - select count (*) from ica_temperature_output
-File count : Number of count in the file
-Expected result : Count should match
-Actual result  :  Count matched

2.Individual File count
 - Barometer   : 97520
 - Temperature : 97520

3.Final Table count :
 - ica_barometer_output   - 97520
 - ica_temperature_output - 97520

**Submit job to Spark on YARN :**

spark-submit --conf spark.port.maxRetries=50 --driver-memory 3G --executor-memory 14G --num-executors 15 --master yarn --class org.ica.weather.icaMain weather_monitoring.jar <input_path> <output_path> ica_barometer_output ica_temperature_output
