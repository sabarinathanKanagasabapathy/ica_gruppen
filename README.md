**Weather Monitoring - Sabari Nathan**

The project is trying to load the data from weather forecast station to spark tables and make the data availablity for data scientist for further analytics.

Raw data will be taken from Stockholm Historical Weather observation. The data is in the form of below
1.Barometer readings in original units 
2.Raw individual temperature observations

Both Barometer readings and temperature observation is having automated and manual observations. The spark will preporcess the different layout of data and load into common table for data scientist.

**Target Tabe :**
1.ica_barometer_output
2.ica_temperature_output

After analysing the data, decided to choose following platform.

1 Apache Spark 2.2.0
2 Scala 2.13.21
3 java version "1.8.0_23"
5 Apache Hadoop: Hadoop 2.5.0-cdh5.3.0
6 SBT: 1.3.4
7.Hadoop Distribution: Cloudera/Horton Works

**Project Artifacts:**

1 WeatherConfig.scala to intialize the spark session and it is having common function for this project
2 BarometerTableLoad.scala for:
  - Loading barometer data into Spark from local directory.
  - Align the different format of file into common temporary view  
  - Load the temporary view into final table 
3 TemperatureTableLoad.scala for:
  - Loading temperature data into Spark from local directory.
  - Align the different format of file into common temporary view  
  - Load the temporary view into final table  
4 PredictWeatherMain.scala is the Main class which will invoke above trait and process the input files
  - it takes input path and output path as an argument
  - it takes target output tables as an argument
  - Print the counts to ensure the data is loaded successfuly without any data drop

**Assumption :**

1) Input file will have space delimitted file
2) Currently code configured to run locally need to change it to yarn

**Compile :**

Assuming you already have **Eclipse Scala IDE with sbt **  or **IntellJ with SBT installed** . Spark jars need to add in lib folder/class path.

1. Create the project with WEATHER_MONITORING in the the eclipse 
2. git clone https://github.com/sabarinathanKanagasabapathy/weather_monitoring.git to local project folder
3. Import the package into project
4. Place the build.sbt file for intell j
4. Build the jar - target/weather_monitoring.jar

**Unit Test :**

Recon counts will print at end of the job run to ensure the data is loaded in all stages

**Unit Test case :**

1. Valid details Temperature
   - scala -cp scalatest-3.0.8.jar:junit-4.4.jar org.ica.test WeatherTest  <Valid input path> <Valid File name> "Temperature"
2. Valid details Barometer
   - scala -cp scalatest-3.0.8.jar:junit-4.4.jar org.ica.test WeatherTest  <Valid input path> <Valid File name> "Barometer"
3. Invalid details Temperature
   - scala -cp scalatest-3.0.8.jar:junit-4.4.jar org.ica.test WeatherTest  <Invalid input path> <Valid File name> "Temperature"
   - scala -cp scalatest-3.0.8.jar:junit-4.4.jar org.ica.test WeatherTest  <Valid input path> <Invalid File name> "Temperature"
4. Invalid details Barometer
   - scala -cp scalatest-3.0.8.jar:junit-4.4.jar org.ica.test WeatherTest  <Invalid input path> <Valid File name> "Barometer"
   - scala -cp scalatest-3.0.8.jar:junit-4.4.jar org.ica.test WeatherTest  <Valid input path> <Invalid File name> "Barometer"

**Recon Test case :**

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
