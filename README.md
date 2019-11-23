# ica_gruppen

Assumption :

1) Input file will have space delimitted file
2) Currently code configured to run locally need to change it to yarn
3) Currently input and output path is hard coded and required to parameterize by passing argument

Compile :

Assuming you already have Eclipse Scala IDE with spark Jar installed:

$ git https://github.com/sabarinathanKanagasabapathy/ica_gruppen.git
$ Create the project with ICA_GRUPPEN in the the eclipse 
$ Import the package into project
$ Build the jar

target/ica_gruppen.jar

Unit Test :

Recon counts will print at end of the job run to ensure the data is loaded in all stages


Submit job to Spark on YARN :

spark-submit --conf spark.port.maxRetries=50 --driver-memory 3G --executor-memory 14G --num-executors 15 --master yarn --class org.ica.gruppen.icaMain ica_gruppen.jar input_path output_path
