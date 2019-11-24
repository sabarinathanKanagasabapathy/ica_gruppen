name := "weather_monitoring"

version := "0.1"

scalaVersion := "2.13.1"

libraryDependencies ++= Seq(
  "org.apache.spark"  %%  "spark-core"    % "2.2.0",
  "org.apache.spark"  %%  "spark-sql"     % "2.2.0",
  "org.apache.spark"  %%  "spark-mllib"   % "2.2.0"
)