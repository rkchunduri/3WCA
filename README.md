# 3WCA
#Distributed 3WCA algorithm for OE and AE conceptgeneration based on Spark#

##Intoduction##

The proposed algorithm is a distributed Three way Formal Concept Analysis algorithm implemented using Apache Spark. The algorithm identifies the OE and AE concepts in parallel by processing
input formal context and its complement simultaneously in two different threads. The algorithm process each concept once and avoids the processing of duplicate concepts.

##Sample Data##

To be used by Spark Concept generation algorithm we prepare a single file data in a horizontal way and specify their number of objects/records and the number of attributes.
Below is a simple example.
##Example Input Formal Context, the first column represemts set of  object and the first row represents set of attributes.
![image](https://user-images.githubusercontent.com/5356816/123883787-4e84e080-d90f-11eb-8616-abd6a2c7596c.png)


For testing purpose, we attached covid 19 dataset with 20P and also the dataset from WHO in the repository. The data is loaded in HDFS while executing in cloud environment.

##Requirments##

This algorithm implementated using Scala and Apache Spark. To execute in local Scala 2.12 with SBT is needed and the dependencies are need to define in .sbt file. 
Clusters with apache spark are needed to exuecte in cloud. DataProc Google Cloud is the cluster we used to run our experiments.

Preview of build.sbt
name := "threeway-concept-analysis"

version := "1.0"

scalaVersion := "2.11.8"
fork := true

scalacOptions := Seq("-unchecked", "-deprecation")
resolvers += ("releases" at "https://oss.sonatype.org/service/local/staging/deploy/maven2")

resolvers += ("snapshots" at "https://oss.sonatype.org/content/repositories/snapshots")
lazy val sparkVersion = "2.3.0"

libraryDependencies ++= Seq(
	
	 "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"
)

##More Resource##


##Feedback##

You are welcome to give any feedback and contributes. We're glad to hear from you about any bug.
You can contact by rkchunduri@gmail.com


