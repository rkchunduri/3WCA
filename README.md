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

The algorithm implementaiton can be found in 3WayData.Scala file. If the build.sbt is able to run successfully, then no configurations are required. If it sees any errors, then something is not correct with the environment.
For testing purpose, we attached covid 19 dataset with 20P and also the dataset from WHO in the repository. The data is loaded in HDFS while executing in cloud environment.

Output is writtetn to AEConcepts and OEConcepts folder structure in the repository.



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

1. Class Not found error : This run time exception can be usually seen when the project is not configured properly in IDE that users are using. Just clean the project using clean menu item in apporpriate IDE, and this error should get rid off.
2. If still seeing the exception after clean, there are multiple dependencies installed or some dependencies are not properly configured. To fix this clear the complete sbt folder and run the command
sbt install
More information can be found at https://www.scala-sbt.org/1.x/docs/Setup.html
3. Major Minor version error. If users are seeing these errors, that indicates the JDK is not compatiable with scala and spark versions that users are using
4. Spark, socket time out issues while running in cloud environment to fix this :
if you have already configured Spark with HDFS/Yarn, by setting the configuration files' locations in spark-env.sh, providing the location on HDFS without the protocol (hdfs), so the code will be :
temp.write.format("orc").option("header", "true").save("/app/Quality/spark_test/")

Add this line in your code
5. 


##Feedback##

You are welcome to give any feedback and contributes. We're glad to hear from you about any bug.
You can contact by rkchunduri@gmail.com


