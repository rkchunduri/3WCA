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