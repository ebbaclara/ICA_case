name:= "Weather"
version:="1.0"
scalaVersion:="2.12.15"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.4.8",
    "org.apache.spark" %% "spark-sql" % "2.4.8" 
)
