name := "com.jpm.test.jpmo"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies++= {
  val sparkVersion="2.2.0"
  Seq(
    "org.apache.spark" %% "spark-core" % "2.4.0",
    "org.apache.spark" %% "spark-sql" % "2.4.0",
    "com.typesafe" % "config" % "1.3.3",
    "org.scalatest" %% "scalatest" % "3.0.5" % Test
  )
}
fork in Test := true