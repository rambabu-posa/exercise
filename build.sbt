name := "com.jpm.test.jpmo"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies++= {
  val sparkVersion="2.2.0"
  Seq(
    "org.apache.spark" %% "spark-core" % "2.4.0",
    "org.apache.spark" %% "spark-sql" % "2.4.0",
    "com.typesafe" % "config" % "1.3.3",
    "org.foo" % "bar_2.11" % "1.2.3" % "test"
  )
}