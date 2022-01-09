ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.7"

lazy val root = (project in file("."))
  .settings(
    name := "wiki"
  )

val circeVersion = "0.14.1"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

libraryDependencies += "org.json4s" %% "json4s-jackson" % "4.0.2"
libraryDependencies ++= Seq(
  "com.typesafe.slick" %% "slick" % "3.3.3",
  "com.typesafe.slick" %% "slick-hikaricp" % "3.3.3",
  "org.slf4j" % "slf4j-nop" % "1.7.32",
  "org.postgresql" % "postgresql" % "9.4-1204-jdbc42"
)

libraryDependencies += "commons-cli" % "commons-cli" % "1.5.0"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4"
