import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "io.github.ximera239.nrt",
      scalaVersion := "2.12.1",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "Scala-Lucene-NRT",
    libraryDependencies ++= Seq(
      "org.apache.lucene" % "lucene-core" % "6.1.0",
      "org.apache.lucene" % "lucene-analyzers-common" % "6.1.0",
      scalaTest % Test
    )
  )
