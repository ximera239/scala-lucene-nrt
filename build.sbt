import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "io.github.ximera239.nrt",
      scalaVersion := "2.12.1",
      version      := "0.2.0-SNAPSHOT"
    )),
    name := "Scala-Lucene-NRT",
    libraryDependencies ++= Seq(
      "org.apache.lucene" % "lucene-core" % "7.2.1",
      "org.apache.lucene" % "lucene-analyzers-common" % "7.2.1",
      scalaTest % Test
    )
  )
