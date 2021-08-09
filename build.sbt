import Dependencies._

ThisBuild / scalaVersion     := "2.12.13"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "org.kurochan"
ThisBuild / organizationName := "snowpark-test"

lazy val root = (project in file("."))
  .settings(
    name := "snowpark-test",
    resolvers += "OSGeo Release Repository" at "https://repo.osgeo.org/repository/release/",
    libraryDependencies ++= Seq(
      snowpark,
      scalaTest % Test
    )
  )
