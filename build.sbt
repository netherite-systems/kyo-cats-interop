ThisBuild / scalaVersion := "3.9.0"
ThisBuild / organization := "com.netheritesystems"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / versionScheme := Some("early-semver")

lazy val root = project
    .in(file("."))
    .enablePlugins(SbtKyoTestPlugin)
    .settings(
        name := "kyo-actors",
        libraryDependencies += "io.getkyo" %% "kyo-actor" % "1.0.0-RC7",
        scalacOptions ++= Seq(
            "-Wvalue-discard",
            "-Wnonunit-statement",
            "-Wconf:msg=(unused.*value|discarded.*value|pure.*statement):error",
            "-language:strictEquality",
            "-release:25"
        )
    )
