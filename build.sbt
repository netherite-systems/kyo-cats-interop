val scala3Version    = "3.9.0"
val scala213Version  = "2.13.18"
val kyoVersion       = "1.0.0-RC7"
val catsEffectVersion = "3.7.1"
val fs2Version       = "3.14.0"
val scalaTestVersion = "3.2.20"

ThisBuild / scalaVersion       := scala3Version
ThisBuild / crossScalaVersions := Seq(scala3Version)
ThisBuild / organization       := "io.github.netheritesystems"
ThisBuild / version            := "0.1.0-SNAPSHOT"
ThisBuild / versionScheme      := Some("early-semver")

inThisBuild(List(
    homepage := Some(url("https://github.com/NetheriteSystems/kyo-cats-interop")),
    licenses := List("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0"))
))

// Timing-sensitive conformance checks should not share a machine with another suite.
Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)

lazy val root = project
    .in(file("."))
    .aggregate(
        kyoCats.jvm,
        kyoCats.js,
        kyoSchedulerCats.jvm,
        kyoCompatCe.jvm,
        kyoCompatCe.js,
        kyoCompatCe.native
    )
    .settings(
        name           := "kyo-cats-interop",
        publish / skip := true
    )

lazy val kyoCats = crossProject(JVMPlatform, JSPlatform)
    .crossType(CrossType.Full)
    .in(file("kyo-cats"))
    .settings(
        name               := "kyo-cats",
        crossScalaVersions := Seq(scala3Version),
        libraryDependencies ++= Seq(
            "io.getkyo"     %%% "kyo-core"    % kyoVersion,
            "org.typelevel" %%% "cats-effect" % catsEffectVersion,
            "io.getkyo"     %%% "kyo-test-api"    % kyoVersion % Test,
            "io.getkyo"     %%% "kyo-test-runner" % kyoVersion % Test
        )
    )
    .jvmSettings(
        Test / testFrameworks += new TestFramework("kyo.test.runner.SbtFramework")
    )
    .jsSettings(
        Test / testFrameworks += new TestFramework("kyo.test.runner.JsFramework")
    )

lazy val kyoSchedulerCats = crossProject(JVMPlatform)
    .crossType(CrossType.Full)
    .in(file("kyo-scheduler-cats"))
    .settings(
        name               := "kyo-scheduler-cats",
        crossScalaVersions := Seq(scala3Version, scala213Version),
        scalacOptions ++= (if (scalaVersion.value.startsWith("2.")) Seq("-Xsource:3") else Nil),
        libraryDependencies ++= Seq(
            "io.getkyo"     %%% "kyo-scheduler" % kyoVersion,
            "org.typelevel" %%% "cats-effect"   % catsEffectVersion,
            "org.scalatest" %%% "scalatest"     % scalaTestVersion % Test
        ),
        Test / fork := true
    )

lazy val kyoCompatCe = crossProject(JVMPlatform, JSPlatform, NativePlatform)
    .crossType(CrossType.Full)
    .in(file("kyo-compat-ce"))
    .settings(
        name               := "kyo-compat-ce",
        crossScalaVersions := Seq(scala3Version),
        scalacOptions += "-Xmax-inlines:1024",
        libraryDependencies ++= Seq(
            "org.typelevel" %%% "cats-effect" % catsEffectVersion,
            "co.fs2"        %%% "fs2-core"    % fs2Version,
            "org.scalatest" %%% "scalatest"   % scalaTestVersion % Test
        ),
        Test / scalacOptions += "-Xmax-inlines:1024"
    )
    .jvmSettings(
        Test / fork := true
    )
    .jsSettings(
        // Scala.js does not ship `java.time`. `CIO.now` returns `java.time.Instant`.
        // See https://github.com/cquiroz/scala-java-time
        libraryDependencies += "io.github.cquiroz" %%% "scala-java-time"      % "2.7.0",
        libraryDependencies += "io.github.cquiroz" %%% "scala-java-time-tzdb" % "2.7.0"
    )
