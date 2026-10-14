val scala3Version     = "3.9.0"
val scala213Version   = "2.13.18"
val kyoVersion        = "1.0.0-RC7"
val catsEffectVersion = "3.7.1"
val fs2Version        = "3.14.0"
val scalaTestVersion  = "3.2.20"

ThisBuild / scalaVersion       := scala3Version
ThisBuild / crossScalaVersions := Seq(scala3Version)
ThisBuild / organization       := "io.github.netherite-systems"
ThisBuild / version            := "0.1.0"
ThisBuild / versionScheme      := Some("early-semver")

inThisBuild(List(
    homepage := Some(url("https://github.com/netherite-systems/kyo-cats-interop")),
    licenses := List("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0")),
    organizationName := "NetheriteSystems",
    organizationHomepage := Some(url("https://github.com/netherite-systems/kyo-cats-interop")),
    scmInfo := Some(ScmInfo(
        url("https://github.com/netherite-systems/kyo-cats-interop"),
        "scm:git:git@github.com:netherite-systems/kyo-cats-interop.git"
    )),
    developers := List(Developer(
        "nojipiz",
        "Orlando Vargas",
        "nojipiz@protonmail.com",
        url("https://github.com/nojipiz")
    )),
    description := "Interop between Kyo and Cats Effect / fs2",
    pomIncludeRepository := { _ => false },
    publishMavenStyle := true,
    publishTo := {
        val centralSnapshots = "https://central.sonatype.com/repository/maven-snapshots/"
        if (version.value.endsWith("SNAPSHOT")) Some("central-snapshots" at centralSnapshots)
        else localStaging.value
    },
    credentials ++= {
        val localCreds = file(".sonatype-credentials")
        if (localCreds.exists()) Seq(Credentials(localCreds)) else Nil
    }
))

// Timing-sensitive conformance checks should not share a machine with another suite.
Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)

// `bindLocally` accepts one project for every platform of a backend, so each platform
// has its own matrix pointed at that platform's binding. `compatConformance` unpacks
// the suite shipped inside `kyo-compat-plugin` into `Test / sourceManaged`.
// See https://github.com/getkyo/kyo/blob/v1.0.0-RC7/kyo-compat/plugin/src/main/scala/io/getkyo/compat/CompatPlugin.scala
val CeLib = CompatBackendAxis.local("ce", "Ce", "-ce", Set("jvm", "js", "native"))

ThisBuild / compatKyoVersion := kyoVersion

lazy val root = project
    .in(file("."))
    .aggregate(
        kyoCats.jvm,
        kyoCats.js,
        kyoSchedulerCats.jvm,
        kyoCompatCe.jvm,
        kyoCompatCe.js,
        kyoCompatCe.native,
        ceConformanceJvm.get(CeLib).get.jvm,
        ceConformanceJs.get(CeLib).get.js,
        ceConformanceNative.get(CeLib).get.native
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
            "io.getkyo"     %%% "kyo-core"        % kyoVersion,
            "org.typelevel" %%% "cats-effect"     % catsEffectVersion,
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
            "co.fs2"        %%% "fs2-core"    % fs2Version
        )
    )
    .jsSettings(
        // Scala.js does not ship `java.time`. `CIO.now` returns `java.time.Instant`.
        // See https://github.com/cquiroz/scala-java-time
        libraryDependencies += "io.github.cquiroz" %%% "scala-java-time" % "2.7.0"
    )

lazy val ceConformanceJvm = (projectMatrix in file("kyo-compat-ce/.conformance/jvm"))
    .settings(
        name           := "kyo-compat-ce-conformance",
        publish / skip := true
    )
    .compatLibrary(CeLib)(VirtualAxis.jvm)(Seq(scala3Version))
    .bindLocally(CeLib, kyoCompatCe.jvm)
    .compatConformance()
    .jvmSettings(Test / fork := true)

lazy val ceConformanceJs = (projectMatrix in file("kyo-compat-ce/.conformance/js"))
    .settings(
        name           := "kyo-compat-ce-conformance",
        publish / skip := true
    )
    .compatLibrary(CeLib)(VirtualAxis.js)(Seq(scala3Version))
    .bindLocally(CeLib, kyoCompatCe.js)
    .compatConformance()

lazy val ceConformanceNative = (projectMatrix in file("kyo-compat-ce/.conformance/native"))
    .settings(
        name           := "kyo-compat-ce-conformance",
        publish / skip := true
    )
    .compatLibrary(CeLib)(VirtualAxis.native)(Seq(scala3Version))
    .bindLocally(CeLib, kyoCompatCe.native)
    .compatConformance()
