import sbt._
import Keys._

object TractionBuild extends Build {

  override def rootProject = Some(core)

  val commonSettings = Seq(
    organization  := "com.gravitydev",
    version       := "0.0.2",
    scalaVersion  := "2.11.1",
    crossScalaVersions := Seq("2.11.1", "2.10.3"),
    scalacOptions ++= Seq("-deprecation","-unchecked"/*,"-Xlog-implicits","-XX:-OmitStackTraceInFastThrow"*/),
    testOptions in Test += Tests.Argument("-oF"),
    resolvers ++= Seq(
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      "bintray/non" at "http://dl.bintray.com/non/maven" // for upickle's deps
    ),
    publishMavenStyle := true,
    // To publish, put these credentials in ~/.m2/credentials
    //credentials += Credentials("Sonatype Nexus Repository Manager", "****", "****", "****"),
    credentials += Credentials(Path.userHome / ".m2" / ".credentials"),
    publishTo := {
        val nexus = "http://nexus.agiledigital.com.au/nexus/"
        if (version.value.trim.endsWith("SNAPSHOT")) {
            Some("snapshots" at nexus + "content/repositories/snapshots")
        } else {
            Some("releases"  at nexus + "content/repositories/releases")
        }
    }
  )

  lazy val core: Project = Project(id = "traction-core", base = file("core"))
    .settings(commonSettings:_*)
    .settings(
      name := "traction-core",
      libraryDependencies ++= Seq(
        "com.lihaoyi" % "upickle_2.11" % "0.2.5",
        "org.scalatest" %% "scalatest" % "2.1.6" % "test",
        "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"
      )
    )

  lazy val amazonswf: Project = Project(id = "traction-amazonswf", base = file("amazonswf"))
    .settings(commonSettings:_*)
    .settings(
      name := "traction-amazonswf",
      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-actor"   % "2.3.5",
        "com.typesafe.akka" %% "akka-agent"   % "2.3.5",
        "com.amazonaws"     % "aws-java-sdk"  % "1.8.9.1",
        "org.scalatest" %% "scalatest" % "2.1.6" % "test"
      ),
      addCompilerPlugin("org.scalamacros" % "paradise" % "2.0.1" cross CrossVersion.full)
    ) dependsOn core

  lazy val sample = Project(id = "traction-sample", base = file("sample"))
    .dependsOn(core, amazonswf)
    .settings(commonSettings:_*)
    .settings(
      libraryDependencies ++= Seq(
        "ch.qos.logback" % "logback-classic" % "1.1.2" % "runtime"
      )
    )
}
