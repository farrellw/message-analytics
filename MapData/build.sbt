import sbt.Keys._
import sbt._
import sbtrelease.Version

name := "MapData"

resolvers += Resolver.sonatypeRepo("public")
scalaVersion := "2.12.8"
releaseNextVersion := { ver => Version(ver).map(_.bumpMinor.string).getOrElse("Error") }
assemblyJarName in assembly := "mapData.jar"

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-lambda-java-events" % "2.2.5",
  "com.amazonaws" % "aws-lambda-java-core" % "1.2.0",
  "com.github.seratch" %% "awscala-dynamodb" % "0.8.+",
  "com.github.seratch" %% "awscala-s3" % "0.8.+",
  "com.typesafe.play" %% "play-json" % "2.7.2",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-feature",
  "-Xfatal-warnings"
)
