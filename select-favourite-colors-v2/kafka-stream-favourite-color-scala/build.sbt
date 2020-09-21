name := "kafka-stream-favourite-color-scala"
organization := "com.kdg.kafka"
version := "0.1"

// Scala Version
scalaVersion := "2.13.3"

// Needed to resolve weird dependency
libraryDependencies += "javax.ws.rs" % "javax.ws.rs-api" % "2.1" artifacts(Artifact("javax.ws.rs-api", "jar", "jar"))

// Kafka Stream
libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-streams" % "2.5.1",
  "org.slf4j" % "slf4j-api" % "1.7.30",
  "org.slf4j" % "slf4j-log4j12" % "1.7.30"
)

// Leverage Java 11
javacOptions ++= Seq("-source", "11", "-target", "11", "-Xlint")
scalacOptions := Seq("-target:jvm-11")
initialize := {
  val _ = initialize.value
  if (sys.props("java.specification.version") != "11")
    sys.error("Java 8 is required for this project.")
}