name := "DistributedTestAutomationDeequ"
version := "2021.1"
scalaVersion := "2.12.12"

resolvers ++= Seq(
  "Maven Repository" at "https://repo1.maven.org/maven2/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.12" % "3.0.1",
  "org.apache.spark" % "spark-sql_2.12" % "3.0.1",
  "com.holdenkarau" %% "spark-testing-base" % "2.4.5_0.14.0" % Test,
  "org.scalactic" %% "scalactic" % "3.0.4",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "com.databricks" % "dbutils-api_2.11" % "0.0.4",
  "com.google.code.gson" % "gson" % "2.8.0",
  "com.microsoft.azure" % "applicationinsights-core" % "2.3.0",
  "com.amazon.deequ" % "deequ" % "1.2.2-spark-3.0"
)

publishArtifact in Test := true

// Custom sbt task that will execute scalastyle
lazy val compileScalastyle = taskKey[Unit]("Call the scalastyle plugin")
compileScalastyle := scalastyle.in(Compile).toTask("").value

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyOption in Test in assembly:= (assemblyOption in Test in assembly)
  .value.copy(includeScala = false)

assemblyJarName in assembly := s"${name.value.toLowerCase}_2.12-${version.value}.jar"
assemblyJarName in Test in assembly := s"${name.value.toLowerCase}_2.12-${version.value}-tests.jar"

assembly := (assembly dependsOn (compileScalastyle)).value
assembly in Test := ((assembly in Test) dependsOn (compileScalastyle)).value
