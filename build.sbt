ThisBuild / tlBaseVersion          := "0.1"
ThisBuild / tlMimaPreviousVersions := Set.empty // reset after multi-module restructure
ThisBuild / scalaVersion           := "3.8.3"
ThisBuild / tlJdkRelease           := Some(17)
ThisBuild / organization           := "io.github.antoniojimeneznieto"
ThisBuild / organizationName       := "persistent4s"
ThisBuild / startYear              := Some(2026)
ThisBuild / licenses               := Seq(License.Apache2)
ThisBuild / developers             := List(
  tlGitHubDev("antoniojimeneznieto", "Antonio Jimenez"),
  tlGitHubDev("Bjolidon", "Bastien Jolidon"),
)
ThisBuild / scalafmtOnCompile := false // recommended in Scala 3
ThisBuild / testFrameworks    += new TestFramework("weaver.framework.CatsEffect")

Global / onChangedBuildSource := ReloadOnSourceChanges
Global / semanticdbEnabled    := true // for metals

// Versions
val CatsEffectV = "3.7.0"

val Fs2V = "3.13.0"

val SkunkV = "0.6.5"

val CirceV = "0.14.15"

val Log4CatsV = "2.8.0"

val Otel4sV = "0.15.2"

val LogbackV = "1.5.32"

val TestcontainersV = "1.21.4"

val Fs2KafkaV = "3.9.1"

val WeaverV = "0.12.0"

val Http4sV = "0.23.33"

val Smithy4sV = smithy4s.codegen.BuildInfo.version

val PureconfigV = "0.17.8"

lazy val root = (project in file("."))
  .enablePlugins(NoPublishPlugin)
  .aggregate(core, postgres, circe, kafka, testkit, tests, examples)

lazy val core = (project in file("modules/core"))
  .settings(
    name                 := "persistent4s-core",
    libraryDependencies ++= List(
      "org.typelevel" %% "cats-effect"    % CatsEffectV,
      "co.fs2"        %% "fs2-core"       % Fs2V,
      "org.typelevel" %% "log4cats-slf4j" % Log4CatsV,
      "org.typelevel" %% "otel4s-core"    % Otel4sV,
      "org.typelevel" %% "weaver-cats"    % WeaverV % Test,
    ),
  )

lazy val postgres = (project in file("modules/postgres"))
  .dependsOn(core, circe)
  .settings(
    name                 := "persistent4s-postgres",
    libraryDependencies ++= List(
      "org.tpolecat"          %% "skunk-core"      % SkunkV,
      "org.tpolecat"          %% "skunk-circe"     % SkunkV,
      "com.github.pureconfig" %% "pureconfig-core" % PureconfigV,
      "org.typelevel"         %% "weaver-cats"     % WeaverV         % Test,
      "ch.qos.logback"         % "logback-classic" % LogbackV        % Test,
      "org.testcontainers"     % "postgresql"      % TestcontainersV % Test,
    ),
  )

lazy val circe = (project in file("modules/circe"))
  .dependsOn(core)
  .settings(
    name                 := "persistent4s-circe",
    libraryDependencies ++= List(
      "io.circe"      %% "circe-core"    % CirceV,
      "io.circe"      %% "circe-generic" % CirceV,
      "io.circe"      %% "circe-parser"  % CirceV,
      "org.typelevel" %% "weaver-cats"   % WeaverV % Test,
    ),
  )

lazy val kafka = (project in file("modules/kafka"))
  .dependsOn(core)
  .settings(
    name                 := "persistent4s-kafka",
    libraryDependencies ++= List(
      "com.github.fd4s"   %% "fs2-kafka"       % Fs2KafkaV,
      "org.typelevel"     %% "weaver-cats"     % WeaverV         % Test,
      "ch.qos.logback"     % "logback-classic" % LogbackV        % Test,
      "org.testcontainers" % "kafka"           % TestcontainersV % Test,
    ),
  )

lazy val testkit = (project in file("modules/testkit"))
  .dependsOn(core)
  .settings(
    name                 := "persistent4s-testkit",
    libraryDependencies ++= List(
      "org.typelevel" %% "cats-effect" % CatsEffectV,
      "org.typelevel" %% "weaver-cats" % WeaverV % Test,
    ),
  )

lazy val tests = (project in file("modules/tests"))
  .dependsOn(core, postgres, circe, kafka, testkit)
  .enablePlugins(NoPublishPlugin)
  .settings(
    name                 := "persistent4s-tests",
    libraryDependencies ++= List(
      "org.typelevel"     %% "weaver-cats"     % WeaverV         % Test,
      "ch.qos.logback"     % "logback-classic" % LogbackV        % Test,
      "org.testcontainers" % "postgresql"      % TestcontainersV % Test,
    ),
  )

lazy val examples = (project in file("modules/examples"))
  .dependsOn(core, testkit, postgres, circe)
  .enablePlugins(NoPublishPlugin, Smithy4sCodegenPlugin)
  .settings(
    name                 := "persistent4s-examples",
    libraryDependencies ++= List(
      "com.disneystreaming.smithy4s" %% "smithy4s-http4s"         % Smithy4sV,
      "com.disneystreaming.smithy4s" %% "smithy4s-http4s-swagger" % Smithy4sV,
      "org.http4s"                   %% "http4s-ember-server"     % Http4sV,
      "ch.qos.logback"                % "logback-classic"         % LogbackV,
    ),
  )

addCommandAlias("lint", ";scalafmtAll ;scalafmtSbt")
