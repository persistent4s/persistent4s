import org.typelevel.sbt.gha.{JavaSpec, PermissionValue, Permissions}

ThisBuild / tlBaseVersion          := "0.2"
ThisBuild / tlMimaPreviousVersions := Set.empty // reset after multi-module restructure
ThisBuild / scalaVersion           := "3.3.8"
ThisBuild / tlJdkRelease           := Some(17)
ThisBuild / organization           := "io.github.persistent4s"
ThisBuild / organizationName       := "Antonio Jimenez and Bastien Jolidon"
ThisBuild / startYear              := Some(2026)
ThisBuild / licenses               := Seq(License.Apache2)
ThisBuild / developers             := List(
  tlGitHubDev("antoniojimeneznieto", "Antonio Jimenez"),
  tlGitHubDev("Bjolidon", "Bastien Jolidon"),
)
ThisBuild / scalafmtOnCompile          := false // recommended in Scala 3
ThisBuild / testFrameworks             += new TestFramework("weaver.framework.CatsEffect")
ThisBuild / Test / logBuffered         := false
ThisBuild / Test / parallelExecution   := false
ThisBuild / githubWorkflowJavaVersions := Seq(JavaSpec.temurin("17"))
ThisBuild / githubWorkflowGeneratedCI  := {
  (ThisBuild / githubWorkflowGeneratedCI).value.map { job =>
    job.id match {
      case "dependency-submission" =>
        job.withPermissions(
          Some(
            Permissions.Specify.defaultRestrictive
              .withContents(PermissionValue.Write),
          ),
        )

      // Overwrite the default Java version for the validate-steward job to use Java 17 instead of Java 11.
      case "validate-steward" =>
        job
          .withJavas(List(JavaSpec.temurin("17")))
          .withSteps(
            WorkflowStep.Run(
              List(
                """echo "JAVA_HOME=$JAVA_HOME_17_X64" >> "$GITHUB_ENV"""",
                """echo "$JAVA_HOME_17_X64/bin" >> "$GITHUB_PATH"""",
                """"$JAVA_HOME_17_X64/bin/java" -version""",
              ),
              name = Some("Select Java 17"),
            ) :: job.steps,
          )
      case _ =>
        job
    }
  }
}

Global / onChangedBuildSource := ReloadOnSourceChanges
Global / semanticdbEnabled    := true // for metals

// Versions
val CatsEffectV = "3.7.0"

val Fs2V = "3.13.0"

val SkunkV = "2.0.0-RC2"

val CirceV = "0.14.16"

val Log4CatsV = "2.8.0"

val Otel4sV = "1.0.1"

val LogbackV = "1.5.38"

val TestcontainersV = "1.21.4"

val Fs2KafkaV = "4.0.0"

val WeaverV = "0.13.0"

val Http4sV = "0.23.36"

val Smithy4sV = smithy4s.codegen.BuildInfo.version

val PureconfigV = "0.17.10"

lazy val root = (project in file("."))
  .enablePlugins(NoPublishPlugin)
  .aggregate(core, postgres, circe, kafka, testkit, tests, examples, monitoring)

lazy val core = (project in file("modules/core"))
  .settings(
    name                 := "persistent4s-core",
    libraryDependencies ++= List(
      "org.typelevel" %% "cats-effect"   % CatsEffectV,
      "co.fs2"        %% "fs2-core"      % Fs2V,
      "org.typelevel" %% "log4cats-core" % Log4CatsV,
      "org.typelevel" %% "otel4s-core"   % Otel4sV,
      "org.typelevel" %% "weaver-cats"   % WeaverV % Test,
    ),
  )

lazy val postgres = (project in file("modules/postgres"))
  .dependsOn(core, circe)
  .settings(
    name                 := "persistent4s-postgres",
    libraryDependencies ++= List(
      "org.tpolecat"          %% "skunk-core"      % SkunkV,
      "org.tpolecat"          %% "skunk-circe"     % SkunkV,
      "org.typelevel"         %% "otel4s-core"     % Otel4sV,
      "org.typelevel"         %% "log4cats-core"   % Log4CatsV,
      "com.github.pureconfig" %% "pureconfig-core" % PureconfigV,
      "org.typelevel"         %% "weaver-cats"     % WeaverV         % Test,
      "ch.qos.logback"         % "logback-classic" % LogbackV        % Test,
      "org.testcontainers"     % "postgresql"      % TestcontainersV % Test,
      "org.typelevel"         %% "log4cats-noop"   % Log4CatsV       % Test,
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
      "org.typelevel"     %% "fs2-kafka"       % Fs2KafkaV,
      "org.typelevel"     %% "weaver-cats"     % WeaverV         % Test,
      "ch.qos.logback"     % "logback-classic" % LogbackV        % Test,
      "org.testcontainers" % "kafka"           % TestcontainersV % Test,
    ),
  )

lazy val testkit = (project in file("modules/testkit"))
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
  .dependsOn(core, testkit, postgres, circe, monitoring)
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

lazy val monitoring = (project in file("modules/monitoring"))
  .dependsOn(core, postgres % Test)
  .settings(
    name                 := "persistent4s-monitoring",
    libraryDependencies ++= List(
      "org.http4s"        %% "http4s-ember-server" % Http4sV,
      "org.http4s"        %% "http4s-dsl"          % Http4sV,
      "org.typelevel"     %% "weaver-cats"         % WeaverV         % Test,
      "ch.qos.logback"     % "logback-classic"     % LogbackV        % Test,
      "org.testcontainers" % "postgresql"          % TestcontainersV % Test,
      "org.http4s"        %% "http4s-ember-client" % Http4sV         % Test,
      "org.typelevel"     %% "otel4s-core"         % Otel4sV,
      "org.typelevel"     %% "log4cats-noop"       % Log4CatsV       % Test,
    ),
  )

addCommandAlias("lint", ";scalafmtAll ;scalafmtSbt")
