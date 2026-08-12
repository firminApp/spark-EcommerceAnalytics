// ============================================================================
//  build.sbt — Projet Final Spark & Scala — GROUPE 3
//  Propriétaire : Membre A (BANIGANTE Kpapou) — Question 1.2
//  Relecteur    : Membre C (CHAKVOURNE Frédéric)
// ============================================================================

// --- Identité du projet ------------------------------------------------------
name         := "EcommerceAnalytics"
version      := "1.0.0"
organization := "com.ecommerce"

// --- Versions ---------------------------------------------------------------
// Spark 3.5.1 est compilé pour Scala 2.12 / 2.13. Nous retenons Scala 2.12.18 :
// c'est la version de référence des distributions Spark 3.5 (les binaires
// officiels spark-3.5.x-bin-hadoop3.tgz embarquent Scala 2.12), ce qui garantit
// que le JAR produit tourne sur un cluster standard sans recompilation.
val scala212     = "2.12.18"
val sparkVersion = "3.5.1"

scalaVersion := scala212

// --- Dépendances ------------------------------------------------------------
libraryDependencies ++= Seq(
  // Spark est marqué "provided" : il est fourni par le cluster (spark-submit)
  // et n'est donc PAS embarqué dans le JAR assembly (qui reste < 1 Mo).
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql"  % sparkVersion % Provided,

  // Gestion de la configuration externalisée (Partie 7).
  "com.typesafe"      % "config"     % "1.4.3",

  // Tests unitaires (Partie 8).
  "org.scalatest"    %% "scalatest"  % "3.2.18" % Test
)

// --- Options du compilateur -------------------------------------------------
scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-encoding", "UTF-8"
)

// --- Exécution locale (sbt run) ---------------------------------------------
// Par défaut, sbt exclut les dépendances "provided" du classpath de run.
// On les réinjecte pour pouvoir lancer le pipeline en local sans installer
// Spark, tout en gardant un JAR assembly léger pour le déploiement cluster.
Compile / run := Defaults
  .runTask(
    Compile / fullClasspath,
    Compile / run / mainClass,
    Compile / run / runner
  )
  .evaluated

Compile / runMain := Defaults
  .runMainTask(Compile / fullClasspath, Compile / run / runner)
  .evaluated

run / fork := true
Test / fork := true

// Options JVM nécessaires à Spark sur JDK 17+ (accès réflexif à java.nio).
val sparkJvmOptions = Seq(
  "-Xmx4g",
  // Les libellés du projet sont en français : sans cette option, la sortie
  // console est illisible sur une JVM dont l'encodage par défaut n'est pas UTF-8.
  "-Dfile.encoding=UTF-8",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)

run / javaOptions ++= sparkJvmOptions
Test / javaOptions ++= sparkJvmOptions

// --- JAR exécutable (sbt-assembly) ------------------------------------------
assembly / mainClass       := Some("com.ecommerce.analytics.MainApp")
assembly / assemblyJarName := "EcommerceAnalytics-assembly-1.0.0.jar"

assembly / assemblyMergeStrategy := {
  // Les jeux de données de src/main/resources/data ne sont PAS embarqués :
  // ils sont lus depuis le système de fichiers via application.conf.
  case PathList("data", _*)                              => MergeStrategy.discard
  case PathList("META-INF", "services", _*)              => MergeStrategy.concat
  case PathList("META-INF", _*)                          => MergeStrategy.discard
  case "reference.conf"                                  => MergeStrategy.concat
  case "application.conf"                                => MergeStrategy.first
  case "module-info.class"                               => MergeStrategy.discard
  case x                                                 =>
    val old = (assembly / assemblyMergeStrategy).value
    old(x)
}
