package com.ecommerce.utils

import com.typesafe.config.{Config, ConfigException, ConfigFactory}

/**
 * Chargement de la configuration externalisée (Partie 7 — Question 7.1).
 *
 * Propriétaire : Membre A (BANIGANTE Kpapou) — Relecteur : Membre C.
 *
 * Deux garanties apportées par cette classe :
 *   1. Aucun chemin, seuil ou paramètre Spark n'est codé en dur ailleurs
 *      dans le projet : tout passe par `ConfigLoader`.
 *   2. Toute clé absente du fichier `application.conf` retombe sur une valeur
 *      par défaut au lieu de faire échouer le job (mécanisme demandé par
 *      l'énoncé). Un avertissement est affiché pour rester traçable.
 */
class ConfigLoader(private val root: Config) extends Serializable {

  // --------------------------------------------------------------------------
  // Accesseurs génériques tolérants aux clés manquantes
  // --------------------------------------------------------------------------

  private def warn(path: String, default: Any): Unit =
    println(s"[CONFIG] Clé absente « $path » → valeur par défaut utilisée : $default")

  def getString(path: String, default: String): String =
    read(path, default)(root.getString)

  def getInt(path: String, default: Int): Int =
    read(path, default)(root.getInt)

  def getLong(path: String, default: Long): Long =
    read(path, default)(root.getLong)

  def getDouble(path: String, default: Double): Double =
    read(path, default)(root.getDouble)

  def getBoolean(path: String, default: Boolean): Boolean =
    read(path, default)(root.getBoolean)

  private def read[T](path: String, default: T)(f: String => T): T =
    try {
      if (root.hasPath(path)) f(path)
      else { warn(path, default); default }
    } catch {
      case _: ConfigException =>
        warn(path, default)
        default
    }

  // --------------------------------------------------------------------------
  // Application
  // --------------------------------------------------------------------------
  val appName: String = getString("app.name", "EcommerceAnalytics")

  // --------------------------------------------------------------------------
  // Spark
  // --------------------------------------------------------------------------
  val sparkMaster: String       = getString("app.spark.master", "local[*]")
  val shufflePartitions: Int    = getInt("app.spark.shuffle.partitions", 8)
  val logLevel: String          = getString("app.spark.log-level", "WARN")
  val autoBroadcastThreshold: Long =
    getLong("app.spark.autoBroadcastJoinThreshold", 10485760L)

  // --------------------------------------------------------------------------
  // Optimisations (Parties 5 et 5.3)
  // --------------------------------------------------------------------------
  val enableCache: Boolean     = getBoolean("app.optimization.enable-cache", true)
  val enableBroadcast: Boolean = getBoolean("app.optimization.enable-broadcast", true)
  val benchmark: Boolean       = getBoolean("app.optimization.benchmark", false)

  /** Copie de la configuration avec les optimisations forcées à `enabled`. */
  def withOptimizations(enabled: Boolean): ConfigLoader = {
    val overrides = ConfigFactory.parseString(
      s"""app.optimization.enable-cache     = $enabled
         |app.optimization.enable-broadcast = $enabled
         |""".stripMargin
    )
    new ConfigLoader(overrides.withFallback(root).resolve())
  }

  // --------------------------------------------------------------------------
  // Chemins d'entrée / sortie
  // --------------------------------------------------------------------------
  val transactionsPath: String =
    getString("app.data.input.transactions", "src/main/resources/data/transactions.csv")
  val usersPath: String =
    getString("app.data.input.users", "src/main/resources/data/users.json")
  val productsPath: String =
    getString("app.data.input.products", "src/main/resources/data/products.parquet")
  val merchantsPath: String =
    getString("app.data.input.merchants", "src/main/resources/data/merchants.csv")

  val outputPath: String    = getString("app.data.output.path", "output/")
  val outputCoalesce: Int   = getInt("app.data.output.coalesce", 1)
  val outputSampleSize: Int = getInt("app.data.output.sample-size", 2000)

  // --------------------------------------------------------------------------
  // Seuils de validation (Question 2.2)
  // --------------------------------------------------------------------------
  val minAmount: Double      = getDouble("app.validation.transaction.min-amount", 0.0)
  val timestampLength: Int   = getInt("app.validation.transaction.timestamp-length", 14)
  val minAge: Int            = getInt("app.validation.user.min-age", 16)
  val maxAge: Int            = getInt("app.validation.user.max-age", 100)
  val minIncome: Double      = getDouble("app.validation.user.min-income", 0.0)
  val minPrice: Double       = getDouble("app.validation.product.min-price", 0.0)
  val minRating: Double      = getDouble("app.validation.product.min-rating", 1.0)
  val maxRating: Double      = getDouble("app.validation.product.max-rating", 5.0)
  val minCommission: Double  = getDouble("app.validation.merchant.min-commission", 0.0)
  val maxCommission: Double  = getDouble("app.validation.merchant.max-commission", 1.0)

  // --------------------------------------------------------------------------
  // Règles métier (Parties 3 et 4)
  // --------------------------------------------------------------------------
  val rollingWindowDays: Int   = getInt("app.business.rolling-window-days", 7)
  val activeUserMinDays: Int   = getInt("app.business.active-user-min-days", 5)
  val suspiciousDeviationPct: Double =
    getDouble("app.business.suspicious.amount-deviation-pct", 300.0)
  val suspiciousMaxDelaySeconds: Int =
    getInt("app.business.suspicious.max-delay-seconds", 300)
  val riskyPaymentMethod: String =
    getString("app.business.suspicious.risky-payment-method", "CRYPTO")
  val suspiciousMinConditions: Int =
    getInt("app.business.suspicious.min-conditions", 2)
  val retentionFocusPeriod: Int = getInt("app.business.retention-focus-period", 3)
  val rfmQuantiles: Int         = getInt("app.business.rfm-quantiles", 5)
  val topProducts: Int          = getInt("app.business.top-products", 10)

  /** Trace lisible de la configuration effectivement appliquée. */
  def printSummary(): Unit = {
    println("=" * 100)
    println("CONFIGURATION EFFECTIVE")
    println("=" * 100)
    println(f"  app.name                    : $appName")
    println(f"  spark.master                : $sparkMaster")
    println(f"  spark.sql.shuffle.partitions: $shufflePartitions")
    println(f"  optimisation.cache          : $enableCache")
    println(f"  optimisation.broadcast      : $enableBroadcast")
    println(f"  input.transactions          : $transactionsPath")
    println(f"  input.users                 : $usersPath")
    println(f"  input.products              : $productsPath")
    println(f"  input.merchants             : $merchantsPath")
    println(f"  output.path                 : $outputPath")
    println("=" * 100)
  }
}

object ConfigLoader {

  /**
   * Charge `application.conf` (classpath) en laissant Typesafe Config appliquer
   * ses règles habituelles de surcharge : `-Dconfig.file=...`, `-Dconfig.resource=...`
   * puis les propriétés système `-Dapp.xxx=...`.
   */
  def load(): ConfigLoader = new ConfigLoader(ConfigFactory.load())

  /** Utilisé par les tests unitaires pour injecter une configuration ad hoc. */
  def fromString(hocon: String): ConfigLoader =
    new ConfigLoader(ConfigFactory.parseString(hocon).withFallback(ConfigFactory.load()).resolve())
}
