package com.ecommerce.analytics

import com.ecommerce.models.StageTiming
import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
 * Optimisations Spark (Partie 5 — Questions 5.1, 5.2, bonus 5.3).
 *
 * Propriétaire : Membre C (CHAKVOURNE Frédéric).
 *
 * Le broadcast des jointures (Question 5.2) est déjà appliqué directement
 * dans `DataTransformation` (méthode privée `maybeBroadcast`, pilotée par
 * `config.enableBroadcast`) — il n'est pas dupliqué ici. Cette classe gère :
 *   - la mise en cache NOMMÉE des DataFrame réutilisés (Question 5.1), avec
 *     deux niveaux : `persistIfEnabled` (MEMORY_AND_DISK_SER, pour les gros
 *     DataFrame comme les transactions) et `cacheIfEnabled` (cache mémoire
 *     simple, pour les petits référentiels users/products/merchants) ;
 *   - le chronométrage de chaque étape du pipeline (`timed`), dont
 *     `MainApp` se sert pour produire le tableau comparatif du bonus 5.3.
 *
 * Une instance est créée par `Pipeline` (une par configuration), donc son
 * `mode` (« sans » ou « avec » optimisation) est fixé une fois pour toutes à
 * la construction, à partir des drapeaux de la configuration reçue.
 */
class SparkOptimizations(spark: SparkSession, config: ConfigLoader) {

  private val timings: mutable.ArrayBuffer[StageTiming] = mutable.ArrayBuffer.empty
  private val cached: mutable.LinkedHashMap[String, DataFrame] = mutable.LinkedHashMap.empty

  // enable-cache et enable-broadcast sont toujours togglés ensemble par
  // ConfigLoader.withOptimizations : l'un suffit à déterminer le mode.
  private val mode: String = if (config.enableCache) "avec-optimisation" else "sans-optimisation"

  // --------------------------------------------------------------------------
  //  Chronométrage (bonus 5.3)
  // --------------------------------------------------------------------------

  def clearTimings(): Unit = timings.clear()

  /** Exécute `block`, mesure sa durée et l'enregistre sous le nom `etape`. */
  def timed[T](etape: String)(block: => T): T = {
    val debut  = System.currentTimeMillis()
    val result = block
    val duree  = System.currentTimeMillis() - debut
    timings += StageTiming(etape, mode, duree)
    println(f"[TIMING] $etape%-15s : $duree%,6d ms ($mode)")
    result
  }

  def allTimings: Seq[StageTiming] = timings.toSeq

  def timingsDataFrame(): DataFrame = {
    import spark.implicits._
    timings.toSeq.toDF()
  }

  // --------------------------------------------------------------------------
  //  Mise en cache nommée (Question 5.1)
  // --------------------------------------------------------------------------

  /** Persistance mémoire + disque sérialisée — pour les gros DataFrame (transactions). */
  def persistIfEnabled(df: DataFrame, name: String): DataFrame = {
    if (config.enableCache) {
      val persisted = df.persist(StorageLevel.MEMORY_AND_DISK_SER)
      cached(name) = persisted
      persisted
    } else df
  }

  /** Cache mémoire simple — pour les petits référentiels (users, products, merchants). */
  def cacheIfEnabled(df: DataFrame, name: String): DataFrame = {
    if (config.enableCache) {
      val c = df.cache()
      cached(name) = c
      c
    } else df
  }

  /** Libère un DataFrame nommé précis. Sans effet si absent du cache suivi. */
  def unpersist(name: String): Unit =
    cached.remove(name).foreach(_.unpersist())

  /** Libère tous les DataFrame actuellement suivis. */
  def unpersistAll(): Unit = {
    cached.values.foreach(_.unpersist())
    cached.clear()
  }

  def printCacheState(): Unit = {
    println("=" * 100)
    if (cached.isEmpty) println("[CACHE] Aucun DataFrame actuellement en cache.")
    else println(s"[CACHE] ${cached.size} DataFrame(s) en cache : ${cached.keys.mkString(", ")}")
    println("=" * 100)
  }
}

object SparkOptimizations {

  /**
   * Tableau comparatif du bonus 5.3 : une ligne par étape (durée sans / avec
   * optimisation, gain en %), plus une ligne TOTAL.
   *
   * La ligne TOTAL a été ajoutée après une difficulté rencontrée en pratique
   * (cf. CONTRIBUTIONS.md) : l'étape « transformation » peut apparaître plus
   * lente AVEC cache prise isolément (le coût de sérialisation est payé
   * immédiatement, alors que sans cache Spark élague les colonnes inutiles
   * pour un simple count()) — le coût est déplacé, pas créé, et seule la vue
   * globale du pipeline permet de juger du gain réel.
   */
  def comparison(spark: SparkSession, sans: Seq[StageTiming], avec: Seq[StageTiming]): DataFrame = {
    import spark.implicits._

    val sansDf = sans.toDF().select(col("etape"), col("duree_ms").as("duree_sans_ms"))
    val avecDf = avec.toDF().select(col("etape"), col("duree_ms").as("duree_avec_ms"))

    val parEtape = sansDf
      .join(avecDf, Seq("etape"))
      .withColumn("gain_pct", round((col("duree_sans_ms") - col("duree_avec_ms")) * 100.0 / col("duree_sans_ms"), 2))
      .orderBy("etape")

    val totalSans = sans.map(_.duree_ms).sum
    val totalAvec = avec.map(_.duree_ms).sum
    val totalGain =
      if (totalSans == 0) 0.0
      else BigDecimal((totalSans - totalAvec) * 100.0 / totalSans).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

    val totalRow = Seq(("TOTAL", totalSans, totalAvec, totalGain))
      .toDF("etape", "duree_sans_ms", "duree_avec_ms", "gain_pct")

    parEtape.unionByName(totalRow)
  }
}
