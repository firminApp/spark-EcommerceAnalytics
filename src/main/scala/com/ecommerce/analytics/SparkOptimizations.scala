package com.ecommerce.analytics

import com.ecommerce.models.StageTiming
import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
 * Optimisations Spark (Partie 5 — Questions 5.1, 5.2 et bonus 5.3).
 *
 * Propriétaire : Membre C (CHAKVOURNE Frédéric) — Relecteurs : Membres A et B.
 *
 * Les trois leviers demandés par l'énoncé sont centralisés ici afin qu'un seul
 * drapeau de configuration (`app.optimization.enable-cache` /
 * `enable-broadcast`) permette de comparer le pipeline optimisé et non optimisé
 * (bonus 5.3), sans dupliquer une ligne de logique métier.
 *
 *  - `cache()`      → DataFrame réutilisés plusieurs fois et de taille modérée
 *                     (référentiels validés, rapport qualité).
 *  - `persist(MEMORY_AND_DISK_SER)` → DataFrame volumineux (les ~138 000
 *                     transactions enrichies, réutilisées par les 4 analyses) :
 *                     sérialisé pour diviser l'empreinte mémoire, et déversé
 *                     sur disque plutôt que recalculé si la RAM manque.
 *  - `unpersist()`  → libération explicite dès que l'étape est terminée.
 */
class SparkOptimizations(spark: SparkSession, config: ConfigLoader) {

  /** Registre des DataFrame mis en cache, pour pouvoir tous les libérer. */
  private val cached = mutable.ListBuffer.empty[(String, DataFrame)]

  /** Chronométrage de chaque grande étape (bonus 5.3 et Question 6.2). */
  private val timings = mutable.ListBuffer.empty[StageTiming]

  private def mode: String =
    if (config.enableCache || config.enableBroadcast) "avec-optimisation" else "sans-optimisation"

  // ==========================================================================
  //  Question 5.1 — stockage
  // ==========================================================================

  /** `cache()` (MEMORY_AND_DISK) pour les DataFrame petits et très réutilisés. */
  def cacheIfEnabled(df: DataFrame, name: String): DataFrame =
    if (!config.enableCache) df
    else {
      val c = df.cache()
      cached += (name -> c)
      println(s"[OPTIM] cache() appliqué à « $name »")
      c
    }

  /** `persist(MEMORY_AND_DISK_SER)` pour les DataFrame trop volumineux. */
  def persistIfEnabled(df: DataFrame, name: String): DataFrame =
    if (!config.enableCache) df
    else {
      val p = df.persist(StorageLevel.MEMORY_AND_DISK_SER)
      cached += (name -> p)
      println(s"[OPTIM] persist(MEMORY_AND_DISK_SER) appliqué à « $name »")
      p
    }

  /** Libère un DataFrame précis. */
  def unpersist(name: String): Unit =
    cached.find(_._1 == name).foreach { case (n, df) =>
      df.unpersist(blocking = false)
      cached -= (n -> df)
      println(s"[OPTIM] unpersist() de « $n »")
    }

  /** Libère tout le cache (appelée en fin de pipeline). */
  def unpersistAll(): Unit = {
    cached.foreach { case (n, df) =>
      df.unpersist(blocking = false)
      println(s"[OPTIM] unpersist() de « $n »")
    }
    cached.clear()
  }

  /** Trace du plan de stockage, utile en soutenance. */
  def printCacheState(): Unit = {
    println("-" * 100)
    println(s"[OPTIM] DataFrame actuellement en cache : ${cached.size}")
    cached.foreach { case (n, df) => println(s"         • $n → ${df.storageLevel}") }
    println(s"[OPTIM] spark.sql.shuffle.partitions = ${spark.conf.get("spark.sql.shuffle.partitions")}")
    println(s"[OPTIM] spark.sql.autoBroadcastJoinThreshold = ${spark.conf.get("spark.sql.autoBroadcastJoinThreshold")}")
    println("-" * 100)
  }

  // ==========================================================================
  //  Bonus 5.3 — chronométrage
  // ==========================================================================

  /**
   * Exécute un bloc en mesurant sa durée. Le bloc doit être « matérialisant »
   * (count, write, show) : Spark étant paresseux, chronométrer une simple
   * construction de DataFrame ne mesurerait rien.
   */
  def timed[T](etape: String)(block: => T): T = {
    val t0  = System.nanoTime()
    val res = block
    val ms  = (System.nanoTime() - t0) / 1000000L
    timings += StageTiming(etape, mode, ms)
    println(f"[TIMING] $etape%-20s ($mode%-18s) : $ms%,8d ms")
    res
  }

  def allTimings: Seq[StageTiming] = timings.toList

  def clearTimings(): Unit = timings.clear()

  /** Tableau des durées de l'exécution courante. */
  def timingsDataFrame(extra: Seq[StageTiming] = Seq.empty): DataFrame = {
    import spark.implicits._
    (timings.toList ++ extra).toDF()
  }
}

object SparkOptimizations {

  /**
   * Bonus 5.3 — construit le tableau comparatif « avant / après » à partir de
   * deux séries de mesures (pipeline sans puis avec optimisations).
   */
  def comparison(spark: SparkSession, sans: Seq[StageTiming], avec: Seq[StageTiming]): DataFrame = {
    import spark.implicits._
    val avecMap = avec.map(t => t.etape -> t.duree_ms).toMap
    def pct(sansMs: Long, avecMs: Long): Double =
      if (sansMs == 0) 0.0
      else BigDecimal((sansMs - avecMs) * 100.0 / sansMs).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

    val rows = sans.map { s =>
      val a = avecMap.getOrElse(s.etape, 0L)
      (s.etape, s.duree_ms, a, pct(s.duree_ms, a))
    }

    // Ligne de synthèse : c'est le total qui décide de l'intérêt des
    // optimisations, une étape prise isolément pouvant être ralentie par le
    // coût d'écriture du cache (voir l'analyse dans le README).
    val totalSans = sans.map(_.duree_ms).sum
    val totalAvec = avec.map(_.duree_ms).sum
    val total     = ("TOTAL", totalSans, totalAvec, pct(totalSans, totalAvec))

    (rows :+ total).toDF("etape", "duree_sans_optimisation_ms", "duree_avec_optimisation_ms", "gain_pct")
  }
}
