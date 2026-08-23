package com.ecommerce.utils

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * Écriture des résultats (Question 6.1).
 *
 * Propriétaire : Membre C (CHAKVOURNE Frédéric) — Relecteur : Membre A.
 *
 * Chaque résultat est écrit dans les deux formats demandés :
 *   - CSV     → lisible directement par une équipe métier (Excel) ;
 *   - Parquet → format colonne compressé, conservant les types, destiné à la
 *               ré-exploitation par un autre job Spark.
 *
 * `coalesce(n)` (n = 1 par défaut, externalisé) évite de livrer des dizaines de
 * fichiers `part-*` pour des tableaux de quelques centaines de lignes.
 */
object DataWriter {

  def write(df: DataFrame, name: String, config: ConfigLoader): Unit = {
    val base = config.outputPath.stripSuffix("/")
    val out  = df.coalesce(math.max(1, config.outputCoalesce))

    try {
      out.write
        .mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv(s"$base/csv/$name")

      out.write
        .mode(SaveMode.Overwrite)
        .parquet(s"$base/parquet/$name")

      println(s"[SORTIE] « $name » écrit dans $base/{csv,parquet}/$name")
    } catch {
      case e: Throwable =>
        println(s"[SORTIE] ERREUR lors de l'écriture de « $name » : ${e.getClass.getSimpleName} — ${e.getMessage}")
    }
  }

  /**
   * Écrit un DataFrame contenant des colonnes complexes (tableaux, structures)
   * en Parquet uniquement : le format CSV ne sait pas représenter ces types.
   */
  def writeParquetOnly(df: DataFrame, name: String, config: ConfigLoader): Unit = {
    val base = config.outputPath.stripSuffix("/")
    try {
      df.coalesce(math.max(1, config.outputCoalesce))
        .write.mode(SaveMode.Overwrite).parquet(s"$base/parquet/$name")
      println(s"[SORTIE] « $name » écrit dans $base/parquet/$name")
    } catch {
      case e: Throwable =>
        println(s"[SORTIE] ERREUR lors de l'écriture de « $name » : ${e.getClass.getSimpleName} — ${e.getMessage}")
    }
  }
}
