package com.ecommerce.utils

import org.apache.spark.sql.SparkSession

/**
 * Construction centralisée de la SparkSession (Question 6.1).
 *
 * Propriétaire : Membre A (BANIGANTE Kpapou) — Relecteur : Membre C.
 *
 * Tous les paramètres proviennent de `application.conf` via [[ConfigLoader]] :
 * le master, le nombre de partitions de shuffle (Question 5.2) et le seuil
 * de broadcast automatique.
 */
object SparkSessionBuilder {

  def build(config: ConfigLoader): SparkSession = {
    val builder = SparkSession
      .builder()
      .appName(config.appName)
      .master(config.sparkMaster)
      // Question 5.2 : le nombre de partitions de shuffle est externalisé.
      // 8 partitions suffisent pour ~138 000 transactions en local ; la valeur
      // par défaut de Spark (200) créerait des partitions quasi vides et un
      // surcoût d'ordonnancement important.
      .config("spark.sql.shuffle.partitions", config.shufflePartitions)
      .config("spark.sql.autoBroadcastJoinThreshold", config.autoBroadcastThreshold)
      // Écriture de CSV plus prévisible et lecture de dates tolérante.
      .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.parquet.compression.codec", "snappy")

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(config.logLevel)
    spark
  }
}
