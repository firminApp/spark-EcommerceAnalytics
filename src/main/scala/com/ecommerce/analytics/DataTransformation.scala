package com.ecommerce.analytics

import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Transformations avancées (Partie 3 — Questions 3.2, 3.3 et bonus 3.4).
 *
 * Propriétaire : Membre B (CAMARA Oumar) — Relecteur : Membre A.
 *
 * ============================================================================
 *  Stratégie de jointure retenue (Question 3.2 — à justifier)
 * ============================================================================
 *  - transactions ⟕ users     : LEFT — la transaction est le fait métier ; on
 *    ne veut pas qu'un `user_id` orphelin fasse disparaître du chiffre
 *    d'affaires réel. Les orphelins sont déjà comptés au bonus 2.5.
 *  - transactions ⟕ products  : LEFT — même raison ; le catalogue peut être
 *    incomplet (produit retiré) sans que la vente n'ait jamais eu lieu.
 *  - transactions ⟕ merchants : LEFT + `broadcast` — 600 lignes, table
 *    diffusée à tous les exécuteurs (Question 5.2).
 *  Un INNER join aurait été plus simple mais aurait supprimé silencieusement
 *  les lignes orphelines : les KPI marchands auraient été faux sans alerte.
 *  Les colonnes manquantes sont remplacées par « Inconnu » / null explicites.
 * ============================================================================
 */
class DataTransformation(spark: SparkSession, config: ConfigLoader) extends Serializable {

  import spark.implicits._

  /** Applique `broadcast()` uniquement si l'optimisation est activée (Q5.2 / Q5.3). */
  private def maybeBroadcast(df: DataFrame): DataFrame =
    if (config.enableBroadcast) broadcast(df) else df

  // ==========================================================================
  //  Question 3.2 — enrichTransactionData
  // ==========================================================================

  /**
   * Joint les quatre tables, applique l'UDF temporelle, ajoute les colonnes de
   * fenêtrage par utilisateur et la tranche d'âge du client.
   */
  def enrichTransactionData(
      transactions: DataFrame,
      users: DataFrame,
      products: DataFrame,
      merchants: DataFrame
  ): DataFrame = {

    // --- 1. Préfixage des colonnes homonymes ------------------------------
    // `name` et `category` existent dans plusieurs tables : on les renomme
    // AVANT la jointure, sinon Spark produit des références ambiguës.
    val u = users.select(
      col("user_id"),
      col("age"),
      col("annual_income"),
      col("city").as("user_city"),
      col("customer_segment"),
      col("registration_date").as("user_registration_date")
    )

    val p = products.select(
      col("product_id"),
      col("name").as("product_name"),
      col("category").as("product_category"),
      col("price").as("product_price"),
      col("rating").as("product_rating"),
      col("stock").as("product_stock")
    )

    val m = merchants.select(
      col("merchant_id"),
      col("name").as("merchant_name"),
      col("category").as("merchant_category"),
      col("region").as("merchant_region"),
      col("commission_rate"),
      col("establishment_date").as("merchant_establishment_date")
    )

    // --- 2. Jointures d'enrichissement ------------------------------------
    val joined = transactions
      .join(maybeBroadcast(u), Seq("user_id"), "left")
      .join(maybeBroadcast(p), Seq("product_id"), "left")
      .join(maybeBroadcast(m), Seq("merchant_id"), "left")

    // --- 3. Question 3.1 — application de l'UDF ---------------------------
    val withTime = joined
      .withColumn("time_features", TimeFeatures.extractTimeFeatures(col("timestamp")))
      .withColumn("hour",             col("time_features.hour"))
      .withColumn("day_of_week",      col("time_features.day_of_week"))
      .withColumn("month_name",       col("time_features.month"))
      .withColumn("is_weekend",       col("time_features.is_weekend"))
      .withColumn("day_period",       col("time_features.day_period"))
      .withColumn("is_working_hours", col("time_features.is_working_hours"))
      .drop("time_features")
      // Colonnes temporelles typées, réutilisées par toutes les fenêtres.
      .withColumn("tx_timestamp", to_timestamp(col("timestamp"), "yyyyMMddHHmmss"))
      .withColumn("tx_date",      to_date(col("tx_timestamp")))
      .withColumn("tx_month",     date_format(col("tx_timestamp"), "yyyy-MM"))
      .withColumn("tx_epoch",     unix_timestamp(col("tx_timestamp")))
      .withColumn("tx_day_epoch", unix_timestamp(col("tx_date")))

    // --- 4. Fonctions de fenêtrage par utilisateur ------------------------
    val wUserOrdered = Window.partitionBy("user_id").orderBy(col("tx_epoch").asc_nulls_last)
    val wUser        = Window.partitionBy("user_id")

    withTime
      // Rang de la transaction pour l'utilisateur, du plus ancien au plus récent.
      .withColumn("transaction_rank", row_number().over(wUserOrdered))
      // Nombre total de transactions de l'utilisateur.
      .withColumn("user_total_transactions", count(lit(1)).over(wUser))
      // Tranche d'âge du client.
      .withColumn("age_group", ageGroup(col("age")))
      // Valeurs de repli lisibles pour les références orphelines.
      .withColumn("merchant_region",   coalesce(col("merchant_region"),   lit("Inconnu")))
      .withColumn("merchant_category", coalesce(col("merchant_category"), lit("Inconnu")))
      .withColumn("customer_segment",  coalesce(col("customer_segment"),  lit("Inconnu")))
  }

  /**
   * Tranche d'âge (Question 3.2).
   * L'énoncé laisse un trou entre « moins de 25 ans » et « 26 à 44 ans » :
   * le groupe a tranché en rattachant 25 ans à la catégorie « Jeune ».
   * Un âge manquant donne « Inconnu » plutôt que d'être rangé arbitrairement.
   */
  def ageGroup(age: org.apache.spark.sql.Column): org.apache.spark.sql.Column =
    when(age.isNull, lit("Inconnu"))
      .when(age <= 25, lit("Jeune"))
      .when(age <= 44, lit("Adulte"))
      .when(age <= 64, lit("Age Moyen"))
      .otherwise(lit("Senior"))

  // ==========================================================================
  //  Question 3.3 — analyse par partition Window
  // ==========================================================================

  /**
   * Ajoute trois indicateurs comportementaux calculés sur une fenêtre glissante
   * de N jours (N = `app.business.rolling-window-days`, 7 par défaut) :
   *
   *  - `rolling_amount_7d`        : montant cumulé sur la fenêtre glissante ;
   *  - `is_active_user`           : 1 si l'utilisateur a acheté au moins
   *    `active-user-min-days` jours **distincts** sur la fenêtre ;
   *  - `days_since_previous_purchase` : délai en jours depuis l'achat précédent
   *    du même utilisateur (`lag`), null pour la première transaction.
   *
   * Point technique : la fenêtre est bornée avec `rangeBetween` sur
   * `tx_day_epoch` (secondes du jour tronqué). `rowsBetween` compterait un
   * nombre de lignes, pas une durée — ce qui ne correspondrait pas à
   * « 7 jours ». La borne vaut −(N−1) jours pour obtenir une fenêtre
   * calendaire de N jours, jour courant inclus.
   */
  def addWindowAnalytics(df: DataFrame): DataFrame = {
    val windowSeconds = (config.rollingWindowDays - 1).toLong * 24L * 3600L

    val wRolling = Window
      .partitionBy("user_id")
      .orderBy(col("tx_day_epoch").asc_nulls_last)
      .rangeBetween(-windowSeconds, Window.currentRow)

    val wUserOrdered = Window.partitionBy("user_id").orderBy(col("tx_epoch").asc_nulls_last)

    df
      .withColumn("rolling_amount_7d", round(sum(col("amount")).over(wRolling), 2))
      .withColumn("distinct_days_7d",  size(collect_set(col("tx_date")).over(wRolling)))
      .withColumn(
        "is_active_user",
        when(col("distinct_days_7d") >= lit(config.activeUserMinDays), lit(1)).otherwise(lit(0))
      )
      .withColumn("previous_tx_date",  lag(col("tx_date"), 1).over(wUserOrdered))
      .withColumn("previous_tx_epoch", lag(col("tx_epoch"), 1).over(wUserOrdered))
      .withColumn("days_since_previous_purchase", datediff(col("tx_date"), col("previous_tx_date")))
  }

  /** Pipeline de la Partie 3, dans l'ordre impose par l'enonce. */
  def run(
      transactions: DataFrame,
      users: DataFrame,
      products: DataFrame,
      merchants: DataFrame
  ): DataFrame =
    addWindowAnalytics(enrichTransactionData(transactions, users, products, merchants))
}
