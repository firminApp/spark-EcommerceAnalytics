package com.ecommerce.analytics

import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Analytique business (Partie 4 — Questions 4.1, 4.2 et bonus 4.3 / 4.4).
 *
 * Propriétaire : Membre C (CHAKVOURNE Frédéric) — Relecteur : Membre B.
 *
 * Toutes les fonctions prennent en entrée le DataFrame **enrichi** produit par
 * [[DataTransformation]] : elles ne refont ni jointure ni parsing de date, ce
 * qui évite de relire les sources et permet de bénéficier du cache (Q5.1).
 */
class Analytics(spark: SparkSession, config: ConfigLoader) extends Serializable {

  import spark.implicits._

  private def r2(c: org.apache.spark.sql.Column) = round(c, 2)

  // ==========================================================================
  //  Question 4.1 — rapport détaillé par marchand
  // ==========================================================================

  /**
   * KPI par marchand : chiffre d'affaires, volumétrie, panier moyen, commission
   * perçue, classements par catégorie et par région, taux de transactions
   * suspectes (bonus 3.4).
   *
   * Le classement utilise `dense_rank` : deux marchands à chiffre d'affaires
   * strictement égal partagent le même rang sans « trouer » la numérotation.
   */
  def merchantReport(enriched: DataFrame): DataFrame = {
    val hasSuspicious = enriched.columns.contains("is_suspicious")

    val baseAggs = Seq(
      r2(sum("amount")).as("chiffre_affaires"),
      count(lit(1)).as("nb_transactions"),
      countDistinct(col("user_id")).as("nb_clients_uniques"),
      r2(avg("amount")).as("montant_moyen"),
      r2(sum(col("amount") * coalesce(col("commission_rate"), lit(0.0)))).as("commission_totale")
    )

    val suspiciousAggs =
      if (hasSuspicious)
        Seq(
          sum(col("is_suspicious")).as("nb_transactions_suspectes"),
          r2(avg(col("is_suspicious")) * 100.0).as("taux_transactions_suspectes_pct")
        )
      else Seq.empty

    val aggs = baseAggs ++ suspiciousAggs

    val grouped = enriched
      .groupBy("merchant_id", "merchant_name", "merchant_category", "merchant_region")
      .agg(aggs.head, aggs.tail: _*)

    // Classements par chiffre d'affaires (fonctions de fenêtrage).
    val wCategory = Window.partitionBy("merchant_category").orderBy(col("chiffre_affaires").desc)
    val wRegion   = Window.partitionBy("merchant_region").orderBy(col("chiffre_affaires").desc)

    val ranked = grouped
      .withColumn("rang_categorie", dense_rank().over(wCategory))
      .withColumn("rang_region",    dense_rank().over(wRegion))

    // Répartition des ventes par tranche d'âge des clients (pivot).
    val byAge = enriched
      .groupBy("merchant_id")
      .pivot("age_group", Seq("Jeune", "Adulte", "Age Moyen", "Senior", "Inconnu"))
      .agg(r2(sum("amount")))
      .na.fill(0.0)
      .withColumnRenamed("Jeune",     "ca_jeune")
      .withColumnRenamed("Adulte",    "ca_adulte")
      .withColumnRenamed("Age Moyen", "ca_age_moyen")
      .withColumnRenamed("Senior",    "ca_senior")
      .withColumnRenamed("Inconnu",   "ca_age_inconnu")

    ranked.join(byAge, Seq("merchant_id"), "left").orderBy(col("chiffre_affaires").desc)
  }

  // ==========================================================================
  //  Question 4.2 — analyse de cohortes
  // ==========================================================================

  /** Résultats de l'analyse de cohortes. */
  case class CohortResult(
      cohortSizes: DataFrame,   // cohort_month, nb_utilisateurs_initiaux
      retention: DataFrame,     // matrice (cohort_month, period_index)
      bestAtFocus: DataFrame    // meilleure cohorte à N mois
  )

  /**
   * Cohortes fondées sur le **mois de première transaction** de chaque
   * utilisateur (et non sur `registration_date` : l'énoncé demande le mois de
   * première transaction, ce qui mesure la fidélité réelle et non l'inscription).
   *
   * `period_index` = nombre de mois entiers écoulés entre le mois de la
   * transaction et le mois de la cohorte (0 pour le mois d'acquisition).
   */
  def cohortAnalysis(enriched: DataFrame): CohortResult = {
    val txs = enriched
      .filter(col("tx_date").isNotNull && col("user_id").isNotNull)
      .select(col("user_id"), col("tx_date"), col("tx_month"), col("amount"))

    // 1. Mois de première transaction par utilisateur.
    val firstTx = txs
      .groupBy("user_id")
      .agg(min(col("tx_date")).as("first_tx_date"))
      .withColumn("cohort_month", date_format(col("first_tx_date"), "yyyy-MM"))

    // 2. Rattachement de chaque transaction à sa cohorte + indice de période.
    val withCohort = txs
      .join(firstTx, Seq("user_id"), "inner")
      .withColumn(
        "period_index",
        months_between(trunc(col("tx_date"), "month"), trunc(col("first_tx_date"), "month")).cast("int")
      )

    // 3. Taille initiale de chaque cohorte.
    val cohortSizes = firstTx
      .groupBy("cohort_month")
      .agg(countDistinct(col("user_id")).as("nb_utilisateurs_initiaux"))

    // 4. Matrice de rétention (+ bonus : CA par cohorte et par période).
    val retention = withCohort
      .groupBy("cohort_month", "period_index")
      .agg(
        countDistinct(col("user_id")).as("nb_utilisateurs_actifs"),
        r2(sum("amount")).as("chiffre_affaires")
      )
      .join(cohortSizes, Seq("cohort_month"), "inner")
      .withColumn("taux_retention_pct", r2(col("nb_utilisateurs_actifs") * 100.0 / col("nb_utilisateurs_initiaux")))
      .withColumn("revenu_moyen_par_utilisateur", r2(col("chiffre_affaires") / col("nb_utilisateurs_actifs")))
      .orderBy(col("cohort_month"), col("period_index"))

    // 5. Meilleure cohorte à N mois (N = app.business.retention-focus-period).
    val bestAtFocus = retention
      .filter(col("period_index") === lit(config.retentionFocusPeriod))
      .orderBy(col("taux_retention_pct").desc)

    CohortResult(cohortSizes.orderBy("cohort_month"), retention, bestAtFocus)
  }

  /** Matrice de rétention pivotée : une ligne par cohorte, une colonne par période. */
  def retentionMatrix(retention: DataFrame, maxPeriod: Int = 12): DataFrame =
    retention
      .filter(col("period_index").between(0, maxPeriod))
      .groupBy("cohort_month", "nb_utilisateurs_initiaux")
      .pivot("period_index", (0 to maxPeriod).map(_.toString))
      .agg(first(col("taux_retention_pct")))
      .orderBy("cohort_month")

}
