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

  // ==========================================================================
  //  Bonus 4.3 — segmentation RFM
  // ==========================================================================

  /** Résultats de la segmentation RFM. */
  case class RfmResult(scores: DataFrame, distribution: DataFrame, crossTab: DataFrame)

  /**
   * Récence / Fréquence / Montant, scorés de 1 à 5 par quintiles (`ntile`).
   *
   * Convention de score : pour la récence, **plus le nombre de jours est petit,
   * meilleur est le client** — le tri est donc décroissant afin que le score 5
   * corresponde toujours au meilleur profil sur les trois axes.
   *
   * Règles d'affectation retenues par le groupe (à justifier en soutenance) :
   *   - Champions      : R ≥ 4 et F ≥ 4 et M ≥ 4 — récents, fréquents, dépensiers ;
   *   - Clients fidèles: F ≥ 4 et R ≥ 3 — reviennent régulièrement ;
   *   - Nouveaux       : R ≥ 4 et F ≤ 2 — arrivés récemment, peu d'historique ;
   *   - À risque       : R ≤ 2 et F ≥ 3 — bons clients qui ne reviennent plus ;
   *   - Perdus         : le reste (R ≤ 2 et F ≤ 2 principalement).
   * L'ordre d'évaluation est important : la première règle satisfaite gagne.
   */
  def rfmSegmentation(enriched: DataFrame): RfmResult = {
    // Date de référence = date la plus récente du jeu de données (et non la date
    // du jour) : la récence reste ainsi reproductible d'une exécution à l'autre.
    val refRow = enriched.agg(max(col("tx_date"))).head()
    val refDate =
      if (refRow.isNullAt(0)) java.sql.Date.valueOf(java.time.LocalDate.now())
      else refRow.getDate(0)

    val rfm = enriched
      .filter(col("user_id").isNotNull && col("tx_date").isNotNull)
      .groupBy("user_id")
      .agg(
        max(col("tx_date")).as("derniere_transaction"),
        count(lit(1)).as("frequence"),
        r2(sum("amount")).as("montant")
      )
      .withColumn("recence_jours", datediff(lit(refDate), col("derniere_transaction")))

    val q = config.rfmQuantiles
    val scored = rfm
      .withColumn("score_r", ntile(q).over(Window.orderBy(col("recence_jours").desc)))
      .withColumn("score_f", ntile(q).over(Window.orderBy(col("frequence").asc)))
      .withColumn("score_m", ntile(q).over(Window.orderBy(col("montant").asc)))
      .withColumn("score_rfm", concat(col("score_r"), col("score_f"), col("score_m")))
      .withColumn(
        "segment_rfm",
        when(col("score_r") >= 4 && col("score_f") >= 4 && col("score_m") >= 4, lit("Champions"))
          .when(col("score_f") >= 4 && col("score_r") >= 3, lit("Clients fideles"))
          .when(col("score_r") >= 4 && col("score_f") <= 2, lit("Nouveaux"))
          .when(col("score_r") <= 2 && col("score_f") >= 3, lit("A risque"))
          .otherwise(lit("Perdus"))
      )

    val distribution = scored
      .groupBy("segment_rfm")
      .agg(
        count(lit(1)).as("nb_clients"),
        r2(avg("recence_jours")).as("recence_moyenne_jours"),
        r2(avg("frequence")).as("frequence_moyenne"),
        r2(avg("montant")).as("montant_moyen")
      )
      .orderBy(col("nb_clients").desc)

    // Tableau croisé segment RFM calculé × customer_segment déclaré dans users.json.
    val declared = enriched.select("user_id", "customer_segment").distinct()
    val crossTab = scored
      .join(declared, Seq("user_id"), "left")
      .groupBy("segment_rfm")
      .pivot("customer_segment")
      .agg(count(lit(1)))
      .na.fill(0L)
      .orderBy("segment_rfm")

    RfmResult(scored, distribution, crossTab)
  }

  // ==========================================================================
  //  Bonus 4.4 — analyse produits et catégories
  // ==========================================================================

  case class ProductResult(topProduits: DataFrame, parCategorieRegion: DataFrame, parPaiementPeriode: DataFrame)

  def productAnalysis(enriched: DataFrame): ProductResult = {
    // 1. Top N produits par chiffre d'affaires.
    val topProduits = enriched
      .groupBy("product_id", "product_name", "product_category")
      .agg(
        r2(sum("amount")).as("chiffre_affaires"),
        count(lit(1)).as("nb_transactions"),
        r2(avg("product_rating")).as("note_moyenne"),
        max(col("product_stock")).as("stock_disponible")
      )
      .orderBy(col("chiffre_affaires").desc)
      .limit(config.topProducts)

    // 2. CA et volumétrie par catégorie et par région, avec le poids relatif
    //    de chaque catégorie dans sa région (fonction de fenêtrage).
    val wRegion = Window.partitionBy("merchant_region")
    val parCategorieRegion = enriched
      .groupBy("merchant_region", "category")
      .agg(
        r2(sum("amount")).as("chiffre_affaires"),
        count(lit(1)).as("nb_transactions")
      )
      .withColumn("ca_region", sum(col("chiffre_affaires")).over(wRegion))
      .withColumn("part_dans_region_pct", r2(col("chiffre_affaires") * 100.0 / col("ca_region")))
      .orderBy(col("merchant_region"), col("chiffre_affaires").desc)

    // 3. Répartition du CA par méthode de paiement et période de la journée.
    val parPaiementPeriode = enriched
      .groupBy("payment_method", "day_period")
      .agg(
        r2(sum("amount")).as("chiffre_affaires"),
        count(lit(1)).as("nb_transactions"),
        r2(avg("amount")).as("montant_moyen")
      )
      .orderBy(col("payment_method"), col("chiffre_affaires").desc)

    ProductResult(topProduits, parCategorieRegion, parPaiementPeriode)
  }
}
