package com.ecommerce.analytics

import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Résultats composites renvoyés par [[Analytics]] — regroupés ici plutôt que
 * dans la classe elle-même par cohérence avec le reste du projet (une case
 * class imbriquée dans une classe pose problème pour les encodeurs Spark ;
 * ces case class ne contiennent que des DataFrame, jamais sérialisées via un
 * Dataset, donc le risque ne s'applique pas ici, mais la convention est gardée).
 */
object Analytics {

  /** Question 4.2 — analyse de cohortes. */
  case class CohortResult(
      cohortSizes: DataFrame, // cohort_month, nb_utilisateurs_initiaux
      retention: DataFrame,   // cohort_month, period_index, nb_utilisateurs_actifs, nb_utilisateurs_initiaux, taux_retention_pct
      bestAtFocus: DataFrame  // retention filtrée à app.business.retention-focus-period, triée par taux décroissant
  )

  /** Bonus 4.3 — segmentation RFM. */
  case class RfmResult(
      scores: DataFrame,       // user_id, recence_jours, frequence, montant, r_score, f_score, m_score, segment_rfm
      distribution: DataFrame, // segment_rfm, nb_clients
      crossTab: DataFrame      // segment_rfm x customer_segment déclaré (pivot)
  )

  /** Bonus 4.4 — analyse produits et catégories. */
  case class ProductAnalysisResult(
      topProduits: DataFrame,        // top N par chiffre d'affaires
      parCategorieRegion: DataFrame, // product_category x merchant_region + part relative
      parPaiementPeriode: DataFrame  // payment_method x day_period
  )
}

/**
 * Analytique business (Partie 4 — Questions 4.1, 4.2, bonus 4.3, bonus 4.4).
 *
 * Propriétaire : Membre C (CHAKVOURNE Frédéric).
 * Relecteur : Membre B (Q4.1 / Q4.2), Membre A (bonus 4.3 / bonus 4.4).
 *
 * Consomme le DataFrame enrichi produit par `DataTransformation.run(...)`
 * (Membre B) : colonnes `merchant_region`, `merchant_category`, `age_group`,
 * `tx_date`, `tx_month`, `product_category`, `product_rating`,
 * `product_stock`, `payment_method`, `day_period`, `customer_segment`, etc.
 */
class Analytics(spark: SparkSession, config: ConfigLoader) extends Serializable {

  import Analytics._

  // ==========================================================================
  //  Question 4.1 — rapport détaillé par marchand
  // ==========================================================================

  /**
   * CA total, nombre de transactions, clients uniques, montant moyen,
   * commission totale, classement par catégorie et par région, répartition
   * des ventes par tranche d'âge (pivot sur `age_group`).
   *
   * `dense_rank()` plutôt que `rank()` : ce dernier laisse des « trous » dans
   * le classement en cas d'ex-æquo, corrigé lors de la relecture croisée du
   * 24/08 (cf. CONTRIBUTIONS.md).
   */
  def merchantReport(enriched: DataFrame): DataFrame = {
    val base = enriched
      .groupBy("merchant_id", "merchant_name", "merchant_category", "merchant_region", "commission_rate")
      .agg(
        round(sum("amount"), 2).as("chiffre_affaires_total"),
        count("transaction_id").as("nb_transactions"),
        countDistinct("user_id").as("nb_clients_uniques"),
        round(avg("amount"), 2).as("montant_moyen_transaction"),
        round(sum("amount") * first("commission_rate"), 2).as("commission_totale")
      )

    val wCategory = Window.partitionBy("merchant_category").orderBy(col("chiffre_affaires_total").desc)
    val wRegion   = Window.partitionBy("merchant_region").orderBy(col("chiffre_affaires_total").desc)

    val ranked = base
      .withColumn("rang_categorie", dense_rank().over(wCategory))
      .withColumn("rang_region", dense_rank().over(wRegion))

    val ageBreakdown = enriched
      .groupBy("merchant_id")
      .pivot("age_group", Seq("Jeune", "Adulte", "Age Moyen", "Senior", "Inconnu"))
      .agg(round(sum("amount"), 2))
      .na.fill(0.0)

    ranked
      .join(ageBreakdown, Seq("merchant_id"), "left")
      .orderBy(col("chiffre_affaires_total").desc)
  }

  // ==========================================================================
  //  Question 4.2 — analyse de cohortes utilisateurs
  // ==========================================================================

  /**
   * Cohorte = mois de la PREMIÈRE TRANSACTION (`tx_month`), pas le mois
   * d'inscription — cf. décision technique 4.b du groupe (fidélité réelle,
   * pas acquisition marketing).
   */
  def cohortAnalysis(enriched: DataFrame): CohortResult = {
    val firstPurchase = enriched
      .groupBy("user_id")
      .agg(min("tx_month").as("cohort_month"))

    val cohortSizes = firstPurchase
      .groupBy("cohort_month")
      .agg(countDistinct("user_id").as("nb_utilisateurs_initiaux"))
      .orderBy("cohort_month")

    val withCohort = enriched
      .join(firstPurchase, Seq("user_id"))
      .withColumn(
        "period_index",
        months_between(
          to_date(concat(col("tx_month"), lit("-01")), "yyyy-MM-dd"),
          to_date(concat(col("cohort_month"), lit("-01")), "yyyy-MM-dd")
        ).cast("int")
      )

    val retention = withCohort
      .groupBy("cohort_month", "period_index")
      .agg(countDistinct("user_id").as("nb_utilisateurs_actifs"))
      .join(cohortSizes, Seq("cohort_month"))
      .withColumn(
        "taux_retention_pct",
        round(col("nb_utilisateurs_actifs") * lit(100.0) / col("nb_utilisateurs_initiaux"), 2)
      )
      .orderBy("cohort_month", "period_index")

    val bestAtFocus = retention
      .filter(col("period_index") === lit(config.retentionFocusPeriod))
      .orderBy(col("taux_retention_pct").desc)

    CohortResult(cohortSizes, retention, bestAtFocus)
  }

  /** Pivot de `retention` : une ligne par cohorte, une colonne par mois écoulé (period_index). */
  def retentionMatrix(retention: DataFrame): DataFrame =
    retention
      .groupBy("cohort_month")
      .pivot("period_index")
      .agg(first("taux_retention_pct"))
      .orderBy("cohort_month")

  // ==========================================================================
  //  Bonus 4.3 — segmentation RFM
  // ==========================================================================

  /**
   * Score 1 à `app.business.rfm-quantiles` (5 par défaut) sur Récence,
   * Fréquence, Montant, puis règle métier combinant les trois scores.
   *
   * Point d'attention (difficulté rencontrée, cf. CONTRIBUTIONS.md) : `ntile`
   * sur un tri croissant de `recence_jours` donnerait le score 5 au client le
   * PLUS ANCIEN. Tri en DÉCROISSANT pour que le score 5 corresponde toujours
   * au client le plus récent, cohérent avec fréquence et montant.
   */
  def rfmSegmentation(enriched: DataFrame): RfmResult = {
    val maxDateRow = enriched.agg(max("tx_date").as("max_date")).first()
    val maxDate    = maxDateRow.getAs[java.sql.Date]("max_date")
    val q          = config.rfmQuantiles

    val rfm = enriched
      .groupBy("user_id")
      .agg(
        datediff(lit(maxDate), max("tx_date")).as("recence_jours"),
        count("transaction_id").as("frequence"),
        round(sum("amount"), 2).as("montant")
      )

    val scored = rfm
      .withColumn("r_score", ntile(q).over(Window.orderBy(col("recence_jours").desc)))
      .withColumn("f_score", ntile(q).over(Window.orderBy(col("frequence").asc)))
      .withColumn("m_score", ntile(q).over(Window.orderBy(col("montant").asc)))

    // Seuils exprimés relativement à q pour rester cohérents si rfm-quantiles
    // change dans application.conf (q=5 par défaut -> seuils 4 / 3 / 2).
    val scores = scored.withColumn(
      "segment_rfm",
      when(col("r_score") >= q - 1 && col("f_score") >= q - 1 && col("m_score") >= q - 1, lit("Champions"))
        .when(col("r_score") >= q - 2 && col("f_score") >= q - 2, lit("Clients fideles"))
        .when(col("r_score") <= 2 && col("f_score") >= q - 2, lit("A risque"))
        .when(col("r_score") <= 2 && col("f_score") <= 2, lit("Perdus"))
        .otherwise(lit("Nouveaux"))
    )

    val distribution = scores
      .groupBy("segment_rfm")
      .agg(count(lit(1)).as("nb_clients"))
      .orderBy(col("nb_clients").desc)

    // customer_segment est porté par chaque transaction dans `enriched` : on
    // en extrait une valeur par utilisateur avant de croiser avec le RFM calculé.
    val declaredSegment = enriched.select("user_id", "customer_segment").dropDuplicates("user_id")

    val crossTab = scores
      .join(declaredSegment, Seq("user_id"), "left")
      .groupBy("segment_rfm")
      .pivot("customer_segment")
      .count()
      .na.fill(0)
      .orderBy("segment_rfm")

    RfmResult(scores, distribution, crossTab)
  }

  // ==========================================================================
  //  Bonus 4.4 — analyse produits et catégories
  // ==========================================================================

  def productAnalysis(enriched: DataFrame): ProductAnalysisResult = {

    val topProduits = enriched
      .groupBy("product_id", "product_name")
      .agg(
        round(sum("amount"), 2).as("chiffre_affaires"),
        round(avg("product_rating"), 2).as("note_moyenne"),
        first("product_stock").as("stock_disponible")
      )
      .orderBy(col("chiffre_affaires").desc)
      .limit(config.topProducts)

    val byCatRegion = enriched
      .groupBy("product_category", "merchant_region")
      .agg(
        round(sum("amount"), 2).as("chiffre_affaires"),
        count("transaction_id").as("nb_transactions")
      )
    val wRegion = Window.partitionBy("merchant_region")
    val parCategorieRegion = byCatRegion
      .withColumn(
        "pct_categorie_dans_region",
        round(col("chiffre_affaires") / sum("chiffre_affaires").over(wRegion) * 100, 2)
      )
      .orderBy(col("merchant_region"), col("chiffre_affaires").desc)

    val parPaiementPeriode = enriched
      .groupBy("payment_method", "day_period")
      .agg(round(sum("amount"), 2).as("chiffre_affaires"))
      .orderBy("payment_method", "day_period")

    ProductAnalysisResult(topProduits, parCategorieRegion, parPaiementPeriode)
  }
}
