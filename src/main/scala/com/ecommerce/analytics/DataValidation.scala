package com.ecommerce.analytics

import com.ecommerce.models._
import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

/**
 * Validation des données et rapport de qualité
 * (Partie 2 — Questions 2.2, 2.4 et bonus 2.5).
 *
 * Propriétaire : Membre A (BANIGANTE Kpapou) — Relecteur : Membre B.
 *
 * Principe retenu (« ajout version groupe » de l'énoncé) : aucune ligne n'est
 * perdue. Chaque validation renvoie un [[ValidationOutcome]] contenant :
 *   - `valid`    : les lignes conformes ;
 *   - `rejected` : les lignes non conformes, enrichies d'une colonne
 *     `rejection_reason` qui **liste toutes** les règles violées (et pas
 *     seulement la première), séparées par « | ».
 *
 * Le fichier des rejets est écrit sur disque par [[MainApp]] : c'est le
 * livrable qui permet à une équipe métier de corriger la source.
 */
object DataValidation {

  /** Résultat d'une validation : lignes valides, lignes rejetées et compteurs. */
  case class ValidationOutcome(
      dataset: String,
      valid: DataFrame,
      rejected: DataFrame,
      nbLues: Long,
      nbValides: Long,
      nbRejetees: Long,
      nbValeursNulles: Long
  ) {
    def tauxRejet: Double =
      if (nbLues == 0) 0.0 else BigDecimal(nbRejetees * 100.0 / nbLues).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

    def print(): Unit = {
      println(f"[VALIDATION] $dataset%-13s : $nbLues%,10d lues → $nbValides%,10d valides / $nbRejetees%,8d rejetées ($tauxRejet%6.2f %%)")
      if (nbRejetees == 0 && nbLues > 0)
        println(s"[VALIDATION] ATTENTION : 0 rejet sur « $dataset ». L'énoncé garantit la présence d'anomalies : vérifier les règles.")
    }
  }

  // ==========================================================================
  //  Moteur générique de validation
  // ==========================================================================

  /**
   * Applique une liste de règles à un DataFrame.
   *
   * @param rules couples (libellé de la règle, condition **de validité**).
   *              Une condition qui vaut `null` (typiquement `amount > 0` quand
   *              `amount` est nul) est traitée comme une violation : on ne
   *              laisse jamais passer une ligne « par ignorance ».
   */
  private def applyRules(name: String, df: DataFrame, rules: Seq[(String, Column)]): ValidationOutcome = {
    val violations: Seq[Column] = rules.map { case (label, isValid) =>
      when(coalesce(isValid, lit(false)) === false, lit(label)).otherwise(lit(null).cast("string"))
    }

    // concat_ws ignore les éléments nuls : la colonne ne contient donc que les
    // règles effectivement violées.
    val reason = concat_ws(" | ", violations: _*)

    val flagged   = df.withColumn("rejection_reason", reason)
    val valid     = flagged.filter(col("rejection_reason") === "").drop("rejection_reason")
    val rejected  = flagged.filter(col("rejection_reason") =!= "")

    val nbLues     = df.count()
    val nbRejetees = rejected.count()

    ValidationOutcome(
      dataset         = name,
      valid           = valid,
      rejected        = rejected,
      nbLues          = nbLues,
      nbValides       = nbLues - nbRejetees,
      nbRejetees      = nbRejetees,
      nbValeursNulles = countNulls(df)
    )
  }

  /**
   * Question 2.4 — nombre total de valeurs nulles, toutes colonnes confondues.
   * Une seule action Spark : on somme, colonne par colonne, dans une même
   * agrégation, puis on additionne les résultats côté driver.
   */
  def countNulls(df: DataFrame): Long = {
    if (df.columns.isEmpty) return 0L
    val exprs = df.columns.map(c => sum(when(col(c).isNull, 1L).otherwise(0L)).as(c))
    val row   = df.agg(exprs.head, exprs.tail: _*).head()
    (0 until row.length).map(i => if (row.isNullAt(i)) 0L else row.getLong(i)).sum
  }

  // ==========================================================================
  //  Question 2.2 — règles par jeu de données
  // ==========================================================================

  /** Transactions : `amount > 0` et `timestamp` de 14 caractères. */
  def validateTransactions(ds: Dataset[Transaction], c: ConfigLoader): ValidationOutcome =
    applyRules(
      "transactions",
      ds.toDF(),
      Seq(
        "transaction_id manquant"                            -> col("transaction_id").isNotNull,
        s"montant <= ${c.minAmount}"                         -> (col("amount") > lit(c.minAmount)),
        s"timestamp != ${c.timestampLength} caracteres"      -> (length(col("timestamp")) === lit(c.timestampLength)),
        "timestamp non numerique"                            -> col("timestamp").rlike("^[0-9]+$")
      )
    )

  /** Users : `age` entre 16 et 100, `annual_income > 0`. */
  def validateUsers(ds: Dataset[User], c: ConfigLoader): ValidationOutcome =
    applyRules(
      "users",
      ds.toDF(),
      Seq(
        "user_id manquant"                                   -> col("user_id").isNotNull,
        s"age hors [${c.minAge};${c.maxAge}]"                -> col("age").between(c.minAge, c.maxAge),
        s"revenu annuel <= ${c.minIncome}"                   -> (col("annual_income") > lit(c.minIncome))
      )
    )

  /** Products : `price > 0`, `rating` entre 1 et 5. */
  def validateProducts(ds: Dataset[Product], c: ConfigLoader): ValidationOutcome =
    applyRules(
      "products",
      ds.toDF(),
      Seq(
        "product_id manquant"                                -> col("product_id").isNotNull,
        s"prix <= ${c.minPrice}"                             -> (col("price") > lit(c.minPrice)),
        s"note hors [${c.minRating};${c.maxRating}]"         -> col("rating").between(c.minRating, c.maxRating)
      )
    )

  /** Merchants : `commission_rate` entre 0 et 1. */
  def validateMerchants(ds: Dataset[Merchant], c: ConfigLoader): ValidationOutcome =
    applyRules(
      "merchants",
      ds.toDF(),
      Seq(
        "merchant_id manquant"                                    -> col("merchant_id").isNotNull,
        s"commission hors [${c.minCommission};${c.maxCommission}]" -> col("commission_rate").between(c.minCommission, c.maxCommission)
      )
    )

  // ==========================================================================
  //  Bonus 2.5 — intégrité référentielle
  // ==========================================================================

  /**
   * Compte les transactions dont `user_id`, `product_id` ou `merchant_id` ne
   * correspond à aucun enregistrement du référentiel associé.
   *
   * Mise en œuvre par trois `left_anti` join : c'est l'opération exactement
   * prévue par Spark pour « les lignes de gauche sans correspondance à droite »,
   * elle évite le `collect()` d'une liste d'identifiants côté driver.
   * Les référentiels utilisés sont les référentiels **validés** : une référence
   * vers un utilisateur lui-même rejeté est donc bien comptée comme orpheline.
   */
  def referentialIntegrity(
      transactions: DataFrame,
      users: DataFrame,
      products: DataFrame,
      merchants: DataFrame
  ): (Long, Long, Long) = {
    // Les colonnes sont renommées avant la jointure : `user_id === user_id`
    // serait une référence ambiguë pour Spark (deux attributs homonymes).
    // Les références nulles sont exclues : une valeur manquante est déjà
    // comptabilisée dans `nb_valeurs_nulles`, ce n'est pas un lien orphelin.
    def orphans(fk: String, ref: DataFrame, pk: String): Long = {
      val gauche = transactions.select(col(fk).as("cle_etrangere")).filter(col("cle_etrangere").isNotNull)
      val droite = ref.select(col(pk).as("cle_primaire")).distinct()
      gauche.join(droite, col("cle_etrangere") === col("cle_primaire"), "left_anti").count()
    }

    (
      orphans("user_id",     users,     "user_id"),
      orphans("product_id",  products,  "product_id"),
      orphans("merchant_id", merchants, "merchant_id")
    )
  }

  // ==========================================================================
  //  Question 2.4 — rapport de qualité
  // ==========================================================================

  /**
   * Construit le DataFrame de synthèse : une ligne par jeu de données, avec le
   * taux de rejet arrondi à deux décimales et le nombre de valeurs nulles.
   * Les trois compteurs d'intégrité référentielle (bonus 2.5) ne sont
   * renseignés que sur la ligne `transactions`.
   */
  def qualityReport(
      spark: SparkSession,
      outcomes: Seq[ValidationOutcome],
      integrity: Option[(Long, Long, Long)]
  ): DataFrame = {
    import spark.implicits._

    val rows = outcomes.map { o =>
      val (u, p, m) =
        if (o.dataset == "transactions") integrity.map { case (a, b, c) => (Some(a), Some(b), Some(c)) }.getOrElse((None, None, None))
        else (None, None, None)

      DataQualityRow(
        dataset                     = o.dataset,
        nb_lignes_lues              = o.nbLues,
        nb_lignes_valides           = o.nbValides,
        nb_lignes_rejetees          = o.nbRejetees,
        taux_rejet                  = o.tauxRejet,
        nb_valeurs_nulles           = o.nbValeursNulles,
        nb_refs_orphelines_user     = u,
        nb_refs_orphelines_product  = p,
        nb_refs_orphelines_merchant = m
      )
    }

    spark.createDataset(rows).toDF()
  }

  /**
   * Détail des motifs de rejet, tous jeux de données confondus.
   * Livrable complémentaire très utile en soutenance : il prouve que les règles
   * attrapent bien les anomalies volontairement injectées dans les fichiers.
   */
  def rejectionBreakdown(spark: SparkSession, outcomes: Seq[ValidationOutcome]): DataFrame = {
    val parts = outcomes.map { o =>
      o.rejected
        .select(lit(o.dataset).as("dataset"), explode(split(col("rejection_reason"), " \\| ")).as("regle"))
    }
    parts
      .reduceOption(_ union _)
      .getOrElse(spark.emptyDataFrame.select(lit("").as("dataset"), lit("").as("regle")))
      .groupBy("dataset", "regle")
      .agg(count(lit(1)).as("nb_lignes"))
      .orderBy(col("dataset"), col("nb_lignes").desc)
  }
}
