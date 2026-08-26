package com.ecommerce.analytics

import com.ecommerce.models.StageTiming
import com.ecommerce.utils.{ConfigLoader, DataWriter, SparkSessionBuilder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

/**
 * Application principale — orchestration du pipeline
 * (Partie 6 — Question 6.1 et bonus 6.2).
 *
 * Propriétaire : Membre C (CHAKVOURNE Frédéric) — Intégration validée par les
 * trois membres.
 *
 * Usage :
 * {{{
 *   spark-submit --class com.ecommerce.analytics.MainApp app.jar all
 *   spark-submit --class com.ecommerce.analytics.MainApp app.jar ingestion
 *   spark-submit --class com.ecommerce.analytics.MainApp app.jar transformation
 *   spark-submit --class com.ecommerce.analytics.MainApp app.jar analytics
 *   spark-submit --class com.ecommerce.analytics.MainApp app.jar benchmark
 * }}}
 */
object MainApp {

  private val EtapesValides = Seq("all", "ingestion", "transformation", "analytics", "benchmark", "help")

  def main(args: Array[String]): Unit = {

    val etape = args.headOption.map(_.trim.toLowerCase).getOrElse("all")

    if (etape == "help" || !EtapesValides.contains(etape)) {
      afficherAide(etape)
      // Un argument inconnu n'est pas une exception : on informe et on sort
      // proprement avec un code d'erreur exploitable par un ordonnanceur.
      System.exit(if (etape == "help") 0 else 2)
    }

    val config = ConfigLoader.load()
    banner(s"DÉMARRAGE — étape demandée : « $etape »")
    config.printSummary()

    var spark: SparkSession = null

    // Question 6.1 — gestion d'erreurs globale garantissant un arrêt propre.
    try {
      spark = SparkSessionBuilder.build(config)
      println(s"[MAIN] SparkSession initialisée — version ${spark.version}, master ${config.sparkMaster}")

      if (etape == "benchmark" || config.benchmark) executerBenchmark(spark, config)
      else new Pipeline(spark, config).executer(etape)

      banner("PIPELINE TERMINÉ AVEC SUCCÈS")

    } catch {
      case e: Throwable =>
        println("=" * 100)
        println(s"[MAIN] ÉCHEC DU PIPELINE : ${e.getClass.getName}")
        println(s"[MAIN] Message : ${e.getMessage}")
        e.getStackTrace.take(12).foreach(l => println(s"        at $l"))
        println("=" * 100)
        if (spark != null) Try(spark.stop())
        System.exit(1)

    } finally {
      // Arrêt propre de la SparkSession dans tous les cas.
      if (spark != null) {
        Try(spark.stop()) match {
          case Success(_) => println("[MAIN] SparkSession arrêtée proprement.")
          case Failure(e) => println(s"[MAIN] Avertissement à l'arrêt de Spark : ${e.getMessage}")
        }
      }
    }
  }

  // ==========================================================================
  //  Bonus 5.3 — exécution comparative sans / avec optimisations
  // ==========================================================================

  private def executerBenchmark(spark: SparkSession, config: ConfigLoader): Unit = {
    banner("BONUS 5.3 — MESURE DU GAIN APPORTÉ PAR LES OPTIMISATIONS")

    println("\n>>> PASSE 1/2 : pipeline SANS cache ni broadcast\n")
    val sansConfig = config.withOptimizations(false)
    spark.catalog.clearCache()
    // Les deux passes écrivent réellement les résultats : l'étape « ecriture »
    // fait partie du comparatif demandé, et la seconde passe écrase la première.
    val sans = new Pipeline(spark, sansConfig).executer("all", ecrire = true)

    println("\n>>> PASSE 2/2 : pipeline AVEC cache et broadcast\n")
    val avecConfig = config.withOptimizations(true)
    spark.catalog.clearCache()
    val avec = new Pipeline(spark, avecConfig).executer("all", ecrire = true)

    val comparaison = SparkOptimizations.comparison(spark, sans, avec)
    banner("TABLEAU COMPARATIF — À REPORTER DANS LE README")
    comparaison.show(false)
    DataWriter.write(comparaison, "05_benchmark_optimisations", config)
  }

  // ==========================================================================
  //  Aide (bonus 6.2)
  // ==========================================================================

  private def afficherAide(argument: String): Unit = {
    if (argument != "help") println(s"\n[MAIN] Argument inconnu : « $argument ».\n")
    println(
      s"""
         |Usage : spark-submit --class com.ecommerce.analytics.MainApp <jar> [étape]
         |
         |Étapes acceptées :
         |  ingestion       Lecture des 4 sources, validation, rapport de qualité
         |  transformation  Ingestion + UDF temporelle, jointures et fenêtrages
         |  analytics       Pipeline complet jusqu'aux KPI métier
         |  all             Tout le pipeline (valeur par défaut)
         |  benchmark       Exécute le pipeline sans puis avec optimisations (bonus 5.3)
         |  help            Affiche ce message
         |
         |Exemple : spark-submit --class com.ecommerce.analytics.MainApp \\
         |            target/scala-2.12/EcommerceAnalytics-assembly-1.0.0.jar all
         |""".stripMargin
    )
  }

  private def banner(titre: String): Unit = {
    println()
    println("#" * 100)
    println(s"#  $titre")
    println("#" * 100)
    println()
  }
}

/**
 * Enchaînement des phases du pipeline. Isolé de `main` afin de pouvoir être
 * exécuté deux fois de suite (bonus 5.3) avec deux configurations différentes.
 */
class Pipeline(spark: SparkSession, config: ConfigLoader) {

  private val optim = new SparkOptimizations(spark, config)

  /**
   * @param etape  ingestion | transformation | analytics | all
   * @param ecrire écrit les résultats sur disque (désactivé pour la passe de
   *               mesure « sans optimisation » du bonus 5.3)
   * @return les durées mesurées par étape
   */
  def executer(etape: String, ecrire: Boolean = true): Seq[StageTiming] = {
    optim.clearTimings()

    // ------------------------------------------------------------------
    // PHASE 1 — INGESTION ET VALIDATION (Partie 2)
    // ------------------------------------------------------------------
    val valides = optim.timed("ingestion") { ingestionEtValidation(ecrire) }

    if (etape == "ingestion") {
      optim.printCacheState()
      optim.unpersistAll()
      return optim.allTimings
    }

    // ------------------------------------------------------------------
    // PHASE 2 — TRANSFORMATIONS (Partie 3)
    // ------------------------------------------------------------------
    val transformation = new DataTransformation(spark, config)
    val enrichi = optim.timed("transformation") {
      val df = optim.persistIfEnabled(
        transformation.run(valides.transactions, valides.users, valides.products, valides.merchants),
        "transactions_enrichies"
      )
      println(f"[TRANSFORMATION] ${df.count()}%,d transactions enrichies (${df.columns.length} colonnes)")
      df
    }

    afficherApercuTransformation(transformation, enrichi)

    if (etape == "transformation") {
      if (ecrire) ecrireEchantillon(enrichi)
      optim.printCacheState()
      optim.unpersistAll()
      return optim.allTimings
    }

    // ------------------------------------------------------------------
    // PHASE 3 — ANALYTIQUE (Partie 4)
    // ------------------------------------------------------------------
    val resultats = optim.timed("analytique") { analytique(enrichi) }

    // ------------------------------------------------------------------
    // PHASE 4 — ÉCRITURE (Question 6.1)
    // ------------------------------------------------------------------
    if (ecrire) {
      optim.timed("ecriture") {
        ecrireEchantillon(enrichi)
        resultats.foreach { case (nom, df) => DataWriter.write(df, nom, config) }
      }
    }

    optim.printCacheState()
    optim.unpersistAll()
    afficherTimings()
    optim.allTimings
  }

  // ==========================================================================
  //  PHASE 1
  // ==========================================================================

  private case class Valides(
      transactions: DataFrame,
      users: DataFrame,
      products: DataFrame,
      merchants: DataFrame
  )

  private def ingestionEtValidation(ecrire: Boolean): Valides = {
    titre("PARTIE 2 — INGESTION ET VALIDATION DES DONNÉES")

    val ingestion = new DataIngestion(spark, config)
    val brut      = ingestion.loadAll()

    // Les sources brutes sont relues par la validation (comptages) puis par les
    // jointures : elles sont donc mises en cache (Question 5.1).
    val txRaw = optim.persistIfEnabled(brut.transactions.toDF(), "transactions_brutes")
    val usRaw = optim.cacheIfEnabled(brut.users.toDF(),          "users_bruts")
    val prRaw = optim.cacheIfEnabled(brut.products.toDF(),       "products_bruts")
    val meRaw = optim.cacheIfEnabled(brut.merchants.toDF(),      "merchants_bruts")

    import spark.implicits._
    val oTx = DataValidation.validateTransactions(txRaw.as[com.ecommerce.models.Transaction], config)
    val oUs = DataValidation.validateUsers(usRaw.as[com.ecommerce.models.User], config)
    val oPr = DataValidation.validateProducts(prRaw.as[com.ecommerce.models.Product], config)
    val oMe = DataValidation.validateMerchants(meRaw.as[com.ecommerce.models.Merchant], config)

    val outcomes = Seq(oTx, oUs, oPr, oMe)
    println()
    outcomes.foreach(_.print())

    val txValides = optim.persistIfEnabled(oTx.valid, "transactions_valides")
    val usValides = optim.cacheIfEnabled(oUs.valid,   "users_valides")
    val prValides = optim.cacheIfEnabled(oPr.valid,   "products_valides")
    val meValides = optim.cacheIfEnabled(oMe.valid,   "merchants_valides")

    // Bonus 2.5 — intégrité référentielle.
    val integrite = DataValidation.referentialIntegrity(txValides, usValides, prValides, meValides)
    println(f"\n[BONUS 2.5] Références orphelines dans les transactions valides :")
    println(f"            user_id inexistant     : ${integrite._1}%,d")
    println(f"            product_id inexistant  : ${integrite._2}%,d")
    println(f"            merchant_id inexistant : ${integrite._3}%,d")

    // Question 2.4 — rapport de qualité.
    val rapport = DataValidation.qualityReport(spark, outcomes, Some(integrite))
    titre("QUESTION 2.4 — RAPPORT DE QUALITÉ DES DONNÉES")
    rapport.show(false)

    val motifs = DataValidation.rejectionBreakdown(spark, outcomes)
    titre("DÉTAIL DES MOTIFS DE REJET")
    motifs.show(50, truncate = false)

    if (ecrire) {
      DataWriter.write(rapport, "01_rapport_qualite", config)
      DataWriter.write(motifs,  "01_motifs_rejet", config)
      DataWriter.write(
        oTx.rejected.limit(config.outputSampleSize),
        "01_transactions_rejetees_echantillon",
        config
      )
    }

    // Les DataFrame bruts ne servent plus : on libère explicitement (Q5.1).
    Seq("transactions_brutes", "users_bruts", "products_bruts", "merchants_bruts").foreach(optim.unpersist)

    Valides(txValides, usValides, prValides, meValides)
  }

  // ==========================================================================
  //  PHASE 2 — aperçus console
  // ==========================================================================

  private def afficherApercuTransformation(t: DataTransformation, enrichi: DataFrame): Unit = {
    titre("QUESTION 3.1 / 3.2 — TRANSACTIONS ENRICHIES (extrait)")
    enrichi
      .select(
        "transaction_id", "user_id", "timestamp", "amount", "day_of_week", "month_name",
        "day_period", "is_weekend", "is_working_hours", "age_group",
        "transaction_rank", "user_total_transactions"
      )
      .show(10, truncate = false)

    titre("QUESTION 3.3 — FENÊTRES GLISSANTES (extrait)")
    enrichi
      .select(
        "user_id", "tx_date", "amount", "rolling_amount_7d", "distinct_days_7d",
        "is_active_user", "days_since_previous_purchase"
      )
      .orderBy(col("user_id"), col("tx_date"))
      .show(15, truncate = false)

    println(f"[3.3] Transactions marquées « utilisateur actif » : ${enrichi.filter(col("is_active_user") === 1).count()}%,d")

    if (enrichi.columns.contains("is_suspicious")) t.showSuspiciousSummary(enrichi)
  }

  // ==========================================================================
  //  PHASE 3
  // ==========================================================================

  private def analytique(enrichi: DataFrame): Seq[(String, DataFrame)] = {
    val analytics = new Analytics(spark, config)

    // --- Question 4.1 ------------------------------------------------------
    titre("QUESTION 4.1 — RAPPORT DÉTAILLÉ PAR MARCHAND (top 20)")
    val marchands = analytics.merchantReport(enrichi)
    marchands.show(20, truncate = false)

    // --- Question 4.2 ------------------------------------------------------
    titre("QUESTION 4.2 — ANALYSE DE COHORTES")
    val cohortes = analytics.cohortAnalysis(enrichi)
    println("Taille initiale de chaque cohorte :")
    cohortes.cohortSizes.show(30, truncate = false)
    println("Matrice de rétention (taux en %, colonnes = mois depuis l'acquisition) :")
    val matrice = analytics.retentionMatrix(cohortes.retention)
    matrice.show(30, truncate = false)
    println(s"Meilleure rétention à ${config.retentionFocusPeriod} mois :")
    cohortes.bestAtFocus.show(5, truncate = false)

    // --- Bonus 4.3 ---------------------------------------------------------
    titre("BONUS 4.3 — SEGMENTATION RFM")
    val rfm = analytics.rfmSegmentation(enrichi)
    rfm.distribution.show(false)
    println("Croisement segment RFM calculé × customer_segment déclaré :")
    rfm.crossTab.show(false)

    // --- Bonus 4.4 ---------------------------------------------------------
    titre("BONUS 4.4 — ANALYSE PRODUITS ET CATÉGORIES")
    val produits = analytics.productAnalysis(enrichi)
    println(s"Top ${config.topProducts} produits par chiffre d'affaires :")
    produits.topProduits.show(false)
    println("Chiffre d'affaires par catégorie et par région :")
    produits.parCategorieRegion.show(20, truncate = false)
    println("Chiffre d'affaires par méthode de paiement et période de la journée :")
    produits.parPaiementPeriode.show(30, truncate = false)

    Seq(
      "02_kpi_marchands"            -> marchands,
      "03_cohortes_tailles"         -> cohortes.cohortSizes,
      "03_cohortes_retention"       -> cohortes.retention,
      "03_cohortes_matrice"         -> matrice,
      "03_cohortes_meilleure_3mois" -> cohortes.bestAtFocus,
      "04_rfm_scores"               -> rfm.scores,
      "04_rfm_distribution"         -> rfm.distribution,
      "04_rfm_croisement"           -> rfm.crossTab,
      "04_top_produits"             -> produits.topProduits,
      "04_ca_categorie_region"      -> produits.parCategorieRegion,
      "04_ca_paiement_periode"      -> produits.parPaiementPeriode
    )
  }

  // ==========================================================================
  //  Écriture de l'échantillon enrichi
  // ==========================================================================

  private def ecrireEchantillon(enrichi: DataFrame): Unit = {
    val echantillon = enrichi.orderBy(col("transaction_id")).limit(config.outputSampleSize)
    DataWriter.write(echantillon, "00_transactions_enrichies_echantillon", config)
  }

  private def afficherTimings(): Unit = {
    titre("DURÉES D'EXÉCUTION PAR ÉTAPE")
    optim.timingsDataFrame().show(false)
  }

  private def titre(t: String): Unit = {
    println()
    println("=" * 100)
    println(s"  $t")
    println("=" * 100)
  }
}
