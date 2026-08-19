package com.ecommerce

import com.ecommerce.analytics.DataValidation
import com.ecommerce.models._
import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Tests unitaires des règles de validation (Partie 8 — Question 2.2).
 *
 * Auteur : Membre A (BANIGANTE Kpapou) — Relecteur : Membre C.
 *
 * On vérifie les deux garanties du « mode groupe » : aucune ligne perdue
 * (valides + rejetées = lues) et présence du motif de rejet.
 */
class DataValidationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private val config = ConfigLoader.load()

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("DataValidationSpec")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  "validateTransactions" should "rejeter les montants négatifs et les horodatages mal formés" in {
    val sp = spark; import sp.implicits._
    val ds = Seq(
      Transaction("TX1", "U1", "P1", "M1", Some(50.0),  "20240701021822", "Paris", "CARD", "Books"),
      Transaction("TX2", "U2", "P2", "M2", Some(-10.0), "20240701021822", "Lyon",  "CASH", "Books"), // montant
      Transaction("TX3", "U3", "P3", "M3", Some(20.0),  "2024070102",     "Nice",  "CARD", "Toys"),  // timestamp
      Transaction("TX4", "U4", "P4", "M4", None,        "20240701021822", "Brest", "CARD", "Toys")   // null
    ).toDS()

    val out = DataValidation.validateTransactions(ds, config)
    out.nbLues shouldBe 4
    out.nbValides shouldBe 1
    out.nbRejetees shouldBe 3
    out.nbValides + out.nbRejetees shouldBe out.nbLues
    out.rejected.columns should contain("rejection_reason")
  }

  "validateUsers" should "rejeter les âges et revenus incohérents" in {
    val sp = spark; import sp.implicits._
    val ds = Seq(
      User("U1", Some(30),  Some(40000.0), "Paris", "Standard", Seq("Books"), "20230101"),
      User("U2", Some(12),  Some(40000.0), "Lyon",  "Standard", Seq("Toys"),  "20230101"), // âge
      User("U3", Some(45),  Some(-5.0),    "Nice",  "Premium",  Seq("Books"), "20230101"), // revenu
      User("U4", None,      Some(40000.0), "Brest", "Budget",   Seq(),        "20230101")  // null
    ).toDS()

    val out = DataValidation.validateUsers(ds, config)
    out.nbValides shouldBe 1
    out.nbRejetees shouldBe 3
  }

  "validateProducts et validateMerchants" should "appliquer les bornes de l'énoncé" in {
    val sp = spark; import sp.implicits._

    val produits = Seq(
      Product("P1", "A", "Books", Some(10.0), "M1", Some(4.5), Some(3)),
      Product("P2", "B", "Books", Some(0.0),  "M1", Some(4.5), Some(3)), // prix
      Product("P3", "C", "Books", Some(10.0), "M1", Some(7.0), Some(3))  // note
    ).toDS()
    DataValidation.validateProducts(produits, config).nbRejetees shouldBe 2

    val marchands = Seq(
      Merchant("M1", "Boutique", "Books", "Bretagne", Some(0.07), "20220821"),
      Merchant("M2", "Boutique", "Books", "Bretagne", Some(1.50), "20220821"), // commission
      Merchant("M3", "Boutique", "Books", "Bretagne", None,       "20220821")  // null
    ).toDS()
    DataValidation.validateMerchants(marchands, config).nbRejetees shouldBe 2
  }

  "countNulls" should "compter les valeurs nulles toutes colonnes confondues" in {
    val sp = spark; import sp.implicits._
    val df = Seq(
      (Some(1), Some("a")),
      (None,    Some("b")),
      (None,    None)
    ).toDF("n", "s")
    DataValidation.countNulls(df) shouldBe 3L
  }
}
