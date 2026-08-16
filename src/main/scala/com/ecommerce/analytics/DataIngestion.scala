package com.ecommerce.analytics

import com.ecommerce.models._
import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

import scala.util.{Failure, Success, Try}

/**
 * Ingestion multi-format (Partie 2 — Questions 2.1 et 2.3).
 *
 * Propriétaire : Membre A (BANIGANTE Kpapou) — Relecteur : Membre B.
 *
 * Quatre sources, quatre stratégies de lecture volontairement différentes,
 * comme demandé par l'énoncé :
 *   - `transactions.csv` : schéma **explicite** (pas d'inférence, donc un seul
 *     passage sur le fichier et un typage garanti et reproductible) ;
 *   - `users.json`       : schéma dérivé de la case class `User`, ce qui gère
 *     nativement le champ imbriqué `preferred_categories: Array[String]` ;
 *   - `products.parquet` : format colonne auto-décrit, on lit puis on aligne
 *     les types sur la case class ;
 *   - `merchants.csv`    : **inférence** de schéma par Spark (fichier de 600
 *     lignes, le double passage de lecture est négligeable).
 *
 * Aucun chemin n'est codé en dur : tout vient de `application.conf`.
 * Toutes les lectures passent par [[readSafely]], qui encapsule le `try-catch`
 * demandé par la Question 2.3 et affiche le nombre de lignes lues.
 */
class DataIngestion(spark: SparkSession, config: ConfigLoader) extends Serializable {

  import spark.implicits._

  // ==========================================================================
  //  Schémas explicites
  // ==========================================================================

  /**
   * Question 2.1 — schéma explicite des transactions.
   * `amount` est déclaré nullable : les montants illisibles du fichier source
   * deviennent `null` (mode PERMISSIVE) au lieu de faire échouer la lecture ;
   * ils seront rejetés par la validation de la Question 2.2.
   */
  val transactionSchema: StructType = StructType(
    Seq(
      StructField("transaction_id", StringType, nullable = true),
      StructField("user_id",        StringType, nullable = true),
      StructField("product_id",     StringType, nullable = true),
      StructField("merchant_id",    StringType, nullable = true),
      StructField("amount",         DoubleType, nullable = true),
      StructField("timestamp",      StringType, nullable = true),
      StructField("location",       StringType, nullable = true),
      StructField("payment_method", StringType, nullable = true),
      StructField("category",       StringType, nullable = true)
    )
  )

  /** Schéma des utilisateurs déduit de la case class (gère le tableau imbriqué). */
  val userSchema: StructType = Encoders.product[User].schema

  // ==========================================================================
  //  Question 2.3 — lecture protégée et journalisée
  // ==========================================================================

  /**
   * Exécute une lecture en capturant toute erreur (fichier introuvable,
   * structure incorrecte, format illisible…) et affiche le nombre de lignes
   * lues avant validation.
   *
   * @return le Dataset lu, ou un Dataset vide si la lecture a échoué — le
   *         pipeline continue alors sur les autres sources au lieu de crasher.
   */
  private def readSafely[T: org.apache.spark.sql.Encoder](
      label: String,
      path: String
  )(read: => Dataset[T]): Dataset[T] = {
    println(s"[INGESTION] Lecture de « $label » depuis : $path")
    Try(read) match {
      case Success(ds) =>
        Try(ds.count()) match {
          case Success(n) =>
            println(f"[INGESTION] OK — $label%-13s : $n%,d lignes lues avant validation")
            ds
          case Failure(e) =>
            println(s"[INGESTION] ERREUR de matérialisation de « $label » : ${e.getClass.getSimpleName} — ${e.getMessage}")
            spark.emptyDataset[T]
        }
      case Failure(e) =>
        println(s"[INGESTION] ERREUR de lecture de « $label » : ${e.getClass.getSimpleName} — ${e.getMessage}")
        println(s"[INGESTION] Vérifiez le chemin « $path » dans application.conf.")
        spark.emptyDataset[T]
    }
  }

  // ==========================================================================
  //  Lectures
  // ==========================================================================

  /** transactions.csv — schéma explicite. */
  def readTransactions(): Dataset[Transaction] =
    readSafely[Transaction]("transactions", config.transactionsPath) {
      spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE") // les lignes malformées deviennent des nulls
        .schema(transactionSchema)
        .csv(config.transactionsPath)
        .as[Transaction]
    }

  /** users.json — un objet JSON par ligne, champ `preferred_categories` imbriqué. */
  def readUsers(): Dataset[User] =
    readSafely[User]("users", config.usersPath) {
      spark.read
        .option("mode", "PERMISSIVE")
        .schema(userSchema) // gère Array[String] sans inférence
        .json(config.usersPath)
        .as[User]
    }

  /** products.parquet — répertoire de 12 fichiers Parquet compressés en snappy. */
  def readProducts(): Dataset[Product] =
    readSafely[Product]("products", config.productsPath) {
      val raw = spark.read.parquet(config.productsPath)
      alignTo(raw, Encoders.product[Product].schema).as[Product]
    }

  /**
   * merchants.csv — schéma inféré par Spark (Question 2.1).
   * L'inférence typerait `establishment_date` en entier (ex. 20220918) : on la
   * réaligne en `String` pour respecter le contrat de la case class et pouvoir
   * la parser en date plus loin dans le pipeline.
   */
  def readMerchants(): Dataset[Merchant] =
    readSafely[Merchant]("merchants", config.merchantsPath) {
      val raw = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(config.merchantsPath)
      alignTo(raw, Encoders.product[Merchant].schema).as[Merchant]
    }

  /**
   * Projette un DataFrame sur le schéma cible d'une case class : sélectionne les
   * colonnes attendues dans l'ordre, les caste au bon type, et matérialise en
   * `null` toute colonne absente du fichier source. Cela rend le pipeline
   * résistant à un fichier dont l'ordre ou le typage des colonnes change.
   */
  private def alignTo(df: DataFrame, target: StructType): DataFrame = {
    val present = df.columns.toSet
    val projected = target.fields.map { f =>
      if (present.contains(f.name)) col(f.name).cast(f.dataType).as(f.name)
      else {
        println(s"[INGESTION] AVERTISSEMENT : colonne « ${f.name} » absente du fichier source → null")
        org.apache.spark.sql.functions.lit(null).cast(f.dataType).as(f.name)
      }
    }
    df.select(projected: _*)
  }

  /** Charge les quatre sources en une fois. */
  def loadAll(): RawDatasets =
    RawDatasets(
      transactions = readTransactions(),
      users        = readUsers(),
      products     = readProducts(),
      merchants    = readMerchants()
    )
}

/** Conteneur des quatre jeux de données bruts. */
case class RawDatasets(
    transactions: Dataset[Transaction],
    users: Dataset[User],
    products: Dataset[Product],
    merchants: Dataset[Merchant]
)
