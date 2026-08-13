package com.ecommerce.models

/**
 * Modèle de données du projet (Question 2.1).
 *
 * Propriétaire : Membre A (BANIGANTE Kpapou) — Relecteur : Membre B.
 *
 * Convention retenue par le groupe : **tout champ susceptible d'être absent ou
 * invalide dans les fichiers sources est déclaré `Option[...]`**. Sans cela,
 * l'encodeur Spark convertirait silencieusement un `null` numérique en `0`
 * (comportement des types primitifs Scala), ce qui fausserait les règles de
 * validation de la Question 2.2 et le comptage des valeurs nulles de la
 * Question 2.4. Les champs identifiants restent en `String` : un identifiant
 * nul est traité comme une valeur manquante par les règles de validation.
 */

/** transactions.csv — ~138 000 lignes, du 01/01/2024 au 31/12/2025. */
case class Transaction(
    transaction_id: String,
    user_id: String,
    product_id: String,
    merchant_id: String,
    amount: Option[Double],
    timestamp: String, // yyyyMMddHHmmss
    location: String,
    payment_method: String,
    category: String
)

/** users.json — 12 000 lignes, une ligne = un objet JSON. */
case class User(
    user_id: String,
    age: Option[Int],
    annual_income: Option[Double],
    city: String,
    customer_segment: String,
    preferred_categories: Seq[String], // champ imbriqué (tableau JSON)
    registration_date: String          // yyyyMMdd
)

/** products.parquet — 6 000 lignes réparties sur 12 fichiers Parquet. */
case class Product(
    product_id: String,
    name: String,
    category: String,
    price: Option[Double],
    merchant_id: String,
    rating: Option[Double],
    stock: Option[Int]
)

/** merchants.csv — 600 lignes, schéma inféré par Spark. */
case class Merchant(
    merchant_id: String,
    name: String,
    category: String,
    region: String,
    commission_rate: Option[Double],
    establishment_date: String // yyyyMMdd
)

/**
 * Structure renvoyée par l'UDF `extractTimeFeatures` (Question 3.1).
 * Propriétaire : Membre B (CAMARA Oumar).
 */
case class TimeFeatures(
    hour: Int,               // 0-23, -1 si l'horodatage est illisible
    day_of_week: String,     // « Lundi », « Mardi », … ou « Inconnu »
    month: String,           // « Janvier », « Février », … ou « Inconnu »
    is_weekend: Int,         // 1 si samedi ou dimanche, 0 sinon
    day_period: String,      // Morning / Afternoon / Evening / Night / Unknown
    is_working_hours: Int    // 1 si 9h ≤ heure < 17h, 0 sinon
)

/**
 * Une ligne du rapport de qualité des données (Question 2.4).
 * Propriétaire : Membre A (BANIGANTE Kpapou).
 *
 * Les trois derniers champs correspondent au bonus 2.5 (intégrité
 * référentielle) ; ils valent `None` pour les jeux de données autres que
 * `transactions`.
 */
case class DataQualityRow(
    dataset: String,
    nb_lignes_lues: Long,
    nb_lignes_valides: Long,
    nb_lignes_rejetees: Long,
    taux_rejet: Double,
    nb_valeurs_nulles: Long,
    nb_refs_orphelines_user: Option[Long] = None,
    nb_refs_orphelines_product: Option[Long] = None,
    nb_refs_orphelines_merchant: Option[Long] = None
)

/**
 * Résultat d'une étape chronométrée du pipeline (Questions 5.3 et 6.2).
 * Propriétaire : Membre C (CHAKVOURNE Frédéric).
 */
case class StageTiming(
    etape: String,
    mode: String,        // « sans-optimisation » ou « avec-optimisation »
    duree_ms: Long
)
