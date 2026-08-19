package com.ecommerce.analytics

import com.ecommerce.models.{TimeFeatures => TF}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * UDF `extractTimeFeatures` (Partie 3 — Question 3.1).
 *
 * Propriétaire : Membre B (CAMARA Oumar) — Relecteur : Membre A.
 *
 * L'UDF prend un horodatage au format `yyyyMMddHHmmss` et renvoie une structure
 * à six champs (voir [[com.ecommerce.models.TimeFeatures]]).
 *
 * **Robustesse** : l'énoncé impose qu'une chaîne nulle, vide ou mal formée ne
 * fasse pas échouer le job. La fonction ne lève donc jamais d'exception : elle
 * renvoie une structure « Inconnu » (`hour = -1`, `day_period = "Unknown"`),
 * ce qui rend les lignes fautives visibles dans les agrégats au lieu de les
 * masquer derrière un `null`.
 *
 * Conventions documentées (l'énoncé laisse une marge d'interprétation) :
 *   - `day_period` : Morning [6h ; 12h[, Afternoon [12h ; 18h[,
 *     Evening [18h ; 22h[, Night [22h ; 6h[ — les heures de nuit d'avant 6h
 *     sont donc bien classées « Night » ;
 *   - `is_working_hours` : 1 si 9 ≤ heure < 17 (journée de travail 9h-17h).
 */
object TimeFeatures extends Serializable {

  private val Format = "yyyyMMddHHmmss"

  /** Le formateur n'est pas sérialisable : il est reconstruit dans chaque exécuteur. */
  @transient private lazy val formatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern(Format)

  private val JoursFr = Array(
    "Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"
  )

  private val MoisFr = Array(
    "Janvier", "Fevrier", "Mars", "Avril", "Mai", "Juin",
    "Juillet", "Aout", "Septembre", "Octobre", "Novembre", "Decembre"
  )

  /** Valeur renvoyée lorsque l'horodatage est inexploitable. */
  val Unknown: TF =
    TF(-1, "Inconnu", "Inconnu", 0, "Unknown", 0)

  /** Étiquette de la période de la journée à partir de l'heure (0-23). */
  def dayPeriod(hour: Int): String = hour match {
    case h if h >= 6  && h < 12 => "Morning"
    case h if h >= 12 && h < 18 => "Afternoon"
    case h if h >= 18 && h < 22 => "Evening"
    case h if h >= 0  && h < 24 => "Night" // 22h-24h et 0h-6h
    case _                      => "Unknown"
  }

  /**
   * Cœur de l'UDF, exposé séparément pour être testable unitairement sans
   * SparkSession (Partie 8).
   */
  def extract(ts: String): TF = {
    if (ts == null) return Unknown
    val cleaned = ts.trim
    if (cleaned.length != 14 || !cleaned.forall(_.isDigit)) return Unknown

    try {
      val dt    = LocalDateTime.parse(cleaned, formatter)
      val hour  = dt.getHour
      val dow   = dt.getDayOfWeek.getValue // 1 = lundi … 7 = dimanche

      TF(
        hour             = hour,
        day_of_week      = JoursFr(dow - 1),
        month            = MoisFr(dt.getMonthValue - 1),
        is_weekend       = if (dow >= 6) 1 else 0,
        day_period       = dayPeriod(hour),
        is_working_hours = if (hour >= 9 && hour < 17) 1 else 0
      )
    } catch {
      // Date syntaxiquement numérique mais impossible (ex. 20241332...) :
      // on dégrade proprement au lieu de faire tomber la tâche Spark.
      case _: Throwable => Unknown
    }
  }

  /** UDF Spark prête à l'emploi : `df.withColumn("tf", TimeFeatures.extractTimeFeatures($"timestamp"))`. */
  val extractTimeFeatures: UserDefinedFunction = udf((ts: String) => extract(ts))
}
