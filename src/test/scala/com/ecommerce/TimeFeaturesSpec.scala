package com.ecommerce

import com.ecommerce.analytics.TimeFeatures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Tests unitaires de l'UDF `extractTimeFeatures` (Partie 8).
 *
 * Auteur : Membre B (CAMARA Oumar) — Relecteur : Membre A.
 *
 * Ces tests ne nécessitent pas de SparkSession : la logique de l'UDF est
 * isolée dans `TimeFeatures.extract`, ce qui les rend instantanés.
 */
class TimeFeaturesSpec extends AnyFlatSpec with Matchers {

  "extractTimeFeatures" should "décoder un horodatage valide en semaine" in {
    // Lundi 1er juillet 2024, 02h18 → nuit, hors heures ouvrées.
    val tf = TimeFeatures.extract("20240701021822")
    tf.hour shouldBe 2
    tf.day_of_week shouldBe "Lundi"
    tf.month shouldBe "Juillet"
    tf.is_weekend shouldBe 0
    tf.day_period shouldBe "Night"
    tf.is_working_hours shouldBe 0
  }

  it should "reconnaître le week-end" in {
    // Samedi 6 juillet 2024, 14h30.
    val tf = TimeFeatures.extract("20240706143000")
    tf.day_of_week shouldBe "Samedi"
    tf.is_weekend shouldBe 1
    tf.day_period shouldBe "Afternoon"
  }

  it should "marquer les heures ouvrées" in {
    TimeFeatures.extract("20240703090000").is_working_hours shouldBe 1
    TimeFeatures.extract("20240703163000").is_working_hours shouldBe 1
    TimeFeatures.extract("20240703170000").is_working_hours shouldBe 0
    TimeFeatures.extract("20240703085959").is_working_hours shouldBe 0
  }

  it should "classer correctement les quatre périodes de la journée" in {
    TimeFeatures.dayPeriod(6)  shouldBe "Morning"
    TimeFeatures.dayPeriod(11) shouldBe "Morning"
    TimeFeatures.dayPeriod(12) shouldBe "Afternoon"
    TimeFeatures.dayPeriod(17) shouldBe "Afternoon"
    TimeFeatures.dayPeriod(18) shouldBe "Evening"
    TimeFeatures.dayPeriod(21) shouldBe "Evening"
    TimeFeatures.dayPeriod(22) shouldBe "Night"
    TimeFeatures.dayPeriod(0)  shouldBe "Night"
    TimeFeatures.dayPeriod(5)  shouldBe "Night"
  }

  it should "ne jamais échouer sur une entrée invalide" in {
    val invalides = Seq(null, "", "   ", "2024", "abcdefghijklmn", "20241332000000", "2024070102182")
    invalides.foreach { ts =>
      val tf = TimeFeatures.extract(ts)
      tf shouldBe TimeFeatures.Unknown
    }
  }
}
