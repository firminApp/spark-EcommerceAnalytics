# EQUIPE.md — GROUPE 3

**Projet Final — Data Engineer : Spark & Scala**
Système d'analyse de données e-commerce distribué

---

## 1. Composition du groupe et rôles

| Rôle | Nom, prénom | Adresse e-mail | Nom configuré dans Git (`git config user.name`) |
|---|---|---|---|
| **Membre A** — Data Ingestion & Platform Engineer | BANIGANTE Kpapou | kpapou.banigante@gozem.co | `Kpapou BANIGANTE` |
| **Membre B** — Data Transformation Engineer | CAMARA Oumar | ⚠️ **À COMPLÉTER AVANT L'ENVOI** | `Oumar CAMARA` |
| **Membre C** — Analytics & Performance Engineer | CHAKVOURNE Frédéric | ⚠️ **À COMPLÉTER AVANT L'ENVOI** | `Frederic CHAKVOURNE` |

> ⚠️ **Action requise avant l'envoi du ZIP** : remplacer les deux mentions
> « À COMPLÉTER AVANT L'ENVOI » par les adresses e-mail réelles de Oumar et
> Frédéric. L'énoncé (Question 0.1) impose la présence des trois adresses.

Un même rôle n'est occupé que par une seule personne, conformément à la
Question 0.1.

---

## 2. Questions traitées par membre

### Membre A — BANIGANTE Kpapou
*Data Ingestion & Platform Engineer — Parties 1, 2 et 7*

| Question | Intitulé | Fichiers dont il est propriétaire |
|---|---|---|
| 1.1 | Structure de projet SBT | arborescence complète du projet |
| 1.2 | Configuration de `build.sbt` | `build.sbt`, `project/plugins.sbt`, `project/build.properties` |
| 1.3 | Documentation `README.md` | `README.md` |
| 2.1 | Ingestion multi-format | `src/main/scala/com/ecommerce/analytics/DataIngestion.scala`, `src/main/scala/com/ecommerce/models/Models.scala` |
| 2.2 | Validation des données (+ lignes rejetées et `rejection_reason`) | `src/main/scala/com/ecommerce/analytics/DataValidation.scala` |
| 2.3 | Gestion d'erreurs et résumé | `DataIngestion.scala` (méthode `readSafely`) |
| 2.4 | Rapport de qualité des données | `DataValidation.scala` (`qualityReport`, `rejectionBreakdown`) |
| **2.5 (bonus)** | Intégrité référentielle | `DataValidation.scala` (`referentialIntegrity`) |
| 7.1 | Fichier `application.conf` et valeurs par défaut | `src/main/resources/application.conf`, `src/main/scala/com/ecommerce/utils/ConfigLoader.scala`, `SparkSessionBuilder.scala` |
| 0.1 / 0.3 | `EQUIPE.md`, `.gitignore` | `EQUIPE.md`, `.gitignore` |
| 8 | Tests unitaires de la validation | `src/test/scala/com/ecommerce/DataValidationSpec.scala` |

### Membre B — CAMARA Oumar
*Data Transformation Engineer — Partie 3*

| Question | Intitulé | Fichiers dont il est propriétaire |
|---|---|---|
| 3.1 | UDF `extractTimeFeatures` | `src/main/scala/com/ecommerce/analytics/TimeFeatures.scala` |
| 3.2 | Fonction `enrichTransactionData` (jointures, UDF, fenêtrage, tranche d'âge) | `src/main/scala/com/ecommerce/analytics/DataTransformation.scala` |
| 3.3 | Analyse par partition `Window` (montant cumulé 7 jours, utilisateur actif, délai entre achats) | `DataTransformation.scala` (`addWindowAnalytics`) |
| **3.4 (bonus)** | Détection de transactions suspectes | `DataTransformation.scala` (`detectSuspiciousTransactions`) |
| 8 | Tests unitaires de l'UDF | `src/test/scala/com/ecommerce/TimeFeaturesSpec.scala` |

### Membre C — CHAKVOURNE Frédéric
*Analytics & Performance Engineer — Parties 4, 5 et 6*

| Question | Intitulé | Fichiers dont il est propriétaire |
|---|---|---|
| 4.1 | Rapport détaillé par marchand | `src/main/scala/com/ecommerce/analytics/Analytics.scala` (`merchantReport`) |
| 4.2 | Analyse de cohortes utilisateurs | `Analytics.scala` (`cohortAnalysis`, `retentionMatrix`) |
| **4.3 (bonus)** | Segmentation RFM des clients | `Analytics.scala` (`rfmSegmentation`) |
| **4.4 (bonus)** | Analyse produits et catégories | `Analytics.scala` (`productAnalysis`) |
| 5.1 | Optimisation du stockage (`cache`, `persist`, `unpersist`) | `src/main/scala/com/ecommerce/analytics/SparkOptimizations.scala` |
| 5.2 | Optimisation des jointures (`broadcast`, `shuffle.partitions`) | `SparkOptimizations.scala`, `DataTransformation.maybeBroadcast` |
| **5.3 (bonus)** | Mesure du gain apporté par les optimisations | `SparkOptimizations.comparison`, `MainApp.executerBenchmark` |
| 6.1 | Application principale `EcommerceAnalyticsApp` | `src/main/scala/com/ecommerce/analytics/MainApp.scala` |
| **6.2 (bonus)** | Exécution modulaire par étape | `MainApp.scala` (parsing des arguments, aide, chronométrage) |
| 6.1 | Écriture des résultats CSV + Parquet | `src/main/scala/com/ecommerce/utils/DataWriter.scala` |

### Travail collectif
Parties 8 et 9 (tests, qualité, documentation, soutenance) : chaque membre
contribue pour la portion de code dont il est propriétaire. Les relectures
croisées sont consignées dans `CONTRIBUTIONS.md`.

---

## 3. Traçabilité Git

Le dépôt Git local se trouve à la racine du projet. Chaque membre a configuré
son identité sur son poste avant de commiter :

```bash
git config user.name  "Kpapou BANIGANTE"
git config user.email "kpapou.banigante@gozem.co"
```

Vérification de l'historique :

```bash
git log --oneline
git shortlog -sn        # nombre de commits par auteur
```

Chaque membre a réalisé au minimum 4 commits portant uniquement sur ses propres
fichiers, conformément au découpage ci-dessus (Question 0.2).
