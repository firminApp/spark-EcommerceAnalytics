# EcommerceAnalytics — Système d'analyse de données e-commerce distribué

**Projet Final Data Engineer — Spark & Scala — GROUPE 6**
BANIGANTE Kpapou · CAMARA Oumar · CHAKVOURNE Frédéric

Pipeline Spark complet : ingestion multi-format (CSV / JSON / Parquet),
validation avec traçabilité des rejets, enrichissement par UDF et fonctions de
fenêtrage, KPI marchands, analyse de cohortes, segmentation RFM, et
optimisations Spark mesurées.

---

## 1. Prérequis

| Composant | Version retenue | Vérification |
|---|---|---|
| **JDK** | 11 ou 17 (LTS) | `java -version` |
| **Scala** | 2.12.18 (téléchargé automatiquement par SBT) | `scala -version` |
| **SBT** | 1.10.7 | `sbt --version` |
| **Apache Spark** | 3.5.1 (build `hadoop3`) — *nécessaire uniquement pour `spark-submit`* | `spark-submit --version` |

> Scala et les dépendances Spark sont récupérées automatiquement par SBT :
> **aucune installation manuelle de Scala n'est nécessaire** pour compiler et
> exécuter le projet en local. Une distribution Spark n'est requise que pour le
> déploiement via `spark-submit`.

### Installation (macOS / Linux)

```bash
# JDK 17 et SBT via Homebrew (macOS)
brew install openjdk@17 sbt

# Linux (Debian / Ubuntu)
sudo apt-get install -y openjdk-17-jdk
curl -sL https://github.com/sbt/sbt/releases/download/v1.10.7/sbt-1.10.7.tgz | tar xz -C /opt
export PATH=/opt/sbt/bin:$PATH

# Distribution Spark (uniquement pour spark-submit)
curl -sL https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz | tar xz -C /opt
export SPARK_HOME=/opt/spark-3.5.1-bin-hadoop3
export PATH=$SPARK_HOME/bin:$PATH
```

> **JDK 17+** : Spark accède par réflexion à des modules internes de la JVM.
> Les options `--add-opens` nécessaires sont **déjà déclarées dans
> `build.sbt`** (`sparkJvmOptions`) pour `sbt run` et `sbt test`. Pour
> `spark-submit`, voir la section 5.

---

## 2. Structure du projet

```
EcommerceAnalytics/
├── build.sbt                       Dépendances, JAR exécutable, options JVM   (Membre A)
├── README.md                       Ce fichier                                  (Membre A)
├── EQUIPE.md                       Rôles, e-mails, identités Git               (Membre A)
├── CONTRIBUTIONS.md                Journal de contribution et relectures       (collectif)
├── .gitignore
├── project/
│   ├── build.properties            Version de SBT
│   └── plugins.sbt                 sbt-assembly
└── src/
    ├── main/scala/com/ecommerce/
    │   ├── analytics/
    │   │   ├── DataIngestion.scala        Lecture des 4 sources          (Membre A)
    │   │   ├── DataValidation.scala       Règles + rapport qualité       (Membre A)
    │   │   ├── TimeFeatures.scala         UDF extractTimeFeatures        (Membre B)
    │   │   ├── DataTransformation.scala   Jointures et fenêtrages        (Membre B)
    │   │   ├── Analytics.scala            KPI, cohortes, RFM, produits   (Membre C)
    │   │   ├── SparkOptimizations.scala   cache / persist / broadcast    (Membre C)
    │   │   └── MainApp.scala              Orchestration du pipeline      (Membre C)
    │   ├── models/Models.scala            Case classes                   (Membre A)
    │   └── utils/
    │       ├── ConfigLoader.scala         Configuration externalisée     (Membre A)
    │       ├── SparkSessionBuilder.scala  Construction de la session     (Membre A)
    │       └── DataWriter.scala           Écriture CSV + Parquet         (Membre C)
    ├── main/resources/
    │   ├── application.conf               Toute la configuration         (Membre A)
    │   └── data/                          transactions.csv, users.json,
    │                                      products.parquet/, merchants.csv
    └── test/scala/com/ecommerce/
        ├── DataValidationSpec.scala       Tests des règles               (Membre A)
        └── TimeFeaturesSpec.scala         Tests de l'UDF                 (Membre B)
```

---

## 3. Compilation

```bash
# Compilation des sources
sbt compile

# Tests unitaires (9 tests)
sbt test

# Génération du JAR exécutable (sbt-assembly)
sbt assembly
# → target/scala-2.12/EcommerceAnalytics-assembly-1.0.0.jar   (~6 Mo)
```

Le JAR est un *assembly* : il embarque la bibliothèque standard Scala et
Typesafe Config, mais **pas Spark** (dépendance `Provided`), qui est fourni par
le cluster. Les jeux de données ne sont pas non plus embarqués : ils sont lus
depuis le système de fichiers via `application.conf`.

---

## 4. Exécution locale (SBT)

```bash
sbt "run all"              # pipeline complet (valeur par défaut)
sbt "run ingestion"        # ingestion + validation + rapport qualité
sbt "run transformation"   # + UDF temporelle, jointures, fenêtrages
sbt "run analytics"        # + KPI marchands, cohortes, RFM, produits
sbt "run benchmark"        # bonus 5.3 : pipeline sans puis avec optimisations
sbt "run help"             # liste des étapes acceptées
```

Un argument inconnu affiche l'aide et sort avec le code 2, sans lever
d'exception non gérée (bonus 6.2).

Les résultats sont écrits dans `output/csv/` et `output/parquet/`.

### Surcharger la configuration sans recompiler

```bash
# Fichier de configuration alternatif
sbt -Dconfig.file=/chemin/prod.conf "run all"

# Surcharge d'une clé isolée
sbt -Dapp.spark.shuffle.partitions=16 "run all"
sbt -Dapp.optimization.enable-cache=false "run all"
```

---

## 5. Déploiement (`spark-submit`)

### Mode local

```bash
spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master "local[*]" \
  --driver-memory 4g \
  target/scala-2.12/EcommerceAnalytics-assembly-1.0.0.jar all
```

### Sur un cluster YARN

```bash
spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 4 \
  --executor-cores 2 \
  --executor-memory 4g \
  --driver-memory 2g \
  --files /chemin/application.conf \
  --driver-java-options "-Dconfig.file=application.conf -Dfile.encoding=UTF-8" \
  --conf spark.executor.extraJavaOptions="-Dfile.encoding=UTF-8" \
  EcommerceAnalytics-assembly-1.0.0.jar all
```

> Sur **JDK 17+**, ajouter les options d'ouverture de modules :
> ```bash
> --driver-java-options "--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED"
> --conf spark.executor.extraJavaOptions="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED"
> ```
> Ces options ne sont **pas** nécessaires sur JDK 8 ou 11.

Sur cluster, remplacer dans `application.conf` les chemins relatifs par des URI
HDFS/S3 (`hdfs:///data/transactions.csv`) et `app.spark.master` par `yarn`.

---

## 6. Résultats produits

Chaque résultat est écrit **en CSV** (lisible par une équipe métier) **et en
Parquet** (typé, compressé, réexploitable par un autre job).

| Répertoire | Contenu | Question |
|---|---|---|
| `00_transactions_enrichies_echantillon` | Extrait des transactions enrichies (49 colonnes) | 3.1 → 3.4 |
| `01_rapport_qualite` | Synthèse qualité, une ligne par dataset | 2.4 / 2.5 |
| `01_motifs_rejet` | Nombre de lignes par règle violée | 2.2 |
| `01_transactions_rejetees_echantillon` | Lignes rejetées avec `rejection_reason` | 2.2 |
| `02_kpi_marchands` | CA, volumétrie, panier moyen, commission, rangs, ventes par tranche d'âge, taux de suspicion | 4.1 |
| `03_cohortes_*` | Tailles de cohortes, matrice de rétention, meilleure cohorte à 3 mois | 4.2 |
| `04_rfm_*` | Scores RFM, distribution des segments, croisement avec `customer_segment` | 4.3 |
| `04_top_produits`, `04_ca_categorie_region`, `04_ca_paiement_periode` | Analyse produits et catégories | 4.4 |
| `05_benchmark_optimisations` | Comparatif sans / avec optimisations | 5.3 |

### Rapport de qualité obtenu sur les données fournies

| dataset | lues | valides | rejetées | taux de rejet | valeurs nulles |
|---|---:|---:|---:|---:|---:|
| transactions | 138 047 | 136 157 | 1 890 | **1,37 %** | 940 |
| users | 12 000 | 11 655 | 345 | **2,88 %** | 160 |
| products | 6 000 | 5 821 | 179 | **2,98 %** | 55 |
| merchants | 600 | 586 | 14 | **2,33 %** | 9 |

Détail des motifs de rejet :

| dataset | règle violée | lignes |
|---|---|---:|
| transactions | montant ≤ 0 | 1 200 |
| transactions | timestamp ≠ 14 caractères | 700 |
| transactions | timestamp non numérique | 276 |
| users | âge hors [16 ; 100] | 220 |
| users | revenu annuel ≤ 0 | 130 |
| products | note hors [1 ; 5] | 95 |
| products | prix ≤ 0 | 85 |
| merchants | commission hors [0 ; 1] | 14 |

*(une même ligne peut violer plusieurs règles : la somme des motifs dépasse le
nombre de lignes rejetées)*

**Bonus 2.5 — intégrité référentielle** (sur les 136 157 transactions valides) :
4 273 `user_id`, 4 324 `product_id` et 3 133 `merchant_id` ne correspondent à
aucun enregistrement des référentiels validés.

**Bonus 3.4 — transactions suspectes** : 2 999 transactions réunissent au moins
deux des quatre signaux de risque.

---

## 7. Bonus 5.3 — Gain apporté par les optimisations

Mesure réalisée avec `sbt "run benchmark"` : le pipeline complet est exécuté
deux fois, d'abord avec `enable-cache = false` et `enable-broadcast = false`,
puis avec les deux optimisations activées.

**Environnement de mesure** : 2 vCPU, 7 Go de RAM, JDK 17, Spark 3.5.1,
`master = local[*]`, `spark.sql.shuffle.partitions = 8`, 138 047 transactions.

| Étape | Sans optimisation | Avec optimisation | Gain |
|---|---:|---:|---:|
| ingestion | 21 942 ms | 7 705 ms | **+64,88 %** |
| transformation | 1 591 ms | 8 812 ms | −453,87 % |
| analytique | 22 197 ms | 12 391 ms | **+44,18 %** |
| écriture | 43 279 ms | 19 184 ms | **+55,67 %** |
| **TOTAL** | **89 009 ms** | **48 092 ms** | **+45,97 %** |

### Lecture des résultats

- **Le gain global est de ~46 %**, soit 41 secondes économisées sur 89.
- L'**ingestion** gagne le plus : sans cache, chaque `count()` du rapport de
  qualité relit et reparse les fichiers sources ; les quatre validations
  déclenchent alors plusieurs relectures complètes de `transactions.csv`.
- La **transformation apparaît plus lente avec le cache**, et c'est attendu :
  en mode optimisé, `persist(MEMORY_AND_DISK_SER)` doit sérialiser les 136 157
  lignes × 49 colonnes au moment du `count()`. En mode non optimisé, Spark
  élague les colonnes inutiles pour un simple `count()` et ne matérialise
  presque rien — la facture est simplement **reportée sur les étapes
  suivantes**, qui doivent alors tout recalculer.
- C'est exactement ce que montrent l'**analytique** (4 analyses réutilisant le
  même DataFrame) et l'**écriture** (15 jeux de résultats écrits) : ces deux
  étapes remboursent largement le coût du cache.
- **Conclusion** : le cache ne se juge jamais sur l'étape qui le crée, mais sur
  le nombre de réutilisations en aval.

---

## 8. Choix techniques structurants

| Sujet | Choix | Raison |
|---|---|---|
| Scala 2.12.18 / Spark 3.5.1 | version de référence des distributions Spark 3.5 | le JAR tourne sur un cluster standard sans recompilation |
| Spark en `Provided` + `sbt-assembly` | JAR de ~6 Mo | le cluster fournit Spark ; embarquer Spark donnerait un JAR de ~300 Mo |
| Champs numériques en `Option[...]` | `amount`, `age`, `price`, `rating`, `commission_rate`… | sans `Option`, Spark convertirait `null` en `0` et fausserait validations et comptage de nulls |
| Jointures `LEFT` | transactions ⟕ users / products / merchants | aucune transaction réelle n'est perdue à cause d'une référence orpheline |
| `broadcast()` sur les référentiels | merchants (600), products (6 000), users (12 000) | supprime le shuffle de la table volumineuse |
| `rangeBetween` (et non `rowsBetween`) | fenêtre glissante de 7 jours | `rowsBetween` compterait des lignes, pas une durée |
| Sortie CSV **et** Parquet | tous les résultats | CSV pour le métier, Parquet pour la chaîne de traitement |

Les justifications détaillées (5 décisions minimum) figurent dans
`CONTRIBUTIONS.md`, section « Décisions techniques du groupe ».

---

## 9. Dépannage

| Symptôme | Cause | Solution |
|---|---|---|
| `InaccessibleObjectException` | JDK 17+ sans `--add-opens` | utiliser `sbt run` (options déjà configurées) ou ajouter les options à `spark-submit` (section 5) |
| Accents illisibles dans la console | encodage JVM ≠ UTF-8 | `-Dfile.encoding=UTF-8` (déjà dans `build.sbt`) |
| `Path does not exist` au démarrage | chemins relatifs | lancer les commandes **depuis la racine du projet**, ou mettre des chemins absolus dans `application.conf` |
| `OutOfMemoryError` | heap trop petit | `--driver-memory 4g`, ou baisser `app.data.output.sample-size` |
| Rapport de qualité à 0 % de rejet | règles mal appliquées | vérifier les seuils de `app.validation` — les fichiers fournis **contiennent** des anomalies |
