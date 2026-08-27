# CONTRIBUTIONS.md — GROUPE 3

**Projet Final Data Engineer — Spark & Scala**
BANIGANTE Kpapou (Membre A) · CAMARA Oumar (Membre B) · CHAKVOURNE Frédéric (Membre C)

---

## 1. Tableau récapitulatif : question → responsable → relecteur

| Question | Intitulé | Responsable | Relecteur |
|---|---|---|---|
| 0.1 | Constitution du groupe, `EQUIPE.md` | Membre A | Membres B et C |
| 0.2 | Dépôt Git et `.gitignore` | Membre A | Membre C |
| 0.3 | Journal de contribution | Membre C | Membres A et B |
| 1.1 | Structure de projet SBT | Membre A | Membre C |
| 1.2 | Configuration `build.sbt` | Membre A | Membre C |
| 1.3 | Documentation `README.md` | Membre A | Membre B |
| 2.1 | Ingestion multi-format | Membre A | Membre B |
| 2.2 | Validation des données | Membre A | Membre B |
| 2.3 | Gestion d'erreurs et résumé | Membre A | Membre B |
| 2.4 | Rapport de qualité des données | Membre A | Membre C |
| **2.5 (bonus)** | Intégrité référentielle | Membre A | Membre C |
| 3.1 | UDF `extractTimeFeatures` | Membre B | Membre A |
| 3.2 | Fonction `enrichTransactionData` | Membre B | Membre A |
| 3.3 | Analyse par partition `Window` | Membre B | Membre C |
| **3.4 (bonus)** | Détection de transactions suspectes | Membre B | Membre C |
| 4.1 | Rapport détaillé par marchand | Membre C | Membre B |
| 4.2 | Analyse de cohortes utilisateurs | Membre C | Membre B |
| **4.3 (bonus)** | Segmentation RFM | Membre C | Membre A |
| **4.4 (bonus)** | Analyse produits et catégories | Membre C | Membre A |
| 5.1 | Optimisation du stockage | Membre C | Membres A et B |
| 5.2 | Optimisation des jointures | Membre C | Membres A et B |
| **5.3 (bonus)** | Mesure du gain des optimisations | Membre C | Membre A |
| 6.1 | Application principale | Membre C | Membres A et B |
| **6.2 (bonus)** | Exécution modulaire par étape | Membre C | Membre A |
| 7.1 | Configuration externalisée | Membre A | Membre C |
| 8 | Tests unitaires, livrables, soutenance | Collectif | Collectif |

**Couverture** : tronc commun complet + les 6 questions bonus (2.5, 3.4, 4.3,
4.4, 5.3, 6.2).

---

## 2. Charge de travail et difficultés rencontrées

### Membre A — BANIGANTE Kpapou — *≈ 14 h*

| Poste | Heures |
|---|---:|
| Mise en place SBT, `build.sbt`, plugin assembly | 2,5 |
| Case classes et schémas explicites | 2,0 |
| `DataIngestion` (4 formats + gestion d'erreurs) | 3,0 |
| `DataValidation` + rapport qualité + bonus 2.5 | 3,5 |
| `application.conf` et `ConfigLoader` | 1,5 |
| `README.md`, `EQUIPE.md`, tests | 1,5 |

**Difficultés :**

1. **`null` silencieusement converti en `0`.** La première version des case
   classes utilisait `amount: Double`. Résultat : les montants illisibles
   devenaient `0.0` et échappaient à la règle `amount > 0`… tout en faussant le
   compte de valeurs nulles. Correction : `Option[Double]` sur tous les champs
   numériques.
2. **Conditions SQL à trois valeurs.** `amount > 0` vaut `null` — et non
   `false` — quand `amount` est nul. Une ligne à montant nul passait donc la
   validation. Correction : `coalesce(condition, lit(false))` dans
   `applyRules`, pour qu'une condition indéterminée soit traitée comme une
   violation.
3. **Jointure `left_anti` ambiguë (bonus 2.5).** `col("user_id") === col("user_id")`
   déclenche `AMBIGUOUS_REFERENCE` : les deux côtés portent le même nom.
   Correction : renommer en `cle_etrangere` / `cle_primaire` avant la jointure.
4. **Inférence de schéma sur `merchants.csv`.** Spark typait
   `establishment_date` en entier (`20220918`), incompatible avec la case class.
   Correction : méthode `alignTo` qui projette et caste sur le schéma cible.

### Membre B — CAMARA Oumar — *≈ 13 h*

| Poste | Heures |
|---|---:|
| UDF `extractTimeFeatures` + tests | 2,5 |
| `enrichTransactionData` (jointures, préfixage) | 3,5 |
| Fonctions de fenêtrage (Q3.2 et Q3.3) | 4,0 |
| Bonus 3.4 — transactions suspectes | 2,0 |
| Relectures et corrections | 1,0 |

**Difficultés :**

1. **Sérialisation du `DateTimeFormatter`.** Un `DateTimeFormatter` déclaré en
   `val` dans un objet utilisé par une UDF provoque un
   `NotSerializableException` sur un vrai cluster. Correction :
   `@transient private lazy val` — le formateur est reconstruit dans chaque
   exécuteur.
2. **`rowsBetween` vs `rangeBetween`.** La première version de la fenêtre
   « 7 jours » utilisait `rowsBetween(-6, 0)`, qui compte **7 lignes** et non
   7 jours : un utilisateur avec 20 achats le même jour donnait un résultat
   absurde. Correction : `rangeBetween` sur `tx_day_epoch` exprimé en secondes.
3. **Colonnes homonymes.** `name` et `category` existent dans plusieurs tables,
   `merchant_id` dans trois. Toute jointure directe produisait des références
   ambiguës. Correction : renommage systématique **avant** la jointure
   (`product_name`, `merchant_category`, …).
4. **Détection du « moins de 5 minutes ».** `datediff` a une granularité
   journalière et ne permet pas de détecter un intervalle de quelques minutes.
   Correction : un second `lag` sur `tx_epoch` (secondes) en parallèle du `lag`
   sur `tx_date`.

### Membre C — CHAKVOURNE Frédéric — *≈ 14 h*

| Poste | Heures |
|---|---:|
| KPI marchands (Q4.1) | 2,5 |
| Analyse de cohortes (Q4.2) | 3,0 |
| Bonus 4.3 (RFM) et 4.4 (produits) | 3,0 |
| `SparkOptimizations` et bonus 5.3 | 2,5 |
| `MainApp`, `DataWriter`, bonus 6.2 | 2,5 |
| Intégration et exécution de bout en bout | 0,5 |

**Difficultés :**

1. **Sens du score de récence (RFM).** `ntile(5)` sur une récence triée
   croissante attribue 5 au **pire** client. Correction : tri décroissant sur
   `recence_jours`, pour que le score 5 corresponde toujours au meilleur profil
   sur les trois axes.
2. **Résultat contre-intuitif du benchmark.** L'étape « transformation »
   apparaît **plus lente** avec `persist`. Après lecture du plan physique :
   sans cache, Spark élague les colonnes pour un simple `count()` ; avec cache,
   il sérialise les 49 colonnes. Le coût est reporté, pas créé — d'où l'ajout
   d'une ligne **TOTAL** au tableau comparatif (+46 % au global).
3. **Cohortes : inscription ou première transaction ?** `registration_date`
   existe dans `users.json`, mais l'énoncé demande le **mois de première
   transaction**. Les deux ne coïncident pas. Décision consignée en 4.b.
4. **Fenêtre sans partition.** Les `ntile` du RFM s'exécutent sur une fenêtre
   globale, que Spark ramène sur une seule partition (avertissement
   `WindowExec`). Acceptable ici (12 000 utilisateurs) ; à remplacer par
   `approxQuantile` au-delà de quelques millions de lignes.

---

## 3. Relectures croisées

Chaque module a été relu par un membre autre que son auteur (Question 0.3).

| Date | Module relu | Auteur | Relecteur | Remarques formulées | Suite donnée |
|---|---|---|---|---|---|
| 2026-08-14 | `build.sbt`, structure SBT | Membre A | Membre C | `sbt run` échouait : les dépendances `Provided` sont exclues du classpath de `run`. | Ajout de `Compile / run := Defaults.runTask(...)`. Corrigé. |
| 2026-08-16 | `Models.scala`, `DataIngestion.scala` | Membre A | Membre B | Champs numériques en types primitifs → `null` converti en `0`. Chemins encore codés en dur dans une méthode de test. | Passage en `Option[...]` ; tous les chemins routés vers `ConfigLoader`. Corrigé. |
| 2026-08-18 | `DataValidation.scala` | Membre A | Membre B | Une ligne à `amount` nul était acceptée (logique SQL à trois valeurs). Le motif de rejet ne gardait que la **première** règle violée. | `coalesce(cond, false)` ; `concat_ws` pour lister **toutes** les règles violées. Corrigé. |
| 2026-08-19 | `TimeFeatures.scala` | Membre B | Membre A | `DateTimeFormatter` non sérialisable ; les heures 0h-6h tombaient dans `Unknown` au lieu de `Night`. | `@transient lazy val` ; branche `[0h ; 6h[ → Night`. Corrigé + 5 tests unitaires ajoutés. |
| 2026-08-21 | `DataTransformation.scala` (Q3.2) | Membre B | Membre A | Type de jointure non documenté ; `INNER` supprimait ~4 300 transactions à référence orpheline sans alerte. | Passage en `LEFT` + justification en tête de fichier. Corrigé. |
| 2026-08-22 | `DataTransformation.scala` (Q3.3 / 3.4) | Membre B | Membre C | `rowsBetween` au lieu de `rangeBetween` pour la fenêtre 7 jours ; seuils de suspicion codés en dur. | `rangeBetween` sur `tx_day_epoch` ; seuils déplacés dans `app.business.suspicious`. Corrigé. |
| 2026-08-24 | `Analytics.scala` (Q4.1 / 4.2) | Membre C | Membre B | `rank()` créait des trous dans les classements ; `period_index` calculé sur les dates et non sur les mois tronqués (valeurs négatives possibles). | `dense_rank()` ; `months_between(trunc(...,"month"), ...)`. Corrigé. |
| 2026-08-25 | `Analytics.scala` (bonus 4.3 / 4.4) | Membre C | Membre A | Score de récence inversé ; `refDate` planterait sur un jeu de données vide. | Tri décroissant sur la récence ; repli sur `current_date` si aucune date. Corrigé. |
| 2026-08-26 | `SparkOptimizations.scala`, `MainApp.scala` | Membre C | Membres A et B | `unpersist()` jamais appelé sur les DataFrame bruts ; un argument inconnu levait une exception au lieu d'afficher l'aide. | Libération explicite après la phase 1 ; aide + `System.exit(2)`. Corrigé. |
| 2026-08-27 | Pipeline complet (intégration) | Collectif | Collectif | Console illisible (accents), et `AMBIGUOUS_REFERENCE` sur le bonus 2.5 en exécution réelle. | `-Dfile.encoding=UTF-8` ; renommage des clés avant `left_anti`. Corrigé — exécution complète OK. |

---

## 4. Décisions techniques du groupe

### 4.a — Spark 3.5.1 et Scala 2.12.18

Spark 3.5.x est distribué officiellement compilé pour Scala 2.12 : les binaires
`spark-3.5.x-bin-hadoop3` embarquent cette version. Retenir 2.13 aurait imposé
de recompiler ou de reconstruire le cluster. Spark 3.5.1 est par ailleurs la
dernière version stable compatible JDK 11 **et** JDK 17, ce qui permet à chaque
membre de travailler avec son JDK installé.

### 4.b — Cohortes fondées sur la première transaction, pas sur l'inscription

`users.json` fournit `registration_date`, mais l'énoncé demande le mois de
**première transaction**. Les deux dates diffèrent nettement dans le jeu
fourni : un client inscrit en janvier peut n'acheter qu'en mai. Nous mesurons
donc la fidélité réelle (comportement d'achat) et non l'acquisition marketing,
qui produirait des taux de rétention artificiellement bas en période 0.

### 4.c — Jointures `LEFT` plutôt qu'`INNER`

Le jeu de données contient volontairement ~4 300 `user_id`, ~4 300 `product_id`
et ~3 100 `merchant_id` orphelins. Un `INNER join` aurait supprimé ces
transactions **sans aucune alerte**, faussant le chiffre d'affaires de plusieurs
pourcents. Avec `LEFT`, la transaction — le fait métier — est toujours
conservée, les attributs manquants valent `null` ou « Inconnu », et les
orphelins sont explicitement comptés dans le rapport de qualité (bonus 2.5).

### 4.d — Spark en `Provided` et JAR *assembly* (~6 Mo)

`sbt-assembly` est préféré à `sbt package` : il embarque la bibliothèque
standard Scala et Typesafe Config, absentes du classpath d'un cluster, ce qui
évite un `--jars` fastidieux. Spark reste en `Provided` : l'embarquer donnerait
un JAR d'environ 300 Mo et risquerait un conflit de versions avec le runtime du
cluster. Les jeux de données sont exclus du JAR (règle `MergeStrategy.discard`
sur `data/**`) puisqu'ils sont lus depuis le système de fichiers.

### 4.e — Sortie en CSV **et** en Parquet, avec `coalesce(1)`

Deux publics : le CSV est ouvert directement dans Excel par l'équipe métier ; le
Parquet conserve les types et la compression pour la chaîne de traitement aval.
`coalesce(1)` évite de livrer des dizaines de fichiers `part-*` pour des
tableaux de quelques centaines de lignes ; le paramètre reste externalisé
(`app.data.output.coalesce`) pour être relevé sur un vrai cluster.

### 4.f — Aucune ligne rejetée n'est perdue

Chaque fonction de validation renvoie les lignes valides **et** les lignes
rejetées, enrichies d'une colonne `rejection_reason` qui liste *toutes* les
règles violées. Un fichier de rejets est écrit sur disque : c'est le livrable
qui permet à l'équipe métier de corriger la source, plutôt qu'un simple
compteur d'erreurs.

### 4.g — Toute la configuration dans `application.conf`, avec valeurs par défaut

Aucun chemin, seuil ou paramètre Spark n'est écrit dans le code Scala. Toute clé
absente retombe sur une valeur par défaut déclarée dans `ConfigLoader`, avec un
avertissement en console : le job ne s'arrête jamais sur une configuration
incomplète, mais l'écart reste tracé. C'est ce mécanisme qui rend possible le
benchmark du bonus 5.3, en surchargeant seulement deux drapeaux.

---

## 5. Bilan

- **Tronc commun** : intégralement traité et exécuté sur les données fournies.
- **Bonus** : 6 questions sur 6 traitées (2.5, 3.4, 4.3, 4.4, 5.3, 6.2).
- **Tests** : 9 tests unitaires, tous au vert (`sbt test`).
- **Exécution de référence** : 136 157 transactions enrichies, 15 jeux de
  résultats produits en CSV et en Parquet, pipeline complet en ~48 s en local.
- **Charge totale du groupe** : ≈ 41 heures.
