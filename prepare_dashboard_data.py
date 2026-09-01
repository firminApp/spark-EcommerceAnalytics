"""
prepare_dashboard_data.py
==========================
Copie les résultats produits par le pipeline Spark (dossiers
output/csv/<nom>/part-*.csv) vers dashboard_data/<nom>.csv — un seul fichier
plat par jeu de résultats, facile à committer sur GitHub et à lire par
l'application Streamlit (qui ne sait pas parler à Spark).

Ce script est prévu pour vivre au même niveau que app.py et que le dossier
output/ (racine du projet, à côté de build.sbt).

Usage : depuis la racine du projet :
    python prepare_dashboard_data.py

À relancer à chaque fois que vous voulez rafraîchir le dashboard après un
nouveau `sbt run`.
"""

import shutil
import sys
from pathlib import Path
from typing import Optional

# Noms des jeux de résultats à exposer dans le dashboard (voir les lignes
# [SORTIE] affichées par MainApp lors de l'exécution du pipeline).
RESULT_NAMES = [
    "01_rapport_qualite",
    "01_motifs_rejet",
    "02_kpi_marchands",
    "03_cohortes_tailles",
    "03_cohortes_retention",
    "03_cohortes_matrice",
    "03_cohortes_meilleure_3mois",
    "04_rfm_scores",
    "04_rfm_distribution",
    "04_rfm_croisement",
    "04_top_produits",
    "04_ca_categorie_region",
    "04_ca_paiement_periode",
]


def find_part_csv(folder: Path) -> Optional[Path]:
    """Spark écrit un dossier contenant un fichier part-00000-....csv (avec
    coalesce=1) ainsi qu'un marqueur _SUCCESS. On cherche le vrai fichier de
    données, en ignorant les fichiers de metadata (_SUCCESS, .crc)."""
    if not folder.is_dir():
        return None
    candidates = sorted(folder.glob("part-*.csv"))
    return candidates[0] if candidates else None


def main() -> None:
    # Ce script vit à la racine du projet, au même niveau que build.sbt,
    # app.py et le dossier output/ — Path(__file__).parent pointe donc
    # directement sur la racine, quel que soit le répertoire courant d'où
    # le script est lancé.
    script_dir = Path(__file__).resolve().parent
    output_csv_dir = script_dir / "output" / "csv"
    dashboard_data_dir = script_dir / "dashboard_data"

    if not output_csv_dir.exists():
        print(f"[ERREUR] Dossier introuvable : {output_csv_dir}")
        print("         Avez-vous bien lancé `sbt run` avant ce script ?")
        sys.exit(1)

    dashboard_data_dir.mkdir(parents=True, exist_ok=True)

    copied, missing = [], []
    for name in RESULT_NAMES:
        source_folder = output_csv_dir / name
        part_file = find_part_csv(source_folder)
        if part_file is None:
            missing.append(name)
            continue
        destination = dashboard_data_dir / f"{name}.csv"
        shutil.copyfile(part_file, destination)
        copied.append(name)

    print(f"[OK] {len(copied)} fichier(s) copié(s) vers {dashboard_data_dir} :")
    for name in copied:
        print(f"     - {name}.csv")

    if missing:
        print(f"\n[ATTENTION] {len(missing)} résultat(s) introuvable(s) dans output/csv/ :")
        for name in missing:
            print(f"     - {name}")
        print("Le dashboard affichera un avertissement pour ces sections.")


if __name__ == "__main__":
    main()
