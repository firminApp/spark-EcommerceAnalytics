"""
Dashboard Streamlit — EcommerceAnalytics
=========================================
Lit les résultats déjà produits par le pipeline Spark/Scala (fichiers CSV
copiés dans dashboard_data/ par prepare_dashboard_data.py) et les présente
sous forme de tableaux et graphiques interactifs.

Ce dashboard ne réexécute PAS Spark : il visualise uniquement des résultats
déjà calculés. Lancer avec : streamlit run app.py
"""

from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Configuration générale et identité visuelle
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="EcommerceAnalytics — Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

DATA_DIR = Path(__file__).parent / "dashboard_data"

NAVY = "#1B2A4A"
NAVY_LIGHT = "#2C3E5D"
ORANGE = "#C1521A"
GREY = "#4A4A4A"
GREY_LIGHT = "#8A93A3"
CARD_BG = "#F4F6F9"

PALETTE = [NAVY, ORANGE, "#4A7C9B", "#E0A458", "#5B7B5B", "#8A93A3", "#9B4A6B", "#4AA3A0"]
PLOTLY_TEMPLATE = "plotly_white"

CUSTOM_CSS = f"""
<style>
    #MainMenu {{visibility: hidden;}}
    footer {{visibility: hidden;}}
    header[data-testid="stHeader"] {{background: transparent;}}

    .hero-banner {{
        background: linear-gradient(135deg, {NAVY} 0%, {NAVY_LIGHT} 100%);
        padding: 2rem 2.4rem;
        border-radius: 14px;
        margin-bottom: 1.6rem;
    }}
    .hero-tag {{
        display: inline-block;
        background: {ORANGE};
        color: white;
        padding: 0.15rem 0.75rem;
        border-radius: 20px;
        font-size: 0.72rem;
        font-weight: 700;
        letter-spacing: 0.06em;
        margin-bottom: 0.7rem;
        text-transform: uppercase;
    }}
    .hero-banner h1 {{
        color: white;
        font-size: 1.9rem;
        font-weight: 700;
        margin: 0 0 0.3rem 0;
    }}
    .hero-banner p {{
        color: #C7CEDC;
        font-size: 0.92rem;
        margin: 0;
    }}

    [data-testid="stMetric"] {{
        background-color: {CARD_BG};
        border-left: 4px solid {ORANGE};
        padding: 0.9rem 1.1rem;
        border-radius: 10px;
    }}
    [data-testid="stMetricLabel"] {{ color: {GREY}; font-weight: 600; }}
    [data-testid="stMetricValue"] {{ color: {NAVY}; }}

    section[data-testid="stSidebar"] {{ background-color: {CARD_BG}; }}
    section[data-testid="stSidebar"] .stRadio label {{ font-size: 0.92rem; }}

    h2, h3 {{ color: {NAVY} !important; }}

    .section-caption {{ color: {GREY_LIGHT}; font-size: 0.85rem; margin-top: -0.6rem; margin-bottom: 1rem; }}
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Chargement des données
# ---------------------------------------------------------------------------

@st.cache_data
def load_csv(name: str) -> pd.DataFrame:
    """Charge un CSV depuis dashboard_data/<name>.csv. DataFrame vide si absent."""
    path = DATA_DIR / f"{name}.csv"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def missing_data_notice(name: str) -> None:
    st.warning(
        f"Fichier « {name}.csv » introuvable dans dashboard_data/. "
        f"Lancez prepare_dashboard_data.py après un `sbt run` pour le générer."
    )


def styled_fig(fig: go.Figure, title: str = "") -> go.Figure:
    """Applique une mise en forme cohérente (police, couleurs, marges) à un graphique Plotly."""
    fig.update_layout(
        template=PLOTLY_TEMPLATE,
        title=dict(text=title, font=dict(size=16, color=NAVY, family="Arial, sans-serif")) if title else None,
        font=dict(family="Arial, sans-serif", color=GREY, size=12),
        margin=dict(l=10, r=10, t=50 if title else 10, b=10),
        legend=dict(bgcolor="rgba(0,0,0,0)"),
        plot_bgcolor="white",
        paper_bgcolor="white",
    )
    return fig


def download_button_for_df(df: pd.DataFrame, filename: str, label: str = "Télécharger en CSV") -> None:
    st.download_button(
        label=f"⬇ {label}",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=filename,
        mime="text/csv",
    )


def hero(title: str, subtitle: str, tag: str = "Spark & Scala · Groupe 6") -> None:
    st.markdown(
        f"""
        <div class="hero-banner">
            <div class="hero-tag">{tag}</div>
            <h1>{title}</h1>
            <p>{subtitle}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def kpi_card(col, label: str, value: str) -> None:
    col.metric(label, value)


def fmt_int(n) -> str:
    try:
        return f"{int(n):,}".replace(",", " ")
    except (ValueError, TypeError):
        return "—"


# ---------------------------------------------------------------------------
# En-tête et navigation
# ---------------------------------------------------------------------------

hero(
    "📊 EcommerceAnalytics — Tableau de bord",
    "BANIGANTE Kpapou · CAMARA Oumar · CHAKVOURNE Frédéric — résultats du dernier pipeline exécuté",
)

st.sidebar.markdown(f"### 🧭 Navigation")
page = st.sidebar.radio(
    "Section",
    [
        "🏠 Vue d'ensemble",
        "✅ Qualité des données",
        "🏪 KPI Marchands",
        "🔁 Cohortes & rétention",
        "🎯 Segmentation RFM",
        "📦 Produits & catégories",
    ],
    label_visibility="collapsed",
)

# ---------------------------------------------------------------------------
# Vue d'ensemble
# ---------------------------------------------------------------------------

if page.endswith("Vue d'ensemble"):
    qualite = load_csv("01_rapport_qualite")
    marchands = load_csv("02_kpi_marchands")

    st.subheader("Volumétrie globale")
    st.markdown('<p class="section-caption">Lignes valides / lignes lues, par source de données</p>', unsafe_allow_html=True)
    if qualite.empty:
        missing_data_notice("01_rapport_qualite")
    else:
        cols = st.columns(len(qualite))
        for col, (_, row) in zip(cols, qualite.iterrows()):
            kpi_card(col, row["dataset"].capitalize(), f"{fmt_int(row['nb_lignes_valides'])} / {fmt_int(row['nb_lignes_lues'])}")

    st.markdown("###")
    st.subheader("Chiffre d'affaires")
    if marchands.empty:
        missing_data_notice("02_kpi_marchands")
    else:
        total_ca = marchands["chiffre_affaires_total"].sum()
        nb_marchands = marchands["merchant_id"].nunique()
        ca_moyen = marchands["montant_moyen_transaction"].mean()

        c1, c2, c3 = st.columns(3)
        kpi_card(c1, "Chiffre d'affaires total", f"{fmt_int(total_ca)} €")
        kpi_card(c2, "Marchands actifs", fmt_int(nb_marchands))
        kpi_card(c3, "Panier moyen (marchands)", f"{ca_moyen:,.2f} €".replace(",", " "))

        top_n = st.slider("Nombre de marchands affichés", min_value=5, max_value=30, value=10, step=5)
        top = marchands.sort_values("chiffre_affaires_total", ascending=False).head(top_n)

        fig = px.bar(
            top,
            x="chiffre_affaires_total",
            y="merchant_name",
            orientation="h",
            color_discrete_sequence=[ORANGE],
            labels={"chiffre_affaires_total": "Chiffre d'affaires (€)", "merchant_name": ""},
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(styled_fig(fig, f"Top {top_n} marchands par chiffre d'affaires"), use_container_width=True)

# ---------------------------------------------------------------------------
# Qualité des données
# ---------------------------------------------------------------------------

elif page.endswith("Qualité des données"):
    qualite = load_csv("01_rapport_qualite")
    motifs = load_csv("01_motifs_rejet")

    st.subheader("Rapport de qualité (Question 2.4)")
    if qualite.empty:
        missing_data_notice("01_rapport_qualite")
    else:
        st.dataframe(qualite, use_container_width=True, hide_index=True)
        download_button_for_df(qualite, "rapport_qualite.csv")

        fig = px.bar(
            qualite,
            x="dataset",
            y="taux_rejet",
            color="dataset",
            color_discrete_sequence=PALETTE,
            labels={"dataset": "", "taux_rejet": "Taux de rejet (%)"},
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(styled_fig(fig, "Taux de rejet par dataset"), use_container_width=True)

        cols_orphelines = [c for c in qualite.columns if c.startswith("nb_refs_orphelines")]
        if cols_orphelines:
            st.subheader("Intégrité référentielle (bonus 2.5)")
            orphelines = qualite.dropna(subset=cols_orphelines, how="all")[["dataset"] + cols_orphelines]
            st.dataframe(orphelines, use_container_width=True, hide_index=True)

    st.subheader("Détail des motifs de rejet")
    if motifs.empty:
        missing_data_notice("01_motifs_rejet")
    else:
        datasets_dispo = sorted(motifs["dataset"].dropna().unique().tolist())
        choix_ds = st.multiselect("Filtrer par dataset", datasets_dispo, default=datasets_dispo)
        df = motifs[motifs["dataset"].isin(choix_ds)] if choix_ds else motifs.iloc[0:0]

        if df.empty:
            st.info("Sélectionnez au moins un dataset pour afficher le détail des rejets.")
        else:
            fig2 = px.bar(
                df.sort_values("nb_lignes", ascending=True),
                x="nb_lignes",
                y="regle",
                color="dataset",
                orientation="h",
                color_discrete_sequence=PALETTE,
                labels={"nb_lignes": "Nombre de lignes rejetées", "regle": ""},
            )
            st.plotly_chart(styled_fig(fig2, "Motifs de rejet"), use_container_width=True)

# ---------------------------------------------------------------------------
# KPI Marchands
# ---------------------------------------------------------------------------

elif page.endswith("KPI Marchands"):
    marchands = load_csv("02_kpi_marchands")

    st.subheader("Rapport détaillé par marchand (Question 4.1)")
    if marchands.empty:
        missing_data_notice("02_kpi_marchands")
    else:
        categories = sorted(marchands["merchant_category"].dropna().unique().tolist())
        regions = sorted(marchands["merchant_region"].dropna().unique().tolist())

        c1, c2 = st.columns(2)
        choix_cats = c1.multiselect("Catégories", categories, default=categories)
        choix_regions = c2.multiselect("Régions", regions, default=regions)

        if not choix_cats or not choix_regions:
            st.info("Sélectionnez au moins une catégorie et une région pour afficher les résultats.")
        else:
            df = marchands[
                marchands["merchant_category"].isin(choix_cats)
                & marchands["merchant_region"].isin(choix_regions)
            ]

            st.dataframe(df.sort_values("chiffre_affaires_total", ascending=False), use_container_width=True, hide_index=True)
            download_button_for_df(df, "kpi_marchands_filtre.csv")

            fig = px.scatter(
                df,
                x="nb_transactions",
                y="montant_moyen_transaction",
                size="chiffre_affaires_total",
                color="merchant_category",
                hover_name="merchant_name",
                color_discrete_sequence=PALETTE,
                labels={"nb_transactions": "Nombre de transactions", "montant_moyen_transaction": "Panier moyen (€)"},
            )
            st.plotly_chart(
                styled_fig(fig, "Positionnement des marchands : volume vs panier moyen"), use_container_width=True
            )

# ---------------------------------------------------------------------------
# Cohortes & rétention
# ---------------------------------------------------------------------------

elif page.endswith("Cohortes & rétention"):
    tailles = load_csv("03_cohortes_tailles")
    matrice = load_csv("03_cohortes_matrice")
    meilleure = load_csv("03_cohortes_meilleure_3mois")

    st.subheader("Analyse de cohortes (Question 4.2)")

    if tailles.empty:
        missing_data_notice("03_cohortes_tailles")
    else:
        fig = px.bar(
            tailles,
            x="cohort_month",
            y="nb_utilisateurs_initiaux",
            color_discrete_sequence=[NAVY],
            labels={"cohort_month": "", "nb_utilisateurs_initiaux": "Nouveaux clients"},
        )
        st.plotly_chart(styled_fig(fig, "Taille de chaque cohorte"), use_container_width=True)

    st.subheader("Matrice de rétention")
    if matrice.empty:
        missing_data_notice("03_cohortes_matrice")
    else:
        toutes_cohortes = matrice["cohort_month"].tolist()
        choix_cohortes = st.multiselect(
            "Cohortes à afficher (par défaut : les 12 dernières)",
            toutes_cohortes,
            default=toutes_cohortes[-12:] if len(toutes_cohortes) > 12 else toutes_cohortes,
        )
        df = matrice[matrice["cohort_month"].isin(choix_cohortes)] if choix_cohortes else matrice.iloc[0:0]

        if df.empty:
            st.info("Sélectionnez au moins une cohorte.")
        else:
            matrice_indexed = df.set_index("cohort_month")
            fig = px.imshow(
                matrice_indexed,
                color_continuous_scale=[[0, "#FFF3EA"], [1, ORANGE]],
                aspect="auto",
                labels={"x": "Mois écoulés", "y": "", "color": "Rétention (%)"},
            )
            st.plotly_chart(styled_fig(fig, "Taux de rétention par cohorte et par mois écoulé"), use_container_width=True)

    st.subheader("Meilleures cohortes à l'horizon de référence")
    if meilleure.empty:
        missing_data_notice("03_cohortes_meilleure_3mois")
    else:
        st.dataframe(meilleure, use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# Segmentation RFM
# ---------------------------------------------------------------------------

elif page.endswith("Segmentation RFM"):
    distribution = load_csv("04_rfm_distribution")
    croisement = load_csv("04_rfm_croisement")
    scores = load_csv("04_rfm_scores")

    st.subheader("Segmentation RFM (bonus 4.3)")

    if distribution.empty:
        missing_data_notice("04_rfm_distribution")
        segments_disponibles = []
    else:
        segments_disponibles = distribution["segment_rfm"].tolist()
        choix_segments = st.multiselect("Segments à inclure", segments_disponibles, default=segments_disponibles)

        dist_f = distribution[distribution["segment_rfm"].isin(choix_segments)] if choix_segments else distribution.iloc[0:0]

        c1, c2 = st.columns([1, 1])
        with c1:
            if dist_f.empty:
                st.info("Sélectionnez au moins un segment.")
            else:
                fig = px.pie(
                    dist_f,
                    names="segment_rfm",
                    values="nb_clients",
                    color_discrete_sequence=PALETTE,
                    hole=0.45,
                )
                st.plotly_chart(styled_fig(fig, "Répartition des clients par segment"), use_container_width=True)
        with c2:
            st.dataframe(dist_f, use_container_width=True, hide_index=True)

    if not croisement.empty:
        st.subheader("Segment RFM calculé × segment déclaré")
        croisement_f = (
            croisement[croisement["segment_rfm"].isin(choix_segments)]
            if segments_disponibles and choix_segments
            else croisement
        )
        st.dataframe(croisement_f, use_container_width=True, hide_index=True)
    else:
        missing_data_notice("04_rfm_croisement")

    if not scores.empty:
        with st.expander("🔍 Voir le détail des scores par client"):
            scores_f = (
                scores[scores["segment_rfm"].isin(choix_segments)]
                if segments_disponibles and choix_segments
                else scores
            )
            st.dataframe(scores_f, use_container_width=True, hide_index=True)
            download_button_for_df(scores_f, "rfm_scores_filtre.csv")

# ---------------------------------------------------------------------------
# Produits & catégories
# ---------------------------------------------------------------------------

elif page.endswith("Produits & catégories"):
    top_produits = load_csv("04_top_produits")
    ca_categorie_region = load_csv("04_ca_categorie_region")
    ca_paiement_periode = load_csv("04_ca_paiement_periode")

    st.subheader("Top produits par chiffre d'affaires (bonus 4.4)")
    if top_produits.empty:
        missing_data_notice("04_top_produits")
    else:
        st.dataframe(top_produits, use_container_width=True, hide_index=True)
        download_button_for_df(top_produits, "top_produits.csv")

    st.subheader("Chiffre d'affaires par catégorie et région")
    if ca_categorie_region.empty:
        missing_data_notice("04_ca_categorie_region")
    else:
        categories = sorted(ca_categorie_region["product_category"].dropna().unique().tolist())
        regions = sorted(ca_categorie_region["merchant_region"].dropna().unique().tolist())

        c1, c2 = st.columns(2)
        choix_cats = c1.multiselect("Catégories de produits", categories, default=categories)
        choix_regions = c2.multiselect("Régions", regions, default=regions)

        df = ca_categorie_region[
            ca_categorie_region["product_category"].isin(choix_cats)
            & ca_categorie_region["merchant_region"].isin(choix_regions)
        ] if choix_cats and choix_regions else ca_categorie_region.iloc[0:0]

        if df.empty:
            st.info("Sélectionnez au moins une catégorie et une région.")
        else:
            fig = px.bar(
                df.sort_values("chiffre_affaires", ascending=False),
                x="product_category",
                y="chiffre_affaires",
                color="merchant_region",
                color_discrete_sequence=PALETTE,
                barmode="group",
                labels={"product_category": "", "chiffre_affaires": "Chiffre d'affaires (€)"},
            )
            st.plotly_chart(styled_fig(fig, "Chiffre d'affaires par catégorie de produit"), use_container_width=True)

    st.subheader("Chiffre d'affaires par méthode de paiement et période")
    if ca_paiement_periode.empty:
        missing_data_notice("04_ca_paiement_periode")
    else:
        methodes = sorted(ca_paiement_periode["payment_method"].dropna().unique().tolist())
        choix_methodes = st.multiselect("Méthodes de paiement", methodes, default=methodes)
        df = ca_paiement_periode[ca_paiement_periode["payment_method"].isin(choix_methodes)] if choix_methodes else ca_paiement_periode.iloc[0:0]

        if df.empty:
            st.info("Sélectionnez au moins une méthode de paiement.")
        else:
            pivot = df.pivot(index="payment_method", columns="day_period", values="chiffre_affaires")
            fig = px.imshow(
                pivot,
                color_continuous_scale=[[0, "#EAF1FB"], [1, NAVY]],
                aspect="auto",
                labels={"x": "Période de la journée", "y": "", "color": "CA (€)"},
            )
            st.plotly_chart(
                styled_fig(fig, "Chiffre d'affaires par méthode de paiement × période"), use_container_width=True
            )

# ---------------------------------------------------------------------------
# Pied de sidebar
# ---------------------------------------------------------------------------

st.sidebar.markdown("---")
st.sidebar.caption(
    "Les données affichées proviennent du dernier export CSV du pipeline "
    "Spark/Scala. Pour les rafraîchir : relancer `sbt run`, puis "
    "`python prepare_dashboard_data.py`, puis commit + push."
)
