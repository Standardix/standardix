import streamlit as st
import pandas as pd
from io import BytesIO
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
import unicodedata
from typing import Optional, List, Dict

from standardix_engine import standardix, read_table

# --------------------------------------------------
# Constantes pour le générateur de descriptions courtes
# --------------------------------------------------

SHEET_EN = "EN"
SHEET_FR = "FR"
RECIPE_SHEET_EN = "Recipe_EN"
RECIPE_SHEET_FR = "Recipe_FR"

COL_SKU = "sku"
COL_PRODUCT_TYPE = "product_type"
COL_SHORT_DESC = "short_description"
ORIG_DESC_COL = "Short Description"

EMPTY_MARKERS = {
    "",
    " ",
    None,
    "unmapped",
    "UNMAPPED",
    "undefined",
    "undefinied",
    "nan",
    "NaN",
    "NAN",
    "UNDEFINITE",   # valeur EN quand non trouvée dans le mapping
    "NON_MAPPÉ",    # valeur FR quand non trouvée dans le mapping
}

SOURCE_TYPE_MAP = {
    "attribute value": "ATTRIBUTE_VALUE",
    "valeur d'attribut": "ATTRIBUTE_VALUE",
    "attribute name": "ATTRIBUTE_NAME",
    "nom d'attribut": "ATTRIBUTE_NAME",
}

SEPARATOR_KEYWORDS = {
    "space": " ",
    "comma": ", ",
    "colon": ": ",
    "dash": " - ",
    "dot": ". ",
    "bullet": " • ",
    "espace": " ",
    "virgule": ", ",
    "virgule-espace": ", ",
    "deux_points": ": ",
    "tiret": " - ",
    "point": ". ",
    "puce": " • ",
    "'s": "'s ",
    "’s": "’s ",
}


def normalize_string(x: Optional[str]) -> str:
    if x is None:
        return ""
    return str(x).strip()


def strip_accents(s: str) -> str:
    s = normalize_string(s)
    nfkd = unicodedata.normalize("NFD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def canon_key(s: str) -> str:
    s = normalize_string(s).lower()
    return s.replace(" ", "").replace("_", "")


def canon_match_key(s: str) -> str:
    s = strip_accents(s).lower()
    return " ".join(s.split())


def is_empty_value(x: Optional[str]) -> bool:
    if x is None or pd.isna(x):
        return True
    s = str(x).strip()
    return s in EMPTY_MARKERS


def load_recipes(df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    """
    Valide et prépare le contenu d'une feuille de recettes
    déjà chargée en DataFrame.
    """
    required_cols = ["product_type", "order", "source_type", "attribute_name", "separator_after"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column '{col}' in sheet '{sheet_name}'")

    # Colonne brand facultative
    if "brand" not in df.columns:
        df["brand"] = ""

    # Normalisation des colonnes texte
    for col in ["product_type", "brand", "source_type", "attribute_name"]:
        df[col] = df[col].apply(lambda v: normalize_string(v))

    return df


def resolve_source_type(raw: str) -> str:
    key = normalize_string(raw).lower()
    if key in SOURCE_TYPE_MAP:
        return SOURCE_TYPE_MAP[key]
    raise ValueError(f"Unknown source_type value: {raw!r}")


def resolve_separator(raw) -> str:
    if raw is None or pd.isna(raw):
        return ""
    raw_str = str(raw)
    key = raw_str.strip().lower()
    if key in ("", "nan", "none", "null"):
        return ""
    if key in SEPARATOR_KEYWORDS:
        return SEPARATOR_KEYWORDS[key]
    return raw_str


def build_attr_lookup(columns: List[str]) -> Dict[str, str]:
    """
    Construit une table de correspondance tolérante pour les noms de colonnes.
    """
    mapping: Dict[str, str] = {}
    suffixes = ["_standard_en", "_standard_fr"]

    for col in columns:
        if not col:
            continue
        col_clean = normalize_string(col)
        col_key = canon_key(col_clean)

        if col_key not in mapping:
            mapping[col_key] = col_clean

        col_lower = col_clean.lower()
        for suf in suffixes:
            if col_lower.endswith(suf):
                base = col_lower[: -len(suf)]
                base = base.rstrip("_")
                base_key = canon_key(base)
                if base_key and base_key not in mapping:
                    mapping[base_key] = col_clean

    return mapping


def get_attribute_value(row: pd.Series, attr_name: str, attr_lookup: Dict[str, str]) -> Optional[str]:
    if not attr_name:
        return None

    # Colonne présente telle quelle
    if attr_name in row.index:
        return row[attr_name]

    # Lookup canonique
    attr_key = canon_key(attr_name)
    if attr_key in attr_lookup:
        col = attr_lookup[attr_key]
        if col in row.index:
            return row[col]

    # Fallback : scan des colonnes avec clef canonique
    for col in row.index:
        if canon_key(col) == attr_key:
            return row[col]

    return None


def build_short_description_for_row(row: pd.Series, recipes: pd.DataFrame, attr_lookup: Dict[str, str]) -> str:
    """
    Construit la short description pour une ligne de produit.
    """
    product_type = normalize_string(row.get(COL_PRODUCT_TYPE, ""))
    if not product_type:
        return ""

    pt_key = canon_match_key(product_type)
    applicable = recipes[recipes["product_type"].apply(canon_match_key) == pt_key]
    if applicable.empty:
        return ""

    applicable = applicable.sort_values("order")
    parts = []

    for _, rec in applicable.iterrows():
        source_type_raw = rec["source_type"]
        attr_name = rec["attribute_name"]
        sep_raw = rec["separator_after"]

        try:
            source_type = resolve_source_type(source_type_raw)
        except ValueError:
            # Valeur de source_type inconnue → on ignore cette ligne de recette
            continue

        if source_type == "ATTRIBUTE_VALUE":
            value = get_attribute_value(row, attr_name, attr_lookup)
        elif source_type == "ATTRIBUTE_NAME":
            value = attr_name
        else:
            value = None

        if is_empty_value(value):
            continue

        value_str = str(value).strip()
        parts.append(value_str)

        sep = resolve_separator(sep_raw)
        if sep:
            parts.append(sep)

    if not parts:
        return ""

    # Évite de finir sur un séparateur seul
    last = str(parts[-1])
    if last.strip() in {",", ":", "-", ".", "•"}:
        parts = parts[:-1]

    text = "".join(str(p) for p in parts).strip()
    return text


def process_language(standardized_df: pd.DataFrame, recipes: pd.DataFrame, lang_label: str) -> pd.DataFrame:
    """
    Applique les recettes pour une langue donnée.
    """
    for col in [COL_SKU, COL_PRODUCT_TYPE]:
        if col not in standardized_df.columns:
            raise ValueError(f"Missing required column '{col}' in standardized data for language {lang_label}.")

    attr_lookup = build_attr_lookup(list(standardized_df.columns))

    out_rows = []
    for _, row in standardized_df.iterrows():
        short_desc = build_short_description_for_row(row, recipes, attr_lookup)
        row_out = {
            COL_SKU: row[COL_SKU],
            COL_PRODUCT_TYPE: row[COL_PRODUCT_TYPE],
        }
        if ORIG_DESC_COL in standardized_df.columns:
            row_out[ORIG_DESC_COL] = row[ORIG_DESC_COL]

        row_out[COL_SHORT_DESC] = short_desc
        out_rows.append(row_out)

    return pd.DataFrame(out_rows)


# --------------------------------------------------
# CONFIG STREAMLIT GÉNÉRALE
# --------------------------------------------------

st.set_page_config(page_title="Standardix", layout="wide")

st.title("Standardix – Outils eCommerce")

tool = st.sidebar.radio(
    "Choisissez un outil :",
    [
        "Standardiser les attributs",
        "Générer des descriptions courtes",
    ],
)

# --------------------------------------------------
# OUTIL 1 – STANDARDISATION DES ATTRIBUTS
# --------------------------------------------------
if tool == "Standardiser les attributs":
    st.header("🧩 Standardisation des attributs")

    uploaded_products = st.file_uploader(
        "Déposez votre fichier fournisseur (CSV ou Excel)",
        type=["csv", "xlsx", "xls"],
        key="products_std",
    )
    uploaded_mapping = st.file_uploader(
        "Déposez votre fichier de mapping (CSV ou Excel)",
        type=["csv", "xlsx", "xls"],
        key="mapping_std",
    )

    # --- Bouton en haut + placeholders juste en dessous ---
    start_standardization = st.button("Lancer la standardisation")
    status_placeholder = st.empty()
    download_placeholder = st.empty()

    # --- Texte & options de standardisation des mesures ---
    st.markdown(
        """
**Standardisation des mesures (pouces / cm)**  

Les mesures, s'il y en a à votre fichier, seront standardisées par défaut en **pouces en fraction (cm)**,  
par exemple : `1-1/4 po (3,18 cm)`.
        """
    )

    show_advanced_measures = st.checkbox(
        "Afficher les options avancées pour les mesures (pouces / centimètres)"
    )

    # Valeurs par défaut : fraction + 2 décimales + pouces + cm
    measure_options = {
        "mode_format": "fraction",   # 'fraction' ou 'decimale'
        "dec_places": 2,             # nb de décimales pour cm / pouces décimaux
        "add_unit": True,
        "unit_final": "les deux",    # pouces + cm par défaut
    }

    if show_advanced_measures:
        format_pouces = st.selectbox(
            "Format des pouces",
            options=[
                "Fraction (ex. 1-1/4)",
                "Décimal (ex. 1,25)",
            ],
            index=0,
        )
        if "Décimal" in format_pouces:
            measure_options["mode_format"] = "decimale"
        else:
            measure_options["mode_format"] = "fraction"

        dec_places = st.number_input(
            "Nombre de chiffres après la virgule pour les cm et les pouces en décimales",
            min_value=0,
            max_value=4,
            value=2,
            step=1,
        )
        measure_options["dec_places"] = int(dec_places)

        # Unité finale : pouces / cm / les deux
        unite_finale = st.selectbox(
            "Unité finale des mesures",
            options=[
                "Pouces + centimètres",
                "Seulement pouces",
                "Seulement centimètres",
            ],
            index=0,
        )
        if "Seulement pouces" in unite_finale:
            measure_options["unit_final"] = "in"
        elif "Seulement centimètres" in unite_finale:
            measure_options["unit_final"] = "cm"
        else:
            measure_options["unit_final"] = "les deux"

    if start_standardization:
        if uploaded_products and uploaded_mapping:

            with st.spinner("Standardisation en cours..."):
                # 1) Standardisation → DataFrames EN / FR
                df_en, df_fr = standardix(
                    uploaded_products,
                    uploaded_mapping,
                    measure_options=measure_options,
                )

                # 2) Lire le fichier fournisseur pour récupérer l'ordre initial
                df_products = read_table(uploaded_products)
                original_cols = list(df_products.columns)

                # 3) Réordonner les colonnes standardisées :
                #    -> elles suivent l'ordre des colonnes d'origine
                std_cols_en = []
                for col in original_cols:
                    cand = f"{col}_standard_en"
                    if cand in df_en.columns:
                        std_cols_en.append(cand)
                df_en = df_en[original_cols + std_cols_en]

                std_cols_fr = []
                for col in original_cols:
                    cand = f"{col}_standard_fr"
                    if cand in df_fr.columns:
                        std_cols_fr.append(cand)
                df_fr = df_fr[original_cols + std_cols_fr]

                # 4) Écriture dans un Excel en mémoire
                buffer = BytesIO()
                with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                    df_en.to_excel(writer, sheet_name="EN", index=False)
                    df_fr.to_excel(writer, sheet_name="FR", index=False)

                buffer.seek(0)

                # 5) Recharger le fichier pour colorer les en-têtes
                wb = load_workbook(buffer)

                green_fill = PatternFill(start_color="00C6EFCE", end_color="00C6EFCE", fill_type="solid")
                red_fill = PatternFill(start_color="00FFC7CE", end_color="00FFC7CE", fill_type="solid")

                # Colonnes qui ne doivent jamais être en rouge
                never_red = {"sku", "Short Description"}

                # ---------- FEUILLE EN ----------
                ws_en = wb["EN"]

                for col_idx, col_name in enumerate(df_en.columns, start=1):

                    # 1) Colonne standardisée → VERT
                    if col_name.endswith("_standard_en"):
                        ws_en.cell(row=1, column=col_idx).fill = green_fill
                        continue

                    # 2) Colonne d’origine → ROUGE SEULEMENT SI elle n’a pas été standardisée
                    if col_name in original_cols and col_name not in never_red:
                        std_version = f"{col_name}_standard_en"
                        if std_version not in df_en.columns:
                            ws_en.cell(row=1, column=col_idx).fill = red_fill

                # ---------- FEUILLE FR ----------
                ws_fr = wb["FR"]

                for col_idx, col_name in enumerate(df_fr.columns, start=1):

                    # 1) Colonne standardisée → VERT
                    if col_name.endswith("_standard_fr"):
                        ws_fr.cell(row=1, column=col_idx).fill = green_fill
                        continue

                    # 2) Colonne d’origine → ROUGE SEULEMENT SI elle n’a pas été standardisée
                    if col_name in original_cols and col_name not in never_red:
                        std_version = f"{col_name}_standard_fr"
                        if std_version not in df_fr.columns:
                            ws_fr.cell(row=1, column=col_idx).fill = red_fill

                # 6) Sauvegarde finale
                output = BytesIO()
                wb.save(output)
                output.seek(0)

            # ✅ Message + bouton apparaissent juste sous le bouton
            status_placeholder.success("✅ Standardisation terminée. Vous pouvez télécharger le fichier.")

            download_placeholder.download_button(
                "📥 Télécharger le fichier standardisé (Excel)",
                data=output,
                file_name="products_standardized.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        else:
            status_placeholder.error("Veuillez téléverser les deux fichiers (fournisseur et mapping).")

# --------------------------------------------------
# OUTIL 2 – GÉNÉRATION DES DESCRIPTIONS COURTES
# --------------------------------------------------
else:
    st.header("✏️ Générer des descriptions courtes")

    st.markdown(
        """
        1. Téléverse le **fichier standardisé** (sorti de l'outil précédent, avec les onglets EN / FR).  
        2. Téléverse le **fichier de recettes** (`short_description_recipes.xlsx`, avec Recipe_EN et Recipe_FR).  
        3. Clique sur le bouton pour générer un Excel avec les courtes descriptions EN / FR.
        """
    )

    uploaded_standardized = st.file_uploader(
        "Fichier standardisé (Excel, avec onglets EN et FR)",
        type=["xlsx", "xls"],
        key="standardized_shortdesc",
    )

    uploaded_recipes = st.file_uploader(
        "Fichier de recettes (short_description_recipes.xlsx)",
        type=["xlsx", "xls"],
        key="recipes_shortdesc",
    )

    if st.button("Générer les descriptions courtes"):
        if not uploaded_standardized or not uploaded_recipes:
            st.error("Merci de téléverser **les 2 fichiers** (standardisé + recettes).")
        else:
            try:
                # ----- Lecture des fichiers uploadés -----
                std_sheets = pd.read_excel(
                    uploaded_standardized,
                    sheet_name=[SHEET_EN, SHEET_FR],
                )
                en_std = std_sheets[SHEET_EN]
                fr_std = std_sheets[SHEET_FR]

                recipe_sheets = pd.read_excel(
                    uploaded_recipes,
                    sheet_name=[RECIPE_SHEET_EN, RECIPE_SHEET_FR],
                )
                recipes_en_raw = recipe_sheets[RECIPE_SHEET_EN]
                recipes_fr_raw = recipe_sheets[RECIPE_SHEET_FR]

                recipes_en = load_recipes(recipes_en_raw, RECIPE_SHEET_EN)
                recipes_fr = load_recipes(recipes_fr_raw, RECIPE_SHEET_FR)

                en_out = process_language(en_std, recipes_en, "EN")
                fr_out = process_language(fr_std, recipes_fr, "FR")

                # ----- Écriture dans un Excel en mémoire -----
                output = BytesIO()
                with pd.ExcelWriter(output, engine="openpyxl") as writer:
                    en_out.to_excel(writer, sheet_name="EN", index=False)
                    fr_out.to_excel(writer, sheet_name="FR", index=False)

                output.seek(0)

                # ----- Coloration des en-têtes -----
                wb = load_workbook(output)

                green_fill = PatternFill(
                    start_color="00C6EFCE",
                    end_color="00C6EFCE",
                    fill_type="solid",
                )
                red_fill = PatternFill(
                    start_color="00FFC7CE",
                    end_color="00FFC7CE",
                    fill_type="solid",
                )

                for sheet_name, df in [("EN", en_out), ("FR", fr_out)]:
                    ws = wb[sheet_name]
                    cols = list(df.columns)
                    for col_idx, col_name in enumerate(cols, start=1):
                        if col_name == COL_SHORT_DESC:  # short_description (nouvelle)
                            ws.cell(row=1, column=col_idx).fill = green_fill
                        elif col_name == ORIG_DESC_COL:  # Short Description (origine)
                            ws.cell(row=1, column=col_idx).fill = red_fill

                # ----- Sauvegarde finale -----
                final_output = BytesIO()
                wb.save(final_output)
                final_output.seek(0)

                st.success("✅ Descriptions courtes générées.")
                st.download_button(
                    "📥 Télécharger le fichier avec descriptions courtes",
                    data=final_output,
                    file_name="products_with_short_description.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

            except Exception as e:
                st.error(f"Une erreur est survenue : {e}")

