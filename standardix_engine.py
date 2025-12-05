import pandas as pd
import re


def read_table(file_like):
    """Lit un CSV ou un Excel (xlsx/xls) en fonction de l'extension."""
    name = getattr(file_like, "name", "").lower()

    if name.endswith((".xlsx", ".xls")):
        df = pd.read_excel(file_like, dtype=str)
    else:
        # On suppose CSV par défaut
        df = pd.read_csv(file_like, dtype=str, sep=None, engine="python")

    # Nettoyage des noms de colonnes
    df.columns = [col.strip().lstrip("\ufeff") for col in df.columns]
    return df


def clean_text(val):
    if pd.isna(val):
        return None
    return str(val).strip().lower()


def load_mapping(df_map):
    """Valide et prépare le fichier de mapping déjà chargé en DataFrame."""
    required_cols = {"attribute", "source", "standard_en", "standard_fr", "match_type"}
    missing = required_cols - set(df_map.columns)
    if missing:
        raise ValueError(f"Colonnes manquantes dans le mapping : {missing}")
    return df_map


def build_rules(df_map, attribute):
    """Construit les règles pour un attribut donné (size, color, etc.)."""
    df = df_map[df_map["attribute"] == attribute].copy()
    if df.empty:
        return {}, {}, []

    df["source_norm"] = df["source"].apply(clean_text)

    df_exact = df[df["match_type"] == "exact"]
    df_regex = df[df["match_type"] == "regex"]

    exact_en = dict(zip(df_exact["source_norm"], df_exact["standard_en"]))
    exact_fr = dict(zip(df_exact["source_norm"], df_exact["standard_fr"]))

    regex_rules = []
    for _, row in df_regex.iterrows():
        pattern_text = row["source_norm"]
        try:
            pattern = re.compile(pattern_text)
            regex_rules.append((pattern, row["standard_en"], row["standard_fr"]))
        except re.error:
            # On ignore les regex invalides
            continue

    return exact_en, exact_fr, regex_rules


def apply_rules(series, exact_en, exact_fr, regex_rules):
    out_en = []
    out_fr = []

    for v in series:
        # 🔹 VRAIS BLANCS (Excel -> NaN, ou chaîne vide) → on laisse vide
        if pd.isna(v) or (isinstance(v, str) and v.strip() == ""):
            out_en.append("")
            out_fr.append("")
            continue

        norm = clean_text(v)
        en = exact_en.get(norm)
        fr = exact_fr.get(norm)

        # Si pas de match exact, on teste les regex
        if en is None and norm:
            for pattern, sen, sfr in regex_rules:
                if pattern.fullmatch(norm):
                    en, fr = sen, sfr
                    break

        # 🔹 Valeur présente mais non trouvée dans le mapping
        out_en.append(en if en is not None else "UNDEFINITE")
        out_fr.append(fr if fr is not None else "NON_MAPPÉ")

    return out_en, out_fr


def standardix(products_file, mapping_file):
    """
    Point d'entrée principal :
    - products_file : CSV/XLSX fournisseur
    - mapping_file : CSV/XLSX mapping
    Renvoie deux DataFrames : (df_en, df_fr).
    """
    # Lecture des fichiers
    df_products = read_table(products_file)
    df_map = read_table(mapping_file)

    # S'assurer que match_type existe
    if "match_type" not in df_map.columns:
        df_map["match_type"] = "exact"
    df_map["match_type"] = df_map["match_type"].fillna("exact").str.lower()

    df_map = load_mapping(df_map)

    # Attributs gérés
    attributes = [
        ("size", "size_supplier"),
        ("color", "color_supplier"),
        ("material", "material_supplier"),
        ("gender", "gender_supplier"),
    ]

    df_en = df_products.copy()
    df_fr = df_products.copy()

    for attr, src_col in attributes:
        if src_col not in df_products.columns:
            continue

        exact_en, exact_fr, regex_rules = build_rules(df_map, attr)
        std_en, std_fr = apply_rules(df_products[src_col], exact_en, exact_fr, regex_rules)

        df_en[f"{attr}_standard_en"] = std_en
        df_fr[f"{attr}_standard_fr"] = std_fr

    return df_en, df_fr
