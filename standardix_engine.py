import pandas as pd
import re
from pathlib import Path

def clean_text(val):
    if pd.isna(val):
        return None
    return str(val).strip().lower()

def load_mapping(mapping_file):
    df_map = pd.read_csv(mapping_file, dtype=str, sep=None, engine="python")
    df_map.columns = [col.strip().lstrip("\ufeff") for col in df_map.columns]

    if "match_type" not in df_map.columns:
        df_map["match_type"] = "exact"

    required_cols = {"attribute","source","standard_en","standard_fr","match_type"}
    if not required_cols.issubset(df_map.columns):
        raise ValueError(f"Missing columns in mapping file")

    return df_map

def build_rules(df_map, attribute):
    df = df_map[df_map["attribute"] == attribute].copy()
    if df.empty:
        return {}, {}, []

    df["source_norm"] = df["source"].apply(clean_text)
    exact = df[df["match_type"] == "exact"]
    regex = df[df["match_type"] == "regex"]

    exact_en = dict(zip(exact["source_norm"], exact["standard_en"]))
    exact_fr = dict(zip(exact["source_norm"], exact["standard_fr"]))

    regex_rules = []
    for _, row in regex.iterrows():
        try:
            pattern = re.compile(row["source_norm"])
            regex_rules.append((pattern, row["standard_en"], row["standard_fr"]))
        except:
            pass

    return exact_en, exact_fr, regex_rules

def apply_rules(series, exact_en, exact_fr, regex_rules):
    out_en, out_fr = [], []
    for val in series:
        norm = clean_text(val)
        en = exact_en.get(norm)
        fr = exact_fr.get(norm)

        # regex fallback
        if en is None and norm:
            for (pattern, e, f) in regex_rules:
                if pattern.fullmatch(norm):
                    en, fr = e, f
                    break

        out_en.append(en if en else "UNMAPPED")
        out_fr.append(fr if fr else "NON_MAPPÉ")

    return out_en, out_fr

def standardix(products_file, mapping_file):
    df_products = pd.read_csv(products_file, dtype=str, sep=None, engine="python")
    df_products.columns = [col.strip().lstrip("\ufeff") for col in df_products.columns]

    df_map = load_mapping(mapping_file)

    attributes = [
        ("size", "size_supplier"),
        ("color", "color_supplier"),
        ("material", "material_supplier"),
        ("gender", "gender_supplier")
    ]

    # copies for EN/FR sheets
    df_en = df_products.copy()
    df_fr = df_products.copy()

    for attr, src in attributes:
        if src not in df_products.columns:
            continue

        exact_en, exact_fr, regex_rules = build_rules(df_map, attr)
        std_en, std_fr = apply_rules(df_products[src], exact_en, exact_fr, regex_rules)

        df_en[f"{attr}_standard_en"] = std_en
        df_fr[f"{attr}_standard_fr"] = std_fr

    return df_en, df_fr
