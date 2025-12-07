import streamlit as st
import pandas as pd
from io import BytesIO
from openpyxl import load_workbook
from openpyxl.styles import PatternFill

from standardix_engine import standardix, read_table

st.title("Standardix – Standardisation des attributs")

uploaded_products = st.file_uploader(
    "Déposez votre fichier fournisseur (CSV ou Excel)",
    type=["csv", "xlsx", "xls"],
)
uploaded_mapping = st.file_uploader(
    "Déposez votre fichier de mapping (CSV ou Excel)",
    type=["csv", "xlsx", "xls"],
)

if st.button("Lancer la standardisation"):
    if uploaded_products and uploaded_mapping:

        # 1) Standardisation → DataFrames EN / FR
        df_en, df_fr = standardix(uploaded_products, uploaded_mapping)

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
        red_fill   = PatternFill(start_color="00FFC7CE", end_color="00FFC7CE", fill_type="solid")

        # Colonnes qui ne doivent jamais être en rouge
        never_red = {"sku"}

        # ---------- FEUILLE EN ----------
        ws_en = wb["EN"]

        # Entêtes en VERT seulement pour colonnes standard_en
        for col_idx, col_name in enumerate(df_en.columns, start=1):
            if col_name.endswith("_standard_en"):
                ws_en.cell(row=1, column=col_idx).fill = green_fill

        # Entêtes en ROUGE si colonne d'origine non standardisée
        for col_idx, col_name in enumerate(df_en.columns, start=1):
            if col_name in original_cols and col_name not in never_red:
                if f"{col_name}_standard_en" not in df_en.columns:
                    ws_en.cell(row=1, column=col_idx).fill = red_fill

        # ---------- FEUILLE FR ----------
        ws_fr = wb["FR"]

        # Entêtes en VERT seulement pour colonnes standard_fr
        for col_idx, col_name in enumerate(df_fr.columns, start=1):
            if col_name.endswith("_standard_fr"):
                ws_fr.cell(row=1, column=col_idx).fill = green_fill

        # Entêtes en ROUGE si colonne d'origine non standardisée
        for col_idx, col_name in enumerate(df_fr.columns, start=1):
            if col_name in original_cols and col_name not in never_red:
                if f"{col_name}_standard_fr" not in df_fr.columns:
                    ws_fr.cell(row=1, column=col_idx).fill = red_fill

        # 6) Sauvegarde finale
        output = BytesIO()
        wb.save(output)
        output.seek(0)

        st.download_button(
            "Télécharger le fichier standardisé (Excel)",
            data=output,
            file_name="products_standardized.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    else:
        st.error("Veuillez téléverser les deux fichiers (fournisseur et mapping).")
