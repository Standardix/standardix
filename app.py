import streamlit as st
import pandas as pd
from io import BytesIO
from openpyxl import load_workbook
from openpyxl.styles import PatternFill

from standardix_engine import standardix

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
        # 1) Standardisation -> DataFrames EN / FR
        df_en, df_fr = standardix(uploaded_products, uploaded_mapping)

        # 2) Écriture dans un Excel en mémoire
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df_en.to_excel(writer, sheet_name="EN", index=False)
            df_fr.to_excel(writer, sheet_name="FR", index=False)

        buffer.seek(0)

        # 3) Recharger le classeur pour colorer les en-têtes standardisées en vert
        wb = load_workbook(buffer)
        green_fill = PatternFill(
            start_color="00C6EFCE", end_color="00C6EFCE", fill_type="solid"
        )

        # Colonnes fournisseur (sans couleur)
        initial_cols = [
            "sku",
            "size_supplier",
            "color_supplier",
            "material_supplier",
            "gender_supplier",
        ]

        # EN
        ws_en = wb["EN"]
        for col_idx, col_name in enumerate(df_en.columns, start=1):
            if col_name not in initial_cols:
                cell = ws_en.cell(row=1, column=col_idx)
                cell.fill = green_fill

        # FR
        ws_fr = wb["FR"]
        for col_idx, col_name in enumerate(df_fr.columns, start=1):
            if col_name not in initial_cols:
                cell = ws_fr.cell(row=1, column=col_idx)
                cell.fill = green_fill

        # 4) Sauvegarde finale dans un nouveau buffer
        output = BytesIO()
        wb.save(output)
        output.seek(0)

        st.download_button(
            "Télécharger le fichier standardisé (Excel)",
            data=output,
            file_name="products_standardized.xlsx",
            mime=(
                "application/"
                "vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            ),
        )
    else:
        st.error("Veuillez téléverser les deux fichiers (fournisseur et mapping).")
