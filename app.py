import streamlit as st
import pandas as pd
from standardix_engine import standardix

st.title("Standardix – Standardisation des attributs")

uploaded_products = st.file_uploader("Déposez votre fichier fournisseur (CSV)", type=["csv"])
uploaded_mapping = st.file_uploader("Déposez votre fichier de mapping (CSV)", type=["csv"])

if st.button("Lancer la standardisation"):
    if uploaded_products and uploaded_mapping:
        df_en, df_fr = standardix(uploaded_products, uploaded_mapping)

        # Convert dataframes to Excel in memory
        with pd.ExcelWriter("output.xlsx", engine="openpyxl") as writer:
            df_en.to_excel(writer, sheet_name="EN", index=False)
            df_fr.to_excel(writer, sheet_name="FR", index=False)

        with open("output.xlsx", "rb") as f:
            st.download_button("Télécharger le fichier standardisé", f, "products_standardized.xlsx")
    else:
        st.error("Veuillez téléverser les deux fichiers.")
