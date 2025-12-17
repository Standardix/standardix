import pandas as pd
import re
import math
from fractions import Fraction
from decimal import Decimal, ROUND_HALF_UP


# ==========================================================
# 1. Helpers GENERIQUES
# ==========================================================

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


# ==========================================================
# 2. LOGIQUE DE MAPPING CLASSIQUE (exact / regex)
# ==========================================================

def build_rules(df_map, attribute):
    """Construit les règles pour un attribut donné (exact/regex)."""
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
            # Ignore les regex invalides
            continue

    return exact_en, exact_fr, regex_rules


def apply_rules(series, exact_en, exact_fr, regex_rules):
    """
    Applique les règles exact/regex pour une série de valeurs.
    - Valeurs vides → vide
    - Valeurs non trouvées → UNDEFINITE / NON_MAPPÉ
    """
    out_en = []
    out_fr = []

    for v in series:
        # 🔹 Valeurs vides → on laisse vide
        if pd.isna(v) or (isinstance(v, str) and v.strip() == ""):
            out_en.append("")
            out_fr.append("")
            continue

        norm = clean_text(v)
        en = exact_en.get(norm)
        fr = exact_fr.get(norm)

        # 🔹 Si pas de match exact → tester les regex
        if en is None and norm:
            for pattern, sen, sfr in regex_rules:
                if pattern.fullmatch(norm):
                    en, fr = sen, sfr
                    break

        # 🔹 Valeur présente mais non trouvée dans le mapping
        out_en.append(en if en is not None else "UNDEFINITE")
        out_fr.append(fr if fr is not None else "NON_MAPPÉ")

    return out_en, out_fr


# ==========================================================
# 2b. Fallback via Short Description (exact / exact + "'s")
# ==========================================================

def _canon_shortdesc(text):
    """Normalise une short description pour la recherche (minuscule, espaces normalisés)."""
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return ""
    s = str(text).strip().lower()
    # normalise espaces
    s = re.sub(r"\s+", " ", s)
    return s


def build_shortdesc_exact_patterns(exact_source_norms):
    """
    Compile des patterns regex pour détecter des matches EXACTS dans la short description:
    - source
    - source + "'s" (ou "’s")
    Le match est "standalone" (bornes non-alphanum).

    ✅ Correctifs:
      1) Prioriser les termes PLUS LONGS d'abord (ex: "2x-large" avant "large")
      2) Empêcher les tailles 1 lettre (S/M/L) de matcher dans "Men's" / "Women's"
         (apostrophe avant le S)
    """
    patterns = []

    # ✅ Prioriser les sources les plus longues d'abord
    bases = [b for b in exact_source_norms if b]
    bases = sorted(bases, key=lambda x: len(str(x)), reverse=True)

    for base in bases:
        base = str(base).strip().lower()
        if not base:
            continue

        # ✅ Cas spécial: base d'une seule lettre (ex: "s")
        # - Empêche de matcher après ' ou ’ (ex: Men’s)
        # - Toujours "standalone" (pas au milieu d'un mot)
        if len(base) == 1:
            pat = re.compile(r"(?<![\w'’])" + re.escape(base) + r"(?!\w)")
            patterns.append((pat, base))
            continue

        # Cas normal: autorise éventuellement "'s" / "’s" après le mot trouvé
        # (?<!\w) ... (?!\w) empêche de matcher au milieu d'un mot
        pat = re.compile(r"(?<!\w)" + re.escape(base) + r"(?:'s|’s)?(?!\w)")
        patterns.append((pat, base))

    return patterns


def infer_source_norm_from_shortdesc(short_desc, compiled_patterns):
    """Retourne la clé (source_norm) à utiliser si un pattern match, sinon None."""
    s = _canon_shortdesc(short_desc)
    if not s:
        return None
    for pat, base in compiled_patterns:
        if pat.search(s):
            return base
    return None


def apply_rules_with_shortdesc_fallback(series, short_desc_series, exact_en, exact_fr, regex_rules):
    """
    Variante de apply_rules:
    - Si la cellule est vide dans la colonne attribut,
      on tente de détecter un match exact (ou exact + "'s") dans Short Description
      par rapport à la colonne 'source' du mapping (match_type='exact').
    - Si trouvé, on utilise directement standard_en / standard_fr correspondants.
    - Sinon, même comportement que apply_rules.
    """
    out_en = []
    out_fr = []

    # patterns basés sur les clés exactes déjà normalisées
    compiled_patterns = build_shortdesc_exact_patterns(list(exact_en.keys()))

    for i, v in enumerate(series):
        # 🔹 Valeurs vides → tenter fallback short description
        if pd.isna(v) or (isinstance(v, str) and v.strip() == ""):
            sd = None
            if short_desc_series is not None and i < len(short_desc_series):
                sd = short_desc_series.iloc[i]
            inferred = infer_source_norm_from_shortdesc(sd, compiled_patterns) if compiled_patterns else None

            if inferred is not None:
                en = exact_en.get(inferred)
                fr = exact_fr.get(inferred)
                out_en.append(en if en is not None else "UNDEFINITE")
                out_fr.append(fr if fr is not None else "NON_MAPPÉ")
            else:
                out_en.append("")
                out_fr.append("")
            continue

        norm = clean_text(v)
        en = exact_en.get(norm)
        fr = exact_fr.get(norm)

        # 🔹 Si pas de match exact → tester les regex
        if en is None and norm:
            for pattern, sen, sfr in regex_rules:
                if pattern.fullmatch(norm):
                    en, fr = sen, sfr
                    break

        # 🔹 Valeur présente mais non trouvée dans le mapping
        out_en.append(en if en is not None else "UNDEFINITE")
        out_fr.append(fr if fr is not None else "NON_MAPPÉ")

    return out_en, out_fr


# ==========================================================
# 3. LOGIQUE SPÉCIFIQUE MESURES (pouces / cm)
# ==========================================================

UNIT_ALIASES = {
    "in": ["in", "inch", "inches", '"', "po", "pouce", "pouces"],
    "ft": ["ft", "foot", "feet", "'"],
    "cm": ["cm", "centimeter", "centimeters", "centimètre", "centimètres"],
    "mm": ["mm", "millimeter", "millimeters", "millimètre", "millimètres"],
    "m": ["m", "meter", "meters", "mètre", "mètres"],
}


def detect_inline_unit(text: str):
    """Essaie de détecter l'unité dans une chaîne libre (in, po, cm, etc.)."""
    t = text.strip()
    tl = t.lower()

    # Si ' et " apparaissent → très probablement pied-pouce
    if ("'" in t) and ('"' in t):
        return "ft"

    for canon, aliases in UNIT_ALIASES.items():
        for a in aliases:
            if a in ["'", '"']:
                if a in t:
                    return canon
            else:
                # 1) Cas classique : unité séparée (avec espaces)
                if re.search(rf'\b{re.escape(a)}\b', tl, flags=re.IGNORECASE):
                    return canon

                # 2) Cas "7.69in" ou "45cm" (unité collée après le nombre)
                if tl.endswith(a.lower()):
                    prefix = tl[:-len(a)].rstrip()
                    if prefix and prefix[-1].isdigit():
                        return canon
    return None


def remove_units_case_insensitive(text: str):
    """Retire les tokens d'unités connus (mots et symboles) sans tenir compte de la casse."""
    t = text

    for canon, aliases in UNIT_ALIASES.items():
        for a in aliases:
            if a in ["'", '"']:
                t = t.replace(a, "")
            else:
                # 1) Cas classique : unité séparée (avec espaces)
                t = re.sub(rf'\b{re.escape(a)}\b', '', t, flags=re.IGNORECASE)

                # 2) Cas "7.69in" ou "45cm" (unité collée après le nombre)
                lower_a = a.lower()
                tl = t.lower()
                if tl.endswith(lower_a):
                    prefix = t[:-len(a)]
                    # on regarde le dernier caractère non-espace avant l'unité
                    i = len(prefix) - 1
                    while i >= 0 and prefix[i].isspace():
                        i -= 1
                    if i >= 0 and prefix[i].isdigit():
                        # on coupe l'unité (on garde le nombre + éventuels espaces avant)
                        t = prefix[:i+1]

    return t


def clean_numeric_part(text: str, keep_spaces=False):
    """
    - Remplace la virgule par un point
    - Retire les unités (mots/symboles)
    - Normalise les espaces
    """
    t = text.strip().replace(",", ".")
    t = remove_units_case_insensitive(t)
    if keep_spaces:
        t = re.sub(r"\s+", " ", t)
    else:
        t = re.sub(r"\s+", "", t)
    return t


def parse_feet_inches_pattern(raw_text: str):
    """
    Parse des formes du type :
    - 6'
    - 4' 3-1/2"
    - 4 ft 3.25 in
    Renvoie la valeur en pouces (float) ou None si non reconnu.
    """
    s = raw_text.strip().replace(",", ".")
    s = re.sub(r"\s+", " ", s)

    # pieds seuls: 6' ou 6 ft
    m = re.match(r"^(-?\d+)\s*(?:'|ft)\s*$", s, flags=re.IGNORECASE)
    if m:
        ft = int(m.group(1))
        return ft * 12.0

    # pieds + pouces décimaux: 4' 3.25" ou 4 ft 3.25 in
    m = re.match(r"^(-?\d+)\s*(?:'|ft)\s*(\d+(?:\.\d+)?)\s*(?:\"|in)\s*$", s, flags=re.IGNORECASE)
    if m:
        ft = int(m.group(1))
        inches = float(m.group(2))
        return ft * 12.0 + inches

    # pieds + pouces fractionnaires: 4' 3-1/2"
    m = re.match(r"^(-?\d+)\s*(?:'|ft)\s*(\d+)-(\d+)\/(\d+)\s*(?:\"|in)\s*$", s, flags=re.IGNORECASE)
    if m:
        ft = int(m.group(1))
        whole_in = int(m.group(2))
        num = int(m.group(3))
        den = int(m.group(4))
        return ft * 12.0 + (whole_in + num / den)

    return None


def parse_value_to_inches(value_str, unit_hint=None):
    """
    Parse décimales, fractions, mixtes, pied-pouce.
    Renvoie la valeur en pouces (float) ou None.
    """
    if value_str is None or str(value_str).strip() == "":
        return None

    raw = str(value_str)

    # 1) Essai des formats pied-pouce
    fi = parse_feet_inches_pattern(raw)
    if fi is not None:
        return fi

    # 2) Détection de l'unité
    inline_unit = detect_inline_unit(raw)
    unit = inline_unit or (unit_hint.lower().strip() if isinstance(unit_hint, str) else None)

    # 3) Mixed fraction : "8 37/64" ou "8-37/64"
    t_mixed = clean_numeric_part(raw, keep_spaces=True)
    m = re.match(r"^(-?\d+)[\-\s](\d+)\/(\d+)$", t_mixed)
    if m:
        whole = int(m.group(1))
        num = int(m.group(2))
        den = int(m.group(3))
        val = whole + (num / den if whole >= 0 else -num / den)
    else:
        # 4) Fraction simple : "3/16"
        t = clean_numeric_part(raw, keep_spaces=False)
        m2 = re.match(r"^(-?\d+)\/(\d+)$", t)
        if m2:
            num = int(m2.group(1))
            den = int(m2.group(2))
            val = num / den
        else:
            # 5) Décimal ou entier
            try:
                val = float(t)
            except Exception:
                return None

    # 6) Conversion vers pouces
    if unit is None or unit == "in":
        inches = val
    elif unit == "ft":
        inches = val * 12.0
    elif unit == "cm":
        inches = val / 2.54
    elif unit == "mm":
        inches = val / 25.4
    elif unit == "m":
        inches = val * 39.37007874
    else:
        inches = val  # défaut : on considère que c'est déjà en pouces

    return inches


def round_to_sixteenth(inches):
    """Arrondit à la 1/16e de pouce la plus proche."""
    if inches is None:
        return None
    if isinstance(inches, float) and (math.isnan(inches) or math.isinf(inches)):
        return None
    return round(inches * 16) / 16


def inches_to_mixed_fraction(inches):
    """Transforme un nombre de pouces en entier + fraction (limité au 1/16e)."""
    if inches is None:
        return 0, 0, 1
    sign = -1 if inches < 0 else 1
    x = abs(inches)
    whole = int(math.floor(x + 1e-9))
    frac = x - whole
    frac_fraction = Fraction(frac).limit_denominator(16)
    num = frac_fraction.numerator
    den = frac_fraction.denominator
    if num == den:
        whole += 1
        num = 0
    whole *= sign
    return whole, num, den


def format_fraction_entier_trait_d_union(whole, num, den):
    """Formate 1 + 1/4 → '1-1/4', 0 + 3/16 → '3/16'."""
    if num == 0:
        return f"{whole}"
    return f"{whole}-{num}/{den}" if whole != 0 else f"{num}/{den}"


def format_decimal(value, locale="EN", places=2):
    """
    Arrondi HALF_UP avec Decimal, puis:
    - EN : séparateur .
    - FR : séparateur ,
    """
    q = Decimal(str(value)).quantize(Decimal("0." + "0" * places), rounding=ROUND_HALF_UP)
    if q == q.to_integral():
        s = f"{int(q)}"
    else:
        s = f"{q:.{places}f}"
    if locale.upper() == "FR":
        s = s.replace(".", ",")
    return s


def render_output(inches_rounded, mode_format, add_unit, unit_final,
                  locale="EN", dec_places=2):
    """
    Rend la chaîne finale selon les options :
    - mode_format: 'fraction' ou 'decimale'
    - unit_final: 'in', 'cm', 'les deux'
    - dec_places: nb de décimales pour les cm et les pouces décimaux
    """
    # Gestion des valeurs invalides / manquantes
    if inches_rounded is None:
        return ""
    if isinstance(inches_rounded, float) and (math.isnan(inches_rounded) or math.isinf(inches_rounded)):
        return ""

    # ----- Partie pouces -----
    if mode_format == "fraction":
        w, n, d = inches_to_mixed_fraction(inches_rounded)
        inch_side = format_fraction_entier_trait_d_union(w, n, d)
    else:
        inch_side = format_decimal(inches_rounded, locale=locale, places=dec_places)

    if add_unit and unit_final in ["in", "les deux"]:
        inch_unit = "in" if locale.upper() == "EN" else "po"
        inch_side = f"{inch_side} {inch_unit}"

    # ----- Partie cm -----
    cm_val = inches_rounded * 2.54
    cm_str = format_decimal(cm_val, locale=locale, places=dec_places)
    cm_side = cm_str
    if add_unit and unit_final in ["cm", "les deux"]:
        cm_side = f"{cm_side} cm"

    # ----- Combinaison finale -----
    if unit_final == "in":
        return inch_side
    elif unit_final == "cm":
        return cm_side
    else:
        # "les deux"
        return f"{inch_side} ({cm_side})"


def is_measurement_match_type(mt) -> bool:
    """
    Détermine si un match_type indique une standardisation 'pouces/cm'.
    Exemples supportés :
      - in-po-inch-pouce
      - in
      - inch
      - pouce
      - po
      (toutes variantes casse / séparateurs)
    """
    if mt is None or (isinstance(mt, float) and pd.isna(mt)):
        return False

    s = str(mt).strip().lower()
    if not s:
        return False

    # On découpe sur tout ce qui n'est pas une lettre
    tokens = re.split(r"[^a-z]+", s)
    tokens = [t for t in tokens if t]

    measurement_tokens = {"in", "inch", "inches", "po", "pouce", "pouces"}
    return any(t in measurement_tokens for t in tokens)


def standardize_measurement_series(series, options):
    """
    Standardise une série de valeurs de mesures :
    - valeurs vides → vide
    - valeurs non parseables → UNDEFINITE / NON_MAPPÉ
    - valeurs valides → format défini dans options
    """
    mode_format = options.get("mode_format", "fraction")  # 'fraction' ou 'decimale'
    dec_places = int(options.get("dec_places", 2))
    add_unit = bool(options.get("add_unit", True))
    unit_final = options.get("unit_final", "les deux")
    if unit_final not in ["in", "cm", "les deux"]:
        unit_final = "les deux"

    out_en = []
    out_fr = []

    for v in series:
        # Valeur vide → vide
        if pd.isna(v) or (isinstance(v, str) and v.strip() == ""):
            out_en.append("")
            out_fr.append("")
            continue

        inches = parse_value_to_inches(v, unit_hint=None)
        if inches is None:
            out_en.append("UNDEFINITE")
            out_fr.append("NON_MAPPÉ")
            continue

        inches_rounded = round_to_sixteenth(inches)
        if inches_rounded is None:
            out_en.append("UNDEFINITE")
            out_fr.append("NON_MAPPÉ")
            continue

        en_str = render_output(
            inches_rounded,
            mode_format=mode_format,
            add_unit=add_unit,
            unit_final=unit_final,
            locale="EN",
            dec_places=dec_places,
        )
        fr_str = render_output(
            inches_rounded,
            mode_format=mode_format,
            add_unit=add_unit,
            unit_final=unit_final,
            locale="FR",
            dec_places=dec_places,
        )

        out_en.append(en_str)
        out_fr.append(fr_str)

    return out_en, out_fr


# ==========================================================
# 4. FONCTION PRINCIPALE standardix
# ==========================================================

def standardix(products_file, mapping_file, measure_options=None):
    """
    Point d'entrée principal :
    - products_file : CSV/XLSX fournisseur
    - mapping_file : CSV/XLSX mapping
    - measure_options : dict pour la standardisation des mesures, ex :
        {
            "mode_format": "fraction" ou "decimale",
            "dec_places": 2,
            "add_unit": True,
            "unit_final": "les deux",
        }

    Renvoie deux DataFrames : df_en, df_fr.
    """
    if measure_options is None:
        measure_options = {
            "mode_format": "fraction",   # fraction par défaut
            "dec_places": 2,             # 2 décimales pour cm / décimal
            "add_unit": True,
            "unit_final": "les deux",    # pouces + cm
        }

    # Lecture des fichiers
    df_products = read_table(products_file)
    df_map = read_table(mapping_file)

    # S'assurer que match_type existe
    if "match_type" not in df_map.columns:
        df_map["match_type"] = "exact"
    df_map["match_type"] = df_map["match_type"].fillna("exact").str.lower()

    df_map = load_mapping(df_map)
    df_map["attribute"] = df_map["attribute"].astype(str).str.strip()

    # 🔹 Lookup insensible à la casse pour les colonnes produits
    product_cols = list(df_products.columns)
    col_lookup = {c.strip().lower(): c for c in product_cols}

    # 🔹 Colonne Short Description (fallback si valeur vide)
    _sd_col = col_lookup.get("short description")
    short_desc_series = df_products[_sd_col] if _sd_col else None

    # 🔹 Attributs dynamiques : viennent du mapping
    attribute_names = sorted(df_map["attribute"].dropna().unique())

    df_en = df_products.copy()
    df_fr = df_products.copy()

    for attr in attribute_names:
        key = str(attr).strip()
        if not key:
            continue

        # On cherche la colonne produit correspondante, sans tenir compte de la casse
        src_col = col_lookup.get(key.lower())
        if not src_col:
            # Attribut présent dans le mapping mais pas dans le fichier produits
            continue

        # Sous-ensemble du mapping pour cet attribut
        attr_rows = df_map[df_map["attribute"] == attr]

        # 1) Faut-il utiliser la logique de mesures ?
        has_measurement = any(is_measurement_match_type(mt) for mt in attr_rows["match_type"])

        if has_measurement:
            # 🔹 MODE MESURES (pouces/cm)
            std_en, std_fr = standardize_measurement_series(df_products[src_col], measure_options)
        else:
            # 🔹 MODE CLASSIQUE (exact / regex)
            exact_en, exact_fr, regex_rules = build_rules(df_map, attr)
            std_en, std_fr = apply_rules_with_shortdesc_fallback(
                df_products[src_col],
                short_desc_series,
                exact_en,
                exact_fr,
                regex_rules
            )

        # 🔹 On nomme les colonnes standard à partir du NOM RÉEL de la colonne produit
        df_en[f"{src_col}_standard_en"] = std_en
        df_fr[f"{src_col}_standard_fr"] = std_fr

    return df_en, df_fr
