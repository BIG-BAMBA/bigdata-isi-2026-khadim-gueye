"""
TP 3 -- Nettoyer et tester : un module de transformations PySpark
Big Data Engineering -- Master 1 -- DMI/FST/UCAD

Fonctions de nettoyage de la table clients. Chaque fonction prend un
DataFrame et retourne un DataFrame (utilisable avec .transform(...)).
"""

import unicodedata

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# ---------------------------------------------------------------------------
# 3.1 Manquants et email
# ---------------------------------------------------------------------------

def unifier_manquants(df: DataFrame) -> DataFrame:
    """Emails "" / "N/A" -> null."""
    e = F.trim(F.col("email"))
    return df.withColumn(
        "email",
        F.when(e.isin("", "N/A", "n/a", "NULL"), None).otherwise(e),
    )


def normaliser_email(df: DataFrame) -> DataFrame:
    """Email en minuscules + trim ; drapeau de validite."""
    motif = r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"
    df = df.withColumn("email", F.lower(F.trim(F.col("email"))))
    return df.withColumn(
        "email_valide",
        F.when(F.col("email").isNull(), F.lit(None))
         .otherwise(F.col("email").rlike(motif)),
    )


# ---------------------------------------------------------------------------
# 3.2 Ville (avec retrait d'accents)
# ---------------------------------------------------------------------------

def sans_accent(s):
    """Retire les accents d'une chaine (None -> None)."""
    if s is None:
        return None
    # NFKD decompose les caracteres accentues en (lettre + accent),
    # puis on ne garde que les octets ASCII (les accents disparaissent).
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


sans_accent_udf = F.udf(sans_accent, StringType())


def normaliser_ville(df: DataFrame) -> DataFrame:
    """ville (affichage, trim) + ville_norm (cle sans accent, minuscule, trim)."""
    df = df.withColumn("ville", F.trim(F.col("ville")))
    return df.withColumn(
        "ville_norm",
        sans_accent_udf(F.lower(F.trim(F.col("ville")))),
    )


# ---------------------------------------------------------------------------
# 3.3 Telephone et date de naissance
# ---------------------------------------------------------------------------

def normaliser_telephone(df: DataFrame) -> DataFrame:
    """9 chiffres, prefixe 70/75/76/77/78 ; drapeau de validite."""
    # On ne garde que les chiffres (retire espaces, points, tirets, "+")
    df = df.withColumn(
        "telephone_norm",
        F.regexp_replace(F.coalesce(F.col("telephone"), F.lit("")), r"[^0-9]", ""),
    )

    # Retire l'indicatif pays "221" quand il est present (12 chiffres -> 9)
    df = df.withColumn(
        "telephone_norm",
        F.when(
            (F.length("telephone_norm") == 12) & (F.col("telephone_norm").startswith("221")),
            F.expr("substring(telephone_norm, 4, 9)"),
        ).otherwise(F.col("telephone_norm")),
    )

    motif = r"^(70|75|76|77|78)[0-9]{7}$"
    df = df.withColumn(
        "telephone_valide",
        F.when(F.col("telephone_norm") == "", F.lit(None))
         .otherwise(F.col("telephone_norm").rlike(motif)),
    )
    return df


def valider_naissance(df: DataFrame) -> DataFrame:
    """Date plausible entre 1920 et aujourd'hui, sinon null.

    NB : suppose que la SparkSession est creee avec
    `spark.sql.ansi.enabled = false` (voir conftest.py / le notebook),
    sinon to_date() leve une exception sur une chaine invalide au lieu
    de renvoyer null.
    """
    df = df.withColumn("date_naissance", F.to_date(F.col("date_naissance")))
    borne_min = F.to_date(F.lit("1920-01-01"))
    return df.withColumn(
        "date_naissance",
        F.when(
            F.col("date_naissance").isNotNull()
            & (F.col("date_naissance") >= borne_min)
            & (F.col("date_naissance") <= F.current_date()),
            F.col("date_naissance"),
        ).otherwise(F.lit(None)),
    )


# ---------------------------------------------------------------------------
# 3.4 Deduplication (apres normalisation)
# ---------------------------------------------------------------------------

def dedupliquer_clients(df: DataFrame) -> DataFrame:
    """Doublons exacts puis 1 ligne par customer_id."""
    df = df.dropDuplicates()  # lignes strictement identiques sur toutes les colonnes
    df = df.dropDuplicates(["customer_id"])  # au plus une ligne par client
    return df


# ---------------------------------------------------------------------------
# 4. Pipeline complet
# ---------------------------------------------------------------------------

def nettoyer_clients(df: DataFrame) -> DataFrame:
    return (
        df.transform(unifier_manquants)
          .transform(normaliser_email)
          .transform(normaliser_ville)
          .transform(normaliser_telephone)
          .transform(valider_naissance)
          .transform(dedupliquer_clients)
    )
