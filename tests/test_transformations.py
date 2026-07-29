import datetime

from pyspark.sql.types import StringType, StructField, StructType

from src.transformations import (
    dedupliquer_clients,
    nettoyer_clients,
    normaliser_email,
    normaliser_telephone,
    normaliser_ville,
    sans_accent,
    unifier_manquants,
    valider_naissance,
)


# ---------------------------------------------------------------------------
# sans_accent (fonction Python pure, pas besoin de Spark)
# ---------------------------------------------------------------------------

def test_sans_accent_retire_les_accents():
    assert sans_accent("Dakar") == "Dakar"
    assert sans_accent("Thiès") == "Thies"
    assert sans_accent("Saint-Louis") == "Saint-Louis"
    assert sans_accent("Ziguinchor") == "Ziguinchor"
    assert sans_accent("Nétéboulou") == "Neteboulou"


def test_sans_accent_none():
    assert sans_accent(None) is None


# ---------------------------------------------------------------------------
# unifier_manquants
# ---------------------------------------------------------------------------

def test_unifier_manquants_convertit_vide_et_na_en_null(spark):
    df = spark.createDataFrame(
        [("1", ""), ("2", "N/A"), ("3", "n/a"), ("4", "NULL"), ("5", "a@b.com")],
        ["customer_id", "email"],
    )
    resultat = unifier_manquants(df).collect()
    emails = {row.customer_id: row.email for row in resultat}
    assert emails["1"] is None
    assert emails["2"] is None
    assert emails["3"] is None
    assert emails["4"] is None
    assert emails["5"] == "a@b.com"


# ---------------------------------------------------------------------------
# normaliser_email
# ---------------------------------------------------------------------------

def test_normaliser_email_minuscule_et_trim(spark):
    df = spark.createDataFrame([("1", "  Jean.DUPONT@Example.COM  ")], ["customer_id", "email"])
    row = normaliser_email(df).collect()[0]
    assert row.email == "jean.dupont@example.com"
    assert row.email_valide is True


def test_normaliser_email_invalide(spark):
    df = spark.createDataFrame([("1", "pas-un-email")], ["customer_id", "email"])
    row = normaliser_email(df).collect()[0]
    assert row.email_valide is False


def test_normaliser_email_null_reste_null(spark):
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("email", StringType(), True),
    ])
    df = spark.createDataFrame([("1", None)], schema)
    row = normaliser_email(df).collect()[0]
    assert row.email is None
    assert row.email_valide is None


# ---------------------------------------------------------------------------
# normaliser_ville
# ---------------------------------------------------------------------------

def test_normaliser_ville_cle_sans_accent(spark):
    df = spark.createDataFrame(
        [("1", "Thiès"), ("2", "THIES"), ("3", "  thies  "), ("4", "Dakar")],
        ["customer_id", "ville"],
    )
    resultat = {row.customer_id: row.ville_norm for row in normaliser_ville(df).collect()}
    # Les 3 variantes de Thies doivent donner la meme cle normalisee
    assert resultat["1"] == resultat["2"] == resultat["3"] == "thies"
    assert resultat["4"] == "dakar"


# ---------------------------------------------------------------------------
# normaliser_telephone
# ---------------------------------------------------------------------------

def test_normaliser_telephone_valide(spark):
    df = spark.createDataFrame(
        [("1", "77 123 45 67"), ("2", "+221701234567"), ("3", "70-12-34-567")],
        ["customer_id", "telephone"],
    )
    resultat = {row.customer_id: row for row in normaliser_telephone(df).collect()}
    assert resultat["1"].telephone_valide is True
    assert resultat["1"].telephone_norm == "771234567"
    assert resultat["2"].telephone_valide is True
    assert resultat["2"].telephone_norm == "701234567"
    assert resultat["3"].telephone_valide is True


def test_normaliser_telephone_invalide(spark):
    df = spark.createDataFrame(
        [("1", "12345"), ("2", "99 123 45 67")],
        ["customer_id", "telephone"],
    )
    resultat = {row.customer_id: row.telephone_valide for row in normaliser_telephone(df).collect()}
    assert resultat["1"] is False
    assert resultat["2"] is False


def test_normaliser_telephone_null(spark):
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("telephone", StringType(), True),
    ])
    df = spark.createDataFrame([("1", None)], schema)
    row = normaliser_telephone(df).collect()[0]
    assert row.telephone_valide is None


# ---------------------------------------------------------------------------
# valider_naissance
# ---------------------------------------------------------------------------

def test_valider_naissance_date_plausible(spark):
    df = spark.createDataFrame([("1", "1990-05-12")], ["customer_id", "date_naissance"])
    row = valider_naissance(df).collect()[0]
    assert row.date_naissance == datetime.date(1990, 5, 12)


def test_valider_naissance_date_trop_ancienne(spark):
    df = spark.createDataFrame([("1", "1850-01-01")], ["customer_id", "date_naissance"])
    row = valider_naissance(df).collect()[0]
    assert row.date_naissance is None


def test_valider_naissance_date_future(spark):
    df = spark.createDataFrame([("1", "2999-01-01")], ["customer_id", "date_naissance"])
    row = valider_naissance(df).collect()[0]
    assert row.date_naissance is None


def test_valider_naissance_date_invalide(spark):
    df = spark.createDataFrame([("1", "pas-une-date")], ["customer_id", "date_naissance"])
    row = valider_naissance(df).collect()[0]
    assert row.date_naissance is None


# ---------------------------------------------------------------------------
# dedupliquer_clients
# ---------------------------------------------------------------------------

def test_dedupliquer_supprime_les_doublons_exacts(spark):
    df = spark.createDataFrame(
        [("1", "Jean"), ("1", "Jean")],
        ["customer_id", "prenom"],
    )
    resultat = dedupliquer_clients(df)
    assert resultat.count() == 1


def test_dedupliquer_garde_une_ligne_par_client(spark):
    # meme customer_id mais lignes differentes (ex: mise a jour de prenom)
    df = spark.createDataFrame(
        [("1", "Jean"), ("1", "Jean-Pierre")],
        ["customer_id", "prenom"],
    )
    resultat = dedupliquer_clients(df)
    assert resultat.count() == 1
    assert resultat.collect()[0].customer_id == "1"


# ---------------------------------------------------------------------------
# nettoyer_clients (pipeline complet)
# ---------------------------------------------------------------------------

def test_nettoyer_clients_pipeline_complet(spark):
    df = spark.createDataFrame(
        [
            ("1", "Jean", "jean@example.com", "77 123 45 67", "Thiès", "1990-01-01"),
            ("1", "Jean", "jean@example.com", "77 123 45 67", "Thiès", "1990-01-01"),
            ("2", "Awa", "N/A", "99999", "THIES", "1850-01-01"),
        ],
        ["customer_id", "prenom", "email", "telephone", "ville", "date_naissance"],
    )
    resultat = nettoyer_clients(df)

    # les 2 lignes du client "1" (doublon exact) doivent fusionner en 1 seule
    assert resultat.count() == 2

    par_id = {row.customer_id: row for row in resultat.collect()}
    assert par_id["1"].ville_norm == "thies"
    assert par_id["2"].ville_norm == "thies"
    assert par_id["2"].email is None
    assert par_id["2"].date_naissance is None
