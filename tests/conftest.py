import os
import sys

import pytest
from pyspark.sql import SparkSession

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")

# CRITIQUE sous Windows : sans ceci, Spark lance les workers Python avec la
# commande generique "python", qui sur Windows tombe souvent sur l'alias
# Microsoft Store ("Python est introuvable ; executez sans arguments a
# installer...") au lieu de l'interpreteur du venv. On force explicitement
# le meme interpreteur que celui qui execute pytest (sys.executable).
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def spark():
    """Une seule SparkSession locale, partagee par tous les tests."""
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("TP3-tests")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        # to_date() etc. renvoient null sur une entree invalide au lieu de
        # lever une exception (comportement uniforme quelle que soit la
        # version de Spark).
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()
