# TP3 — Qualité des données clients

Tableau de relevés à compléter après exécution du notebook `notebooks/TP3_nettoyage.ipynb`
(section 4.2 imprime les valeurs à recopier ici).

| Indicateur                     | Valeur |
|---------------------------------|--------|
| Lignes brutes                   |        |
| Emails manquants (vides / N/A)  |        |
| Villes distinctes (avant)       |        |
| Villes distinctes (après)       |        |
| Doublons exacts                 |        |
| Lignes après nettoyage          |        |

## Questions de réflexion

1. Combien de villes distinctes avant/après normalisation ? Effet de la casse et des accents ?
2. Décision prise pour les emails manquants (drop ou fill) ? Pourquoi ?
3. Coût de l'UDF `sans_accent` ? Pourquoi justifiée malgré la règle « fonctions intégrées d'abord » ?
4. Différence entre déduplication après normalisation et déduplication naïve ?
