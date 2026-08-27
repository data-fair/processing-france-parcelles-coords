# @data-fair/processing-france-parcelles-coords

Plugin data-fair-processings : construit la donnée de référence
`cadastre-parcelles-coords`, qui associe chaque code de parcelle du plan
cadastral français à une coordonnée géographique simple (un point situé sur la
parcelle).

Source : [plan cadastral informatisé d'Etalab](https://cadastre.data.gouv.fr/datasets/cadastre-etalab),
téléchargé depuis `files.data.gouv.fr` (Licence Ouverte).

## Fonctionnement

- Le jeu de données produit est un jeu de données REST à 2 colonnes, `code` et
  `coord`, exposé comme donnée de référence via la recherche en masse
  `parcelle-coords` (`masterData.bulkSearchs`). Il permet donc de géolocaliser
  n'importe quel jeu de données contenant un code parcelle.
- Toutes les publications successives d'Etalab sont traitées, de la plus
  ancienne à la plus récente, et les lignes sont insérées avec le code parcelle
  comme identifiant. **Les parcelles disparues des publications récentes restent
  donc présentes**, ce qui permet de géolocaliser des données qui référencent
  des parcelles expirées.
- Le traitement est **incrémental** : la dernière date de publication traitée
  pour chaque département est mémorisée dans `<dir>/last-processed-dates.json`,
  et les exécutions suivantes ne téléchargent que les nouvelles publications.

  ⚠️ `dir` n'est persistant que si le service processings est déployé avec un
  volume `dataDir`. Sans ce volume, cet état est perdu au redémarrage et
  l'exécution suivante retraite l'intégralité de l'historique.
- Le plugin gère l'interruption : une demande d'arrêt annule immédiatement le
  téléchargement et la lecture en cours, et le département interrompu n'est pas
  marqué comme traité.

## Configuration

| Onglet | Champ | Description |
| ------ | ----- | ----------- |
| Jeu de données | `datasetMode` | `create` pour créer le jeu de données, `update` pour mettre à jour un jeu existant |
| Jeu de données | `dataset` | Titre et identifiant du jeu de données à créer, ou jeu de données à mettre à jour |
| Paramètres | `deps` | Départements à traiter (tous par défaut, codes INSEE dont `2A`, `2B` et l'outre-mer) |

Après une exécution en mode `create`, la configuration est automatiquement
basculée en mode `update` sur le jeu de données créé.

## Développement

- Node 24 (`nvm use`), TypeScript natif exécuté par Node (aucune étape de build).
- `npm install`
- `npm run build-types` — génère les types depuis `processing-config-schema.json`
  et `config/type/schema.ts`. À relancer après toute modification du schéma.
- `npm test` — tests `node:test` (`test-it/`). Ils téléchargent réellement une
  publication du département 976.
- `npm run lint` / `npm run lint-fix`

## Publication

La publication au registre data-fair est faite par CI, jamais manuellement (il
n'y a pas de `npm publish`) : un push sur `main` publie au registre de staging,
un tag `v*` publie en production.

```bash
npm version minor       # bump de version + tag v*
git push --follow-tags  # la CI publie au registre de production
```
