# Entités et couche brute

**Date :** 2026-09-27
**Statut :** validé en brainstorming, en attente de relecture
**Modifie :** `2026-09-27-explosion-v2-design.md` (extraction), `2026-09-27-qualification-v2-design.md` (liens, valeur, priorité)

## Pourquoi

1. **Une même personne répartie sur plusieurs wallets est mal jugée.** Aujourd'hui, tout envoi signé par un acheteur compte comme une vente, et une réception non signée est ignorée. Si A achète puis envoie ses tokens à son coffre B avant le creux, A tombe à 0 (exclu) et B est invisible : on perd l'entité.
2. **Le prix de revient d'un transfert interne est faux.** A achète à 0,02, envoie à B quand le prix est à 0,2 : B ne l'a pas payé 0,2. Côté Zerion, B a une réception « gratuite » et A un achat jamais revendu.
3. **La couche brute est incomplète pour un scoring sophistiqué** : pas de métadonnées de token (supply), portefeuille stocké en total seulement, transferts on-chain du token explosif non conservés, retraits/ajouts de liquidité non récupérés.

## Mesure sur AI (Robinhood, lancement → creux du 18/08, 825 608 transferts)

3 721 gros envois hors pool (≥ 20 % des achats de l'expéditeur, ≥ 500 $ au creux) :

| Destinataire | Envois | Adresses | Valeur au creux |
|---|---|---|---|
| Hub (≥ 10 acheteurs expéditeurs : router / exchange) | 3 482 | 32 | 30,5 M$ |
| Contrat | 101 | 80 | 1,2 M$ |
| Wallet qui achète lui-même | 79 | 60 | 1,1 M$ |
| Wallet qui n'achète jamais (coffre) | 59 | 53 | 677 k$ |

49 coffres dépasseraient le seuil du top 300.

**Bots.** `0x000461…c111` (un des acheteurs mesurés) signe 128 transactions en 24 h, 135/jour sur 7 jours, 182/jour sur 30 jours (seuil `max_txs_per_day` = 50) : c'est un bot. Aujourd'hui le filtre bot n'existe qu'à la qualification : un bot peut occuper une place du top et ses envois pourraient créer une entité.

## Décisions

| Sujet | Décision |
|---|---|
| Où détecter les liens | **Découverte** (HyperSync, token explosif) **et qualification** (Zerion, tous tokens), même table d'entités, mêmes règles de fusion |
| Unité classée | **L'entité** : sa position totale au creux entre ou non dans le top `max_buyers` ; une ligne `EarlyBuyer` par wallet reste stockée |
| Vente | Envoi vers un pool du token, un hub, une adresse de dépôt ou une adresse d'exchange connue |
| Transfert d'entité | Gros envoi vers toute autre adresse (wallet ordinaire ou smart wallet peu alimenté) : quantité, prix de revient (au prorata) et date du premier achat passent au destinataire |
| Coffre qui achète lui-même | Rattaché à l'entité ; garde ses achats, hérite du prix de revient seulement pour la part reçue |
| Pendant la montée | Les coffres découverts sont suivis (passe filtrée), jusqu'à `vault_follow_depth` niveaux |
| Qualification | Liens → entités, mouvements internes marqués, valeur d'entité ; filtres et tags restent par wallet (spec scoring suivante) |
| Bots | **Filtrés dès la découverte**, avant le classement, sur l'activité des 7 jours précédant le creux (`max_txs_per_day`) ; un bot ne forme pas d'entité |
| Couche brute | `TokenInfo` (Zerion `/fungibles`), `TokenTransfer` (on-chain), photo de portefeuille par token, `deposit`/`withdraw` récupérés ; filtre anti-spam Zerion conservé ; pas de bougies |

## Classement d'un destinataire (fonction pure)

Entrée : les transferts du token lus en passe 1 (et en passe entité), les pools du token, les adresses d'exchange connues, les réglages.

1. Pool du token → **vente**.
2. A reçu ce token d'au moins `hub_min_senders` expéditeurs distincts (tous transferts du token confondus) → **hub** (router, exchange, staking, bridge) → sortie.
3. A fait suivre au moins `deposit_forward_pct` % de ce qu'il a reçu vers un hub en moins de `deposit_forward_hours` → **adresse de dépôt** → sortie.
4. Présent dans `KnownAddress` (types bloquants) → **sortie**.
5. Sinon → **coffre** : transfert d'entité.

Un envoi est « gros » s'il représente au moins `transfer_after_buy_pct` % de la position de l'expéditeur au moment de l'envoi. Les petits envois vers un coffre restent des sorties (ils réduisent la position sans créer de lien).

## Filtre bot (découverte)

Après la passe 1, les candidats sont vérifiés **du plus gros au plus petit** (position de l'entité au creux) jusqu'à remplir `max_buyers` entités :

- pour chaque wallet d'une entité candidate : nombre de transactions signées (HyperSync, `wallet_tx_count`) entre `creux − bot_window_days` et le creux ;
- au-delà de `max_txs_per_day` × `bot_window_days` → **bot** : le wallet est écarté (raison `bot` conservée) ;
- un bot est simplement écarté : **aucun lien, aucune entité** ; ses envois sont des sorties et leur destinataire n'hérite de rien ;
- une entité dont tous les wallets sont des bots est écartée ; sinon on retire les bots et on recalcule sa position.

Le seuil réutilise `max_txs_per_day` (réglage de qualification, valeur globale) ; `bot_window_days` (7) est un réglage de détection. Les résultats sont mis en cache Redis par (chaîne, wallet, jour du creux) pour ne pas recompter.

## Position au creux et héritage (fonction pure)

Pour chaque adresse, dans l'ordre des blocs :

- achat : + quantité, + coût (quantité × prix au moment de l'achat) ;
- vente ou sortie : − quantité, − coût au prorata ;
- gros envoi vers un coffre : − quantité et − coût au prorata chez l'expéditeur, + les mêmes chez le coffre ; le coffre hérite de la date du premier achat de l'expéditeur si elle est antérieure à la sienne.

Position au creux = quantité restante × prix du creux. Les chaînes A → B → C se résolvent naturellement dans l'ordre des blocs.

## Entités

- Un lien fort (transfert d'entité côté HyperSync, `TRANSFER_AFTER_BUY` / `BIG_RECEIVE` côté Zerion) rattache les deux wallets à la même entité : création si aucun n'en a, rattachement si un seul en a, **fusion** si les deux en ont une différente (l'absorbée garde `merged_into`).
- Un service unique `entities.link(from_wallet, to_wallet, kind, source, evidence)` est utilisé par la découverte et la qualification.
- « Détacher un wallet » (admin) retire le wallet de l'entité et marque les liens concernés `rejected` : ils ne sont plus recréés.
- Les liens faibles (`FUNDING`) restent informatifs et ne forment pas d'entité.

## Extraction (découverte)

**Passe 1 — scan complet** (fenêtre d'achat → creux, page par page) : achats par signataire ; envois vers les pools ; totaux par couple (expéditeur, destinataire) pour les autres envois ; nombre d'expéditeurs distincts par destinataire (plafonné à `hub_min_senders`) ; envois des adresses ayant reçu un envoi (pour détecter les dépôts). En fin de passe : classement des destinataires, positions au creux avec héritage, formation des entités, classement des **entités** par position totale au creux, **filtre bot** (section dédiée) en remplissant le top `max_buyers` (entités) au-dessus de `min_buy_usd`.

**Passe entité — filtrée** (fenêtre d'achat → pic) : transferts dont l'expéditeur **ou** le destinataire est un wallet d'une entité retenue (par lots de `sell_pass_batch_size` adresses). Enregistre les `TokenTransfer`, calcule le % revendu pendant la montée par entité, ajoute les nouveaux coffres découverts pendant la montée et relance la passe sur eux, jusqu'à `vault_follow_depth`.

**Écriture** : `Wallet`, `Entity`, `EarlyBuyer` (par wallet), `EntityEarlyBuy` (par entité), `WalletLink` (source `hypersync`), `TokenTransfer`.

`max_transfers_per_token` limite toujours la passe 1 (explosion `partial`).

## Qualification

- Les liens forts Zerion passent par `entities.link` (source `zerion`).
- Après l'enregistrement de l'historique, un `TokenTrade` d'envoi ou de réception dont la contrepartie est dans la même entité est marqué `is_internal` ; le calcul des positions l'ignore.
- Valeur de l'entité = somme des portefeuilles de ses wallets ; remplace `linked_value_usd` dans la décision.
- Priorité calculée par entité (explosions captées, position totale au creux, pondération rug inchangée).

## Couche brute

- **`TokenInfo`** : un token rencontré dans l'historique et absent (ou plus vieux que `token_info_refresh_days`) est récupéré via `/fungibles` par lots de 25 implémentations. Champs : chaîne, adresse, `fungible_id`, symbole, nom, décimales, supply totale et en circulation, vérifié, JSON brut, date de récupération.
- **Portefeuille** : la réponse Zerion déjà appelée est conservée en `PortfolioSnapshot` (wallet, date, total, JSON brut) + `PortfolioPosition` (chaîne, token, quantité, prix, valeur). Aucun appel supplémentaire.
- **Types d'opérations Zerion** : deviennent un réglage (`zerion_operation_types`), avec `deposit` et `withdraw` ajoutés. Avant activation, mesure sur 20 wallets du nombre de pages supplémentaires ; le budget quotidien reste plafonné par `zerion_daily_budget`.
- **`TokenTransfer`** (voir extraction) : chaîne, token, hash, index du log, bloc, date, signataire, expéditeur, destinataire, quantité, type (`buy` / `sell` / `exit` / `internal` / `receive`), explosion.

## Modèles

- `Entity` : `created_at`, `updated_at`, `merged_into` (nullable).
- `Wallet.entity` (FK nullable).
- `WalletLink` : + `source` (`hypersync` / `zerion`), + `rejected` (bool), + type `TRANSFER_TO_VAULT`.
- `EarlyBuyer` : + `entity`, + `inherited_amount`, + `inherited_usd`, + `inherited_from` (FK Wallet nullable).
- `ExcludedBuyer` : `explosion`, `wallet`, `reason` (`bot`), `txs_per_day`, `held_usd` — trace des candidats écartés, pour contrôle.
- `EntityEarlyBuy` : `entity`, `explosion`, `held_usd`, `held_amount`, `first_buy_at`, `sold_during_rise_pct`, `rank` ; unique (entité, explosion).
- `TokenTransfer`, `TokenInfo`, `PortfolioSnapshot`, `PortfolioPosition` (ci-dessus).
- `TokenTrade` : + `is_internal`.
- Réglages : `DetectionSettings` + `hub_min_senders` (10), `vault_follow_depth` (2), `bot_window_days` (7) ; `PipelineSettings` + `zerion_operation_types`, `token_info_refresh_days` (30) ; réutilisés : `transfer_after_buy_pct`, `deposit_forward_pct`, `deposit_forward_hours` (`QualificationSettings`, valeurs globales).

## Admin

Page **Entités** : wallets, liens avec preuves (source, hash, %), early buys par explosion, valeur ; action « détacher ce wallet ». `EntityEarlyBuy` en lecture seule, trié par rang. `TokenInfo`, `TokenTransfer`, portefeuilles en lecture seule.

## Tests

- **Purs** : classement des destinataires (pool, hub, dépôt, exchange connu, coffre dont smart wallet peu alimenté) ; positions avec héritage A → B → C et prix de revient au prorata ; petits envois = sorties ; union et fusion d'entités ; classement par entité (4 wallets = 1 place) ; un bot est écarté et remplacé par le suivant ; un bot ne crée ni lien ni entité.
- **Extraction** (faux HyperSync) : un coffre entre dans le top par héritage ; suivi pendant la montée jusqu'à la profondeur ; dépôt reclassé en sortie ; `TokenTransfer` enregistrés.
- **Qualification** : liens Zerion → entités ; `is_internal` ; positions sans mouvements internes ; valeur d'entité ; priorité par entité ; détacher un wallet.
- **Couche brute** : `TokenInfo` par lots et rafraîchissement ; photo de portefeuille par token.
- **Live (AI)** : `0x000461…c111` est écarté comme bot, sans lien ni entité ; `0x44df…` reste dans le top ; nombre de coffres entrés proche de 49.

## Critères de réussite

1. Sur AI, les coffres mesurés sont rattachés à leur entité et l'entité est classée sur sa position totale.
2. Un transfert interne ne crée ni achat ni vente, et le prix de revient suit la quantité.
3. Aucun bot (au-delà de `max_txs_per_day` sur les 7 jours précédant le creux) n'occupe une place du top, et aucun bot ne forme d'entité.
4. Les données brutes (transferts on-chain retenus, métadonnées de tokens, portefeuille par token) sont stockées et rejouables.
5. `make test` et `make lint` passent.

## Hors périmètre

- Formule de scoring, lots FIFO, gains réalisés / latents (spec scoring)
- Filtres et tags au niveau de l'entité (spec scoring)
- Snipers en secondes, cas « pool mort » (astro)
- Logs du worker (évolution déjà convenue)
