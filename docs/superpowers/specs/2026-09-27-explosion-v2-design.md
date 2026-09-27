# Explosion v2 — vagues, dernier creux, extraction au fil de l'eau

**Date :** 2026-09-27
**Statut :** validé en brainstorming, en attente de relecture
**Modifie :** `2026-09-26-discovery-multichaine-design.md` (détection d'explosion et extraction des early buyers)

## Pourquoi

Sur un vrai token (AI, Robinhood, `0x2e8c…1e18`, lancé le 14 juillet, explosion à partir du 20 août, pic le 18 septembre) :

1. **Mauvais point de coupure.** L'algorithme prend le point le plus bas avant le pic (8 août), avant une première petite vague retombée. Les acheteurs du creux du 9 au 18 août — juste avant la vraie explosion — sont exclus.
2. **Explosion invisible.** La fenêtre de 72 h exige que le creux *et* le pic soient récents : une explosion qui monte pendant un mois n'est jamais détectée.
3. **Extraction impossible sur les gros tokens.** Les transferts sont tous chargés en mémoire, du lancement au pic ; au-delà de 500 000, le token est rejeté (`too_many_transfers`). Mesure : 829 358 transferts du lancement au creux pour AI (5 min 40 de téléchargement).
4. **Un jour de retard.** L'attente de 24 h après le pic (contrôle anti-rug) retarde l'extraction d'acheteurs déjà connus.

## Décisions

| Sujet | Avant | Après |
|---|---|---|
| Point de coupure (« creux ») | Point le plus bas avant le pic | **Dernier creux avant la montée finale** : dernier point bas après lequel le prix a dépassé `breakout_multiplier` × ce creux sans jamais repasser dessous jusqu'au pic |
| Choix entre plusieurs vagues | Meilleur multiplicateur | **Score = multiplicateur × min(1, âge du token au creux ÷ `maturity_hours`)** ; âge compté depuis la création du pool |
| Fenêtre de temps | Creux et pic dans les 72 h | **Seul le pic** dans les `explosion_window_hours` (7 jours) ; aucune fenêtre pour un ajout manuel |
| Attente de 24 h | Bloquante (`WAITING_CONFIRMATION`) | **Supprimée** : extraction immédiate ; la rétention devient une note (`held` / `rug`) mesurée plus tard |
| Fenêtre d'achat | Création du pool → creux | **`max(création du pool, creux − buyer_window_hours)` → creux** ; `buyer_window_hours` = 0 = depuis le lancement |
| Extraction | Tout en mémoire, plafond bloquant | **Agrégation page par page** (passe 1 : achats jusqu'au creux) + **passe 2 filtrée sur les acheteurs retenus** (ventes jusqu'au pic) |
| Plafond de transferts | Rejet `too_many_transfers` | **Garde-fou** : arrêt propre, explosion marquée `partial` |
| Nouvelle vague plus tard | — | Nouvelle explosion avec ses propres acheteurs (règle `already_extracted` inchangée : le pic doit être plus récent) |

## Détection (fonction pure)

Entrée : bougies OHLCV (clôtures), date de création du pool, maintenant, seuils.

1. **Pics candidats** : les bougies dont la clôture est un maximum local, situées dans les `explosion_window_hours` dernières heures (toutes les bougies si ajout manuel).
2. **Creux de chaque pic** : en remontant depuis le pic, le **dernier** point `t` tel que :
   - la clôture ne repasse jamais sous `close(t)` entre `t` et le pic ;
   - le prix dépasse `breakout_multiplier × close(t)` quelque part entre `t` et le pic.
3. **Vague valable** : `pic / creux ≥ min_multiplier`, volume autour du pic ≥ `min_volume_usd`, liquidité actuelle ≥ `min_liquidity_usd`.
4. **Score** : `multiplicateur × min(1, (ts_creux − création du pool) ÷ maturity_hours)`. La vague au meilleur score est retenue.

Exemple (création le 18/07) : vague ×50 au 20/07 (2 j) → 50 × 2/14 ≈ 7,1 ; vague ×10 au 20/08 (33 j) → 10 × 1 = 10 → la vague du 20/08 est retenue.

`find_best_run` est remplacé par cette détection ; `choose_resolution` reste inchangé.

## Rétention (note, non bloquante)

Mesurée au premier passage où `now ≥ pic + confirmation_hours` : `retention_pct` = clôture à `pic + confirmation_hours` ÷ pic. `retention_status` = `held` si ≥ `min_retention_pct`, sinon `rug` ; `pending` tant que non mesurable. Les explosions `rug` restent en base ; leurs acheteurs pèsent moins dans la priorité de qualification (poids réglable `rug_priority_weight`, défaut 0,2).

## Extraction au fil de l'eau (HyperSync)

**Passe 1 — achats** : logs `Transfer` du token de `max(bloc de création du pool, bloc(creux − buyer_window_hours))` au bloc du creux, joints à la transaction (`tx.from`). Pour chaque page : achat si `recipient == tx.from` (règle EOA inchangée) ; mise à jour des totaux par acheteur (quantité, montant $ via les bougies, premier bloc d'achat). La page est ensuite libérée : mémoire proportionnelle au nombre d'acheteurs.

Puis : acheteurs `≥ min_buy_usd`, triés par montant, coupés à `max_buyers` (0 = sans plafond) ; `is_sniper` inchangé.

**Passe 2 — ventes** : logs `Transfer` du token **dont l'expéditeur (topic1) est un acheteur retenu** (adresses par lots de `sell_pass_batch_size`, défaut 500), du creux au pic ; `sold_amount` = envois signés par l'acheteur (règle inchangée).

**Garde-fou** : `max_transfers_per_token` (réglable) limite la passe 1. Atteint → on garde les acheteurs agrégés jusque-là, l'explosion est marquée `extraction_status = partial` (sinon `complete`).

## Modèles

- `Explosion` : + `score`, `trough_at` / `trough_block` (renommage de `low_*`), `retention_status` (`pending` / `held` / `rug`), `extraction_status` (`complete` / `partial`) ; `retention_pct` devient nullable.
- `Candidate.Status` : `WAITING_CONFIRMATION` n'est plus utilisé par la détection (conservé pour l'historique) ; `rug` n'est plus une raison de rejet.
- `DetectionSettings` : + `maturity_hours` (336), `breakout_multiplier` (2), `buyer_window_hours` (0), `rug_priority_weight` (0,2) ; `explosion_window_hours` → 168 et porte sur le pic seulement.
- `PipelineSettings` : + `sell_pass_batch_size` (500).

## Pipeline

`analyze_candidates` : détection → `CONFIRMED` (ou `REJECTED`) en un passage. Nouveau pas `measure_retention` (dans la même tâche) pour les explosions `pending` dont le délai est écoulé. `extract_early_buyers` inchangé côté orchestration.

`compute_priority` (qualification) : explosions `rug` pondérées par `rug_priority_weight`.

## Tests

- **Purs (détection)** : cas AI (creux du 18/08 retenu, pas le 08/08) ; « ×50 au jour 2 contre ×10 au jour 33 » ; rebond sous `breakout_multiplier` ignoré ; pic hors fenêtre rejeté ; ajout manuel sans fenêtre ; score avec âge ≥ maturité.
- **Rétention** : `pending` → `held` / `rug`.
- **Extraction** : agrégation page par page (faux client HyperSync paginé), passe 2 filtrée sur les acheteurs, garde-fou → `partial`.
- **Live** : détection sur le token AI (creux ≈ 17-18/08).

## Critères de réussite

1. Sur AI, le creux retenu est le 17-18 août et l'explosion est extraite (plus de `too_many_transfers`).
2. Une explosion est extraite au passage où elle est détectée, sans attente.
3. La mémoire de l'extraction ne dépend plus du nombre de transferts.
4. `make test` et `make lint` passent.

## Hors périmètre

- Scoring des wallets (rendement)
- Logs du worker (évolution séparée, déjà convenue)
