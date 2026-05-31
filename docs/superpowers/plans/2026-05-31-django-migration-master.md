# Django Migration Master Plan

> **For agentic workers:** Execute phases sequentially. Each phase has its own detailed sub-plan. Complete all tasks in a phase before moving to the next.

**Goal:** Migrer WIT V1 vers architecture Django + PostgreSQL + Celery + Redis + Docker

**Architecture:** Multi-conteneurs Docker (backend Django API, worker Celery, beat scheduler, PostgreSQL, Redis). 2 apps Django (wallets pour analyses, notifications pour outputs). Migration progressive sur branche séparée.

**Tech Stack:** Django 5.0, Django REST Framework, PostgreSQL 16, Celery 5.x, Redis 7, Docker, Gunicorn

---

## Overview

Cette migration se déroule en **5 phases indépendantes et testables** :

1. **Infrastructure** - Setup Django + Docker + PostgreSQL + migration données
2. **API REST** - Endpoints DRF pour le frontend Next.js
3. **Business Logic** - Migration services (discovery, tracking, FIFO, scoring, consensus)
4. **Celery** - Tasks asynchrones + scheduler
5. **Production** - docker-compose.prod + déploiement final

**Branche Git :** `feature/django-migration`

**Durée estimée :** 5-8 jours (avec Claude)

---

## Phase 1: Infrastructure Setup ✓

**Sub-plan:** `docs/superpowers/plans/2026-05-31-phase-1-infrastructure.md`

**Objectif :** Projet Django fonctionnel avec Docker, PostgreSQL, et données migrées depuis SQLite.

**Livrables :**
- [ ] Projet Django créé avec structure complète (config, apps, common)
- [ ] Docker setup (5 conteneurs : postgres, redis, backend, worker, beat)
- [ ] Modèles Django créés (inspectdb + refactoring)
- [ ] Migrations Django appliquées
- [ ] Données SQLite migrées vers PostgreSQL
- [ ] Django Admin fonctionnel (visualiser wallets, tokens, transactions)

**Validation :**
```bash
docker-compose up -d
docker-compose exec backend python manage.py test
# Expected: Tests passent, admin accessible à http://localhost:8000/admin
```

**Checkpoint :** PostgreSQL contient toutes les données de l'ancien SQLite, accessibles via Django Admin.

---

## Phase 2: API REST ✓

**Sub-plan:** `docs/superpowers/plans/2026-05-31-phase-2-api-rest.md`

**Objectif :** API REST complète pour le frontend Next.js.

**Livrables :**
- [ ] Serializers DRF (wallets, analytics, consensus)
- [ ] ViewSets et routes API
- [ ] Endpoints wallets (list, detail, positions, transactions, analytics)
- [ ] Endpoints analytics (performance, tiers)
- [ ] Endpoints consensus (signals, history)
- [ ] Tests API (200 OK, pagination, filtres)
- [ ] Frontend Next.js connecté à l'API (remplace mock data)

**Validation :**
```bash
# API accessible
curl http://localhost:8000/api/v1/wallets/ | jq

# Frontend affiche vraies données
cd frontend && npm run dev
# Expected: Dashboard affiche wallets depuis API Django
```

**Checkpoint :** Frontend Next.js affiche les vraies données depuis l'API Django, pas de mock data.

---

## Phase 3: Business Logic Migration ✓

**Sub-plan:** `docs/superpowers/plans/2026-05-31-phase-3-business-logic.md`

**Objectif :** Migrer toute la logique métier (discovery, tracking, FIFO, scoring, consensus) vers Django services.

**Livrables :**
- [ ] Service Discovery (Dune client + explosion detector)
- [ ] Service Tracking (Zerion client + balance tracker)
- [ ] Service FIFO Calculator (Django ORM)
- [ ] Service Wallet Scorer
- [ ] Service Tier Analyzer
- [ ] Service Consensus Detector
- [ ] Django Management Commands (run_discovery, run_tracking, run_scoring, run_consensus)
- [ ] Tests de validation (ancien vs nouveau : FIFO, scoring identiques)

**Validation :**
```bash
# Lancer discovery pipeline
docker-compose exec backend python manage.py run_discovery

# Lancer scoring pipeline
docker-compose exec backend python manage.py run_scoring

# Comparer résultats PostgreSQL vs SQLite
docker-compose exec backend python manage.py validate_migration
# Expected: FIFO, scoring, consensus identiques à l'ancien système
```

**Checkpoint :** Pipelines Django produisent les mêmes résultats que l'ancien système SQLite.

---

## Phase 4: Celery + Scheduler ✓

**Sub-plan:** `docs/superpowers/plans/2026-05-31-phase-4-celery.md`

**Objectif :** Convertir pipelines en tâches Celery asynchrones avec scheduler automatique.

**Livrables :**
- [ ] Config Celery (`config/celery.py`)
- [ ] Celery tasks (run_discovery_pipeline, run_tracking_live, run_scoring_pipeline, run_consensus_detection)
- [ ] Celery Beat schedule (discovery daily, scoring every 2 days, tracking every 2h)
- [ ] Tests Celery (tasks s'exécutent sans erreur)
- [ ] Monitoring Flower (optionnel)

**Validation :**
```bash
# Worker et beat démarrés
docker-compose logs -f worker
docker-compose logs -f beat

# Déclencher une tâche manuellement
docker-compose exec backend python manage.py shell
>>> from wallets.tasks import run_discovery_pipeline
>>> result = run_discovery_pipeline.delay()
>>> result.status
'SUCCESS'

# Vérifier scheduler automatique
docker-compose exec beat celery -A config inspect scheduled
# Expected: Tâches planifiées visibles
```

**Checkpoint :** Celery Beat exécute automatiquement les pipelines selon le schedule configuré.

---

## Phase 5: Production Ready ✓

**Sub-plan:** `docs/superpowers/plans/2026-05-31-phase-5-production.md`

**Objectif :** Préparer et déployer en production sur VPS.

**Livrables :**
- [ ] `docker-compose.prod.yml` (optimisé pour production)
- [ ] Variables d'environnement production (`.env.example` documenté)
- [ ] Nginx reverse proxy (optionnel)
- [ ] Health checks et monitoring
- [ ] Documentation déploiement
- [ ] Tests end-to-end complets
- [ ] Migration finale sur VPS
- [ ] Rollback plan documenté

**Validation :**
```bash
# Build production
docker-compose -f docker-compose.prod.yml up -d --build

# Health checks
curl http://localhost:8000/health/
# Expected: {"status": "ok"}

# Vérifier tous les services
docker-compose ps
# Expected: Tous UP
```

**Checkpoint :** Stack complète tourne en production, ancien système peut être éteint.

---

## Progression Tracking

Utilise cette checklist pour suivre l'avancement global :

### Phase 1: Infrastructure
- [ ] Django project créé
- [ ] Docker running (5 conteneurs)
- [ ] Modèles Django + migrations
- [ ] Données migrées SQLite → PostgreSQL
- [ ] Admin fonctionnel
- [ ] ✅ Checkpoint validé

### Phase 2: API REST
- [ ] Serializers + ViewSets
- [ ] Tous les endpoints implémentés
- [ ] Tests API passent
- [ ] Frontend connecté
- [ ] ✅ Checkpoint validé

### Phase 3: Business Logic
- [ ] Services discovery + tracking
- [ ] Services analytics (FIFO, scorer, tier)
- [ ] Service consensus
- [ ] Management commands
- [ ] Tests validation (ancien vs nouveau)
- [ ] ✅ Checkpoint validé

### Phase 4: Celery
- [ ] Config Celery
- [ ] Tasks implémentées
- [ ] Beat schedule configuré
- [ ] Tests Celery
- [ ] ✅ Checkpoint validé

### Phase 5: Production
- [ ] docker-compose.prod.yml
- [ ] Documentation déploiement
- [ ] Tests end-to-end
- [ ] Déployé sur VPS
- [ ] ✅ Migration complète

---

## Rollback Strategy

À chaque phase, si un problème bloquant apparaît :

```bash
# Retour à l'ancien système
git checkout main
python -m smart_wallet_analysis.discovery_pipeline_runner

# OU sur VPS
ssh root@46.224.0.146
systemctl start wit_scheduler.service
```

Les données SQLite originales sont conservées en backup pendant toute la migration.

---

## Next Steps

1. **Créer la branche :** `git checkout -b feature/django-migration`
2. **Commencer Phase 1 :** Ouvrir `docs/superpowers/plans/2026-05-31-phase-1-infrastructure.md`
3. **Exécuter task par task** avec subagent-driven-development ou executing-plans
4. **Valider checkpoint** avant de passer à la phase suivante

Bon courage ! 🚀
