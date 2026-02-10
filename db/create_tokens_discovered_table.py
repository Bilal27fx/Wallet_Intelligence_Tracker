#!/usr/bin/env python3
"""
Création de la table tokens_discovered
Stocke les résultats du module token_discovery

Cette table centralise tous les tokens découverts via le pipeline:
- Top tokens performants (CoinGecko)
- Métadonnées enrichies (CMC, CoinGecko)
- Contrats EVM associés
- Métriques de performance
"""

import sqlite3
from pathlib import Path
from datetime import datetime


DB_PATH = Path(__file__).parent.parent / "data" / "db" / "wit_database.db"


def create_tokens_discovered_table():
    """Crée la table tokens_discovered si elle n'existe pas"""

    print("=" * 80)
    print("📊 CRÉATION TABLE: tokens_discovered")
    print("=" * 80)
    print(f"📁 Base: {DB_PATH}")
    print(f"⏰ Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    if not DB_PATH.exists():
        print(f"❌ Base de données introuvable: {DB_PATH}")
        return False

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Vérifier si la table existe déjà
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='tokens_discovered'
    """)

    if cursor.fetchone():
        print("⚠️  La table 'tokens_discovered' existe déjà")
        print()

        # Afficher le schéma existant
        cursor.execute("PRAGMA table_info(tokens_discovered)")
        columns = cursor.fetchall()

        print("📋 Schéma actuel:")
        for col in columns:
            print(f"   • {col[1]:<25} {col[2]:<15} {'NOT NULL' if col[3] else ''}")

        conn.close()

        response = input("\nVoulez-vous recréer la table ? (oui/non) : ").strip().lower()
        if response not in ['oui', 'o', 'yes', 'y']:
            print("❌ Opération annulée")
            return False

        # Supprimer l'ancienne table
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("DROP TABLE tokens_discovered")
        print("🗑️  Ancienne table supprimée")
        print()

    print("🔧 Création de la table tokens_discovered...")
    print()

    # Créer la table
    cursor.execute("""
        CREATE TABLE tokens_discovered (
            -- Identifiants
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_id TEXT NOT NULL,              -- ID CoinGecko (ex: "bitcoin")
            symbol TEXT NOT NULL,                 -- Symbole (ex: "BTC")
            name TEXT NOT NULL,                   -- Nom complet (ex: "Bitcoin")

            -- Contrat blockchain
            contract_address TEXT,                -- Adresse du contrat (0x...)
            platform TEXT,                        -- Blockchain (ethereum, bsc, etc.)
            cmc_id INTEGER,                       -- ID CoinMarketCap

            -- Métriques de performance (au moment de la découverte)
            current_price_usd REAL,               -- Prix actuel en USD
            market_cap_usd REAL,                  -- Capitalisation en USD
            total_volume_usd REAL,                -- Volume 24h en USD

            -- Performance par période
            price_change_1h REAL,                 -- % changement 1h
            price_change_24h REAL,                -- % changement 24h
            price_change_7d REAL,                 -- % changement 7j
            price_change_14d REAL,                -- % changement 14j
            price_change_30d REAL,                -- % changement 30j
            price_change_200d REAL,               -- % changement 200j
            price_change_1y REAL,                 -- % changement 1an

            -- Métadonnées de découverte
            discovery_period TEXT NOT NULL,       -- Période de découverte (14d, 30d, 200d, 1y)
            discovery_rank INTEGER,               -- Rang lors de la découverte (1-8)
            discovered_at TIMESTAMP NOT NULL,     -- Date/heure de découverte
            source TEXT DEFAULT 'coingecko',      -- Source (coingecko, manual, etc.)

            -- Statut
            is_active BOOLEAN DEFAULT 1,          -- Token actif (1) ou archivé (0)
            has_contract BOOLEAN DEFAULT 0,       -- A un contrat EVM (1/0)
            is_evm_compatible BOOLEAN DEFAULT 0,  -- Compatible EVM (1/0)

            -- Tracking
            wallets_extracted BOOLEAN DEFAULT 0,  -- Wallets extraits via Dune (1/0)
            wallets_count INTEGER DEFAULT 0,      -- Nombre de wallets découverts

            -- Timestamps
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            -- Contraintes
            UNIQUE(token_id, discovery_period, discovered_at)
        )
    """)

    # Index pour performances
    print("🔧 Création des index...")
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_tokens_discovered_symbol
        ON tokens_discovered(symbol)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_tokens_discovered_contract
        ON tokens_discovered(contract_address)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_tokens_discovered_period
        ON tokens_discovered(discovery_period)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_tokens_discovered_active
        ON tokens_discovered(is_active)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_tokens_discovered_date
        ON tokens_discovered(discovered_at DESC)
    """)

    # Commit et fermeture
    conn.commit()

    print("✅ Table créée avec succès!")
    print()

    # Afficher le schéma
    cursor.execute("PRAGMA table_info(tokens_discovered)")
    columns = cursor.fetchall()

    print("📋 Schéma de la table tokens_discovered:")
    print()
    for col in columns:
        cid, name, type_, notnull, default, pk = col
        constraints = []
        if pk:
            constraints.append("PRIMARY KEY")
        if notnull:
            constraints.append("NOT NULL")
        if default:
            constraints.append(f"DEFAULT {default}")

        constraint_str = f" ({', '.join(constraints)})" if constraints else ""
        print(f"   {cid+1:2d}. {name:<25} {type_:<15} {constraint_str}")

    print()
    print(f"📊 Index créés: 5")
    print()

    conn.close()

    print("=" * 80)
    print("✅ MIGRATION TERMINÉE")
    print("=" * 80)
    print()
    print("📝 Prochaines étapes:")
    print("   1. Modifier le module token_discovery pour insérer dans cette table")
    print("   2. Lancer le Discovery Pipeline:")
    print("      python smart_wallet_analysis/discovery_pipeline_runner.py")
    print()

    return True


def show_table_info():
    """Affiche les informations sur la table tokens_discovered"""

    if not DB_PATH.exists():
        print(f"❌ Base de données introuvable: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Vérifier si la table existe
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='tokens_discovered'
    """)

    if not cursor.fetchone():
        print("⚠️  La table 'tokens_discovered' n'existe pas")
        conn.close()
        return

    # Compter les enregistrements
    cursor.execute("SELECT COUNT(*) FROM tokens_discovered")
    count = cursor.fetchone()[0]

    print("=" * 80)
    print("📊 INFORMATIONS TABLE: tokens_discovered")
    print("=" * 80)
    print(f"📁 Base: {DB_PATH}")
    print(f"📊 Nombre d'enregistrements: {count:,}")
    print()

    if count > 0:
        # Statistiques
        cursor.execute("""
            SELECT
                COUNT(DISTINCT symbol) as unique_tokens,
                COUNT(DISTINCT discovery_period) as periods,
                COUNT(CASE WHEN has_contract = 1 THEN 1 END) as with_contracts,
                COUNT(CASE WHEN wallets_extracted = 1 THEN 1 END) as wallets_extracted,
                MIN(discovered_at) as first_discovery,
                MAX(discovered_at) as last_discovery
            FROM tokens_discovered
        """)

        stats = cursor.fetchone()

        print("📈 Statistiques:")
        print(f"   • Tokens uniques: {stats[0]}")
        print(f"   • Périodes: {stats[1]}")
        print(f"   • Avec contrats: {stats[2]}")
        print(f"   • Wallets extraits: {stats[3]}")
        print(f"   • Première découverte: {stats[4]}")
        print(f"   • Dernière découverte: {stats[5]}")
        print()

        # Top 10
        print("🏆 Top 10 tokens récents:")
        cursor.execute("""
            SELECT symbol, name, discovery_period,
                   ROUND(price_change_30d, 2) as perf_30d,
                   discovered_at
            FROM tokens_discovered
            ORDER BY discovered_at DESC
            LIMIT 10
        """)

        for row in cursor.fetchall():
            print(f"   • {row[0]:8} {row[1]:20} ({row[2]:4}) +{row[3]:6}% - {row[4]}")

    conn.close()
    print("=" * 80)


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--info":
        show_table_info()
    else:
        try:
            success = create_tokens_discovered_table()
            sys.exit(0 if success else 1)
        except KeyboardInterrupt:
            print("\n⚠️  Opération annulée")
            sys.exit(1)
        except Exception as e:
            print(f"\n❌ Erreur: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
