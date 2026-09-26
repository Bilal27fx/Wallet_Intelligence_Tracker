from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = ROOT_DIR / "data" / "db" / "wit_database.db"
ENV_PATH = ROOT_DIR / ".env"
DUNE_YML_PATH = ROOT_DIR / "config" / "dune.yml"

PIPELINES = {
    "TRACKING_MIN_USD": 500,
    "TRACKING_HOURS_LOOKBACK": 24,
    "RESCORING_MIN_USD": 500,
    "RESCORING_HOURS_LOOKBACK": 24,
    "SCORING_MIN_SCORE_DEFAULT": 20,
    "SCORING_MIN_SCORE_FULL": 0
}

TOKEN_DISCOVERY_MANUAL = {
    "DUNE_BASE_URL": "https://api.dune.com/api/v1",
    "MAX_WAIT_TIME_SECONDS": 700,
    "SLEEP_INTERVAL_SECONDS": 5,
    "ETHERSCAN_BASE_URL": "https://api.etherscan.io/v2/api",
    "ETHERSCAN_CHAIN_ID": "1",
    "ETHERSCAN_TIMEOUT_SECONDS": 10,
    "ETHERSCAN_MAX_RETRIES": 3,
    "ETHERSCAN_RETRY_BACKOFF_SECONDS": 0.5,
    "ETHERSCAN_RATE_LIMIT_SLEEP_SECONDS": 10,
    "CHAIN_MAPPING": {
        "base": "base",
        "ethereum": "ethereum",
        "bnbchain": "bnb",
        "bnb": "bnb",
        "bsc": "bnb"
    },
    "INPUT_JSON_PATH": ROOT_DIR / "smart_wallet_analysis" / "explosive_tokens_manual.json",
    "EXPORT_DIR": ROOT_DIR / "data" / "raw" / "csv" / "top_wallets",
    "CACHE_PATH": ROOT_DIR / "data" / "cache" / "early_wallets_extracted_manual.csv",
    "EOA_CHECK_DELAY_SECONDS": 0.2,
    "EARLY_WINDOW_HOURS_BY_TYPE": {1: 24, 2: 168, 3: 720}
}

SMART_WALLETS_PIPELINE = {
    "QUALITY_FILTER": 0.0,
    "PAUSE_AFTER_TRACKING_SECONDS": 5,
    "PAUSE_BETWEEN_STEPS_SECONDS": 3
}

GECKO_TOP_PERFORMERS = {
    "BASE_URL": "https://api.geckoterminal.com/api/v2",
    "RATE_LIMIT_DELAY_SECONDS": 1.5,
    "REQUEST_TIMEOUT_SECONDS": 15,
    "RETRY_WAIT_SECONDS": 60,
    "MAX_RETRIES": 3,
    "MIN_AGE_HOURS": 24,
    "NETWORKS": ("base", "bsc"),
    "LIMIT": 30,
    "MIN_PRICE_CHANGE_24H": 20,
    "MIN_VOLUME_24H": 5000,
    "MIN_LIQUIDITY": 3000,
    "MAX_FDV": 100000000,
    "MIN_TXNS_24H": 50,
    "MIN_BUYS_RATIO": 0.15,
    "PERF_WINDOWS_DAYS": (5, 3, 2),
    "MAX_POOLS_PER_NETWORK": 200,
    "MAX_POOLS_PER_NETWORK_MULTI": 30,
    "OUTPUT_DIR": ROOT_DIR / "data" / "raw" / "json",
    "MIN_MARKET_CAP": 500_000,
    "MAX_POOL_AGE_DAYS": 30,
    "OHLCV_LIMIT": 200,
    "OHLCV_AGGREGATE": 4,
    "OHLCV_AGE_THRESHOLD_HOURS": 168,
    "MIN_HOURS_BEFORE_EXPLOSION": 12,
    "MIN_EXPLOSION_PCT": 200,
}

CONSENSUS_LIVE = {
    "MIN_WHALES_CONSENSUS": 2,
    "PERIOD_DAYS": 5,
    "MAX_MARKET_CAP": 100_000_000,
    "MIN_MARKET_CAP": 100_000,
    "EXCLUDED_TOKENS": (
        "USDC", "USDT", "DAI", "BUSD", "ETH", "WETH", "BTC", "BITCOIN", "BNB", "ETHEREUM"
    ),
    "PRICE_CHECK_DELAY": 0.5,
    "UPDATE_INTERVAL_HOURS": 6,
    "PERFORMANCE_THRESHOLDS": {
        "MOON_SHOT": 1000,
        "EXCELLENT": 500,
        "TRES_BON": 100,
        "BON": 50,
        "POSITIF": 0,
        "NEGATIF": -30
    }
}

SCORE_ENGINE = {
    "EXCLUDED_TOKENS": (
        "USDC", "USDT", "DAI", "USDAI", "BUSD", "ETH", "WETH", "BTC", "WBTC", "BNB"
    ),
    "FIFO": {
        "STABLECOINS": (
            "USDT", "USDC", "DAI", "BUSD", "FRAX", "TUSD", "USDP", "UST", "MIM", "FEI",
            "USDD", "GUSD", "LUSD", "SUSD", "HUSD", "CUSD", "OUSD", "RUSD", "USDE",
            "USDBC", "FDUSD", "PYUSD", "CRVUSD", "MKUSD", "ULTRA", "EURC", "EURT",
            "USDC.E"
        ),
        "ETH_SYMBOLS": ("ETH", "WETH", "ETHEREUM"),
        "USD_PREFIX": "USD",
        "ETH_FALLBACK_PRICE": 3900.0,
        "MAX_PRICE_USD": 1_000_000,
        "MAX_VALUE_USD": 1_000_000_000,
        "AIRDROP_MAX_INVESTED": 0.01,
        "SLEEP_EVERY_WALLETS": 10,
        "SLEEP_SECONDS": 1
    },
    "WALLET_SCORING": {
        "MIN_TRADES": 5,
        "MIN_SIGNIFICANT_WINS": 3,
        "ROI_CONCENTRATION_TOP_N": 3,
        "ROI_CONCENTRATION_MAX_RATIO": 0.99,
        "ROI_WIN_THRESHOLD": 50,
        "ROI_LOSS_THRESHOLD": -20,
        "ROI_SCORE_BASE": 50,
        "ROI_SCORE_DIVISOR": 4.5,
        "ACTIVITY_LOG_MAX_TRADES": 20,
        "SUCCESS_SCORE_MULTIPLIER": 2,
        "QUALITY_BONUS_MULTIPLIER": 50,
        "SCORE_WEIGHTS": {"ROI": 0.40, "ACTIVITY": 0.25, "SUCCESS": 0.25, "QUALITY": 0.10},
        "CLASS_THRESHOLDS": {"ELITE": 80, "EXCELLENT": 60, "BON": 40, "MOYEN": 20}
    },
    "TIER_ANALYSIS": {
        "TIER_START_USD": 1000,
        "TIER_END_USD": 12000,
        "TIER_STEP_USD": 1000,
        "WIN_ROI_THRESHOLD": 50,
        "LOSS_ROI_THRESHOLD": -20
    },
    "OPTIMAL_THRESHOLD": {
        "ALPHA_BAYESIAN": 30,
        "MIN_TRADES_THRESHOLD": 5,
        "MIN_WINRATE_THRESHOLD": 20.0,
        "MIN_RELIABLE_ROI": 70,
        "STABILITY_THRESHOLD": 0.15,
        "QUALITY_THRESHOLD": 0.1,
        "MIN_TRADES_QUALITY": 10,
        "FILTER_QUALITY_MIN": 0.3,
        "PERCENTILE": 60,
        "J_SCORE_WEIGHTS": {"ROI": 0.5, "WINRATE": 0.3, "TRADES_LOG": 0.2},
        "PENALTY_COEF": 0.05,
        "ROI_SCORE_MAX": 300,
        "WINRATE_SCORE_MAX": 80,
        "VOLUME_SCORE_MAX_TRADES": 50,
        "NEUTRAL_RATE_TARGET": 20,
        "NEUTRAL_RATE_OVER_PENALTY": 30,
        "QUALITY_BASE": 0.1,
        "QUALITY_SCALE": 0.9,
        "STATUS_THRESHOLDS": {
            "EXCEPTIONAL": 0.9,
            "EXCELLENT": 0.7,
            "GOOD": 0.5,
            "AVERAGE": 0.3,
            "NEUTRAL": 0.15
        },
        "MIN_INSERT_QUALITY": 0.3,
        "MIN_OPTIMAL_ROI": 70,
        "MIN_OPTIMAL_WINRATE": 20
    }
}

WALLET_TRACKER = {
    "MIN_TOKEN_VOLUME_USD": 500,
    "MAX_TRANSACTIONS": 10000,
    "MAX_PORTFOLIO_TOKENS": 450,
    "TRASH_NAMES": ("test", "airdrop", "scam", "spam", "fake", "shit"),
    "TRASH_SYMBOLS": ("test", "fake", "scam", "spam", "lplz"),
    "RATE_LIMIT_SLEEP_SECONDS": 5,
    "PAGE_DELAY_SECONDS": 0.3,
    "MAX_PAGES_DEFAULT": 2000,
    "PAGE_SIZE": 100,
    "HTTP_TIMEOUT_SECONDS": 30,
    "BATCH_SIZE_DEFAULT": 10,
    "BATCH_DELAY_SECONDS": 30,
    "BATCH_DELAY_SECONDS_MAIN": 10,
    "WALLET_DELAY_SECONDS": 3
}

WALLET_BALANCES = {
    "MIN_TOKEN_VALUE_USD": 500,
    "MIN_WALLET_VALUE_USD": 100000,
    "MAX_WALLET_VALUE_USD": 50000000,
    "MIN_TOKENS_PER_WALLET": 3,
    "MAX_TOKENS_PER_WALLET": 60,
    "BATCH_SIZE": 5,
    "DELAY_BETWEEN_BATCHES": 5,
    "PERIODS": ("14d", "30d", "200d", "360d", "manual"),
    "EXCLUDED_TOKENS": (
        "USDC", "USDT", "DAI", "BUSD", "FRAX", "TUSD", "USDP", "GUSD", "LUSD", "MIM", "USTC", "UST",
        "USDD", "USDN", "HUSD", "SUSD", "CUSD", "DUSD", "OUSD", "MUSD", "ZUSD", "RUSD", "VUSD",
        "USDX", "USDK", "EURS", "EURT", "CADC", "XSGD", "IDRT", "TRYB", "NZDS", "BIDR",
        "ETH", "WETH", "ETHEREUM", "STETH", "WSTETH", "RETH", "CBETH", "FRXETH", "SFRXETH",
        "ANKRETH", "SETH2", "ALETH", "AETHC", "QETH", "EETH", "WEETH", "OETH", "WOETH",
        "METH", "SWETH", "XETH", "LSETH", "UNIETH", "PXETH", "APXETH", "YETH", "EZETH",
        "RSETH", "UNIAETH", "ETHX", "SAETH", "TETH", "VETH", "DETH", "HETH", "PTETH",
        "BTC", "WBTC", "BITCOIN", "RENBTC", "SBTC", "HBTC", "OBTC", "TBTC", "WIBBTC",
        "PBTC", "XBTC", "BBTC", "FBTC", "LBTC", "CBTC", "VBTC", "RBTC", "KBTC", "ABTC",
        "BTCB", "MBTC", "UBTC", "DBTC", "NBTC", "GBTC", "YBTC", "ZBTC",
        "BNB", "BNBCHAIN", "WBNB", "BBNB", "SBNB", "VBNB",
        "WMATIC", "SMATIC", "STMATIC", "MATIC", "POLYGON",
        "WAVAX", "SAVAX", "STAVAX", "AVAX", "AVALANCHE",
        "WSOL", "SSOL", "STSOL", "SOL", "SOLANA",
        "WFTM", "SFTM", "STFTM", "FTM", "FANTOM",
        "WDOT", "SDOT", "STDOT", "DOT", "POLKADOT",
        "WADA", "SADA", "STADA", "ADA", "CARDANO",
        "WATOM", "SATOM", "STATOM", "ATOM", "COSMOS",
        "NEAR", "WNEAR", "STNEAR", "LINEAR",
        "LUNA", "WLUNA", "STLUNA", "TERRA",
        "ONE", "WONE", "STONE", "HARMONY", "TONE", "VONE", "SONE"
    ),
    "TOKEN_LOOKUP_DELAY": 0.2,
    "FUNGIBLE_TIMEOUT_SECONDS": 10,
    "POSITIONS_TIMEOUT_SECONDS": 30,
    "RATE_LIMIT_SLEEP_SECONDS": 5
}

TRACKING_LIVE = {
    "MIN_TOKEN_QUANTITY": 0.001,
    "MIN_TOKEN_VALUE_USD": 500,
    "HOURS_LOOKBACK_DEFAULT": 24,
    "BATCH_SIZE": 5,
    "DELAY_BETWEEN_BATCHES": 10,
    "SMART_WALLETS_LIMIT": 100,
    "TOKEN_LOOKUP_DELAY": 0.3,
    "RATE_LIMIT_SLEEP_SECONDS": 5,
    "HTTP_TIMEOUT_SECONDS": 20,
    "FUNGIBLE_TIMEOUT_SECONDS": 15,
    "HTTP_RETRY_TOTAL": 3,
    "HTTP_RETRY_BACKOFF": 1,
    "HTTP_RETRY_STATUS": (429, 500, 502, 503, 504),
    "HTTP_RETRY_METHODS": ("GET",),
    "API_KEY_ROTATION_SLEEP_SECONDS": 3,
    "WALLET_DELAY_SECONDS": 3,
    "POSITION_CHANGE_MIN_RATIO": 0.001,
    "POSITION_CHANGE_MIN_USD": 10,
    "TX_PAGE_SIZE": 100,
    "TX_RETRIES": 3,
    "TX_PAGE_DELAY_SECONDS": 2.0,
    "TX_RETRY_DELAY_SECONDS": 2,
    "TX_RATE_LIMIT_SLEEP_SECONDS": 5,
    "TX_TOKEN_DELAY_SECONDS": 1.5,
    "TX_WALLET_DELAY_SECONDS": 2,
    "TX_SWAP_RATIO_THRESHOLD": 0.8,
    "TX_HTTP_TIMEOUT_SECONDS": 15
}

MIGRATION_DETECTOR = {
    "HOURS_LOOKBACK": 168,
    "MIN_TRANSFER_PERCENTAGE": 70,
    "MAX_DAYS": 7,
    "MAX_PAGES": 10,
    "RETRIES": 3,
    "RATE_LIMIT_SLEEP_SECONDS": 3,
    "PAGE_DELAY_SECONDS": 1.5,
    "RETRY_DELAY_SECONDS": 2,
    "SHORT_SLEEP_SECONDS": 0.5,
    "AFTER_MIGRATION_SLEEP_SECONDS": 1.0
}

TOKEN_DISCOVERY = {
    "PERIODS": ("14d", "30d", "200d", "1y"),
    "TOP_N": 8,
    "MAX_TOKENS": 3000,
    "DELAY_BETWEEN": 30
}

TOKEN_ENRICHMENT = {
    "VALID_PERIODS": ("1h", "24h", "7d", "14d", "30d", "200d", "1y"),
    "TOP_N": 8,
    "MAX_TOKENS": 1000,
    "PAGE_SIZE": 250,
    "PAGE_DELAY_SECONDS": 3,
    "RATE_LIMIT_DELAY": 20,
    "SAFE_REQUEST_RETRIES": 3,
    "SAFE_REQUEST_DELAY": 15,
    "COINGECKO_VOLUME_MIN_USD": 1000000,
    "EVM_PLATFORMS": (
        "ethereum", "binance smart chain", "base",
        "arbitrum one", "polygon pos", "optimistic ethereum",
        "avalanche", "fantom", "moonriver", "cronos",
        "bnb", "linea", "scroll", "zksync", "mantle", "blast"
    )
}

SMART_CONTRACT_REMOVER = {
    "BATCH_SIZE": 20,
    "DELAY_BETWEEN_CALLS": 0.5,
    "DELAY_BETWEEN_BATCHES": 5,
    "MAX_RETRIES": 3,
    "TIMEOUT_SECONDS": 10
}

TELEGRAM = {
    "QUALITY_MARKET_CAP_THRESHOLDS": {
        "ULTRA_HIGH": 50_000_000,
        "HIGH": 10_000_000,
        "MEDIUM": 1_000_000
    },
    "FORMATION_INVESTMENT_THRESHOLDS": {
        "EXPLOSIVE": 100_000,
        "RAPID": 50_000
    },
    "DEFAULT_CHAIN_ID": "ethereum"
}
