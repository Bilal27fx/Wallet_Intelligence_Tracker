#!/usr/bin/env python3
"""Scoring wallet: trade quality + ROI 12m + success rate, with recency/activity gates."""

import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from smart_wallet_analysis.config import DB_PATH, SCORE_ENGINE, WALLET_BALANCES
from smart_wallet_analysis.logger import get_logger

logger = get_logger("score_engine.wallet_scorer")
_WS = SCORE_ENGINE["WALLET_SCORER"]


def _cfg(key: str, default):
    return _WS.get(key, default)


def _safe_div(num: float, den: float) -> float:
    return num / den if den else 0.0


def _parse_date(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _cutoff(days: int) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=days)


def _get_wallet_trades(wallet: str, conn: sqlite3.Connection) -> List[Dict]:
    """Trades valides (airdrop exclus, ticket minimum, tokens exclus)."""
    excluded = SCORE_ENGINE["EXCLUDED_TOKENS"]
    min_trade_size = float(_cfg("MIN_TRADE_SIZE_USD", 50.0))

    query = """
        SELECT profit_loss, total_invested, roi_percentage, first_transaction_date
        FROM token_analytics
        WHERE wallet_address = ?
          AND is_airdrop = 0
          AND total_invested >= ?
    """
    params: List = [wallet, min_trade_size]

    if excluded:
        query += f" AND token_symbol NOT IN ({','.join('?' * len(excluded))})"
        params.extend(excluded)

    rows = conn.execute(query, params).fetchall()
    trades: List[Dict] = []
    for pnl, invested, roi_pct, first_tx_date in rows:
        if invested is None or invested <= 0:
            continue
        trades.append(
            {
                "profit_loss": float(pnl or 0.0),
                "total_invested": float(invested),
                "roi_pct": float(roi_pct) if roi_pct is not None else None,
                "date": _parse_date(first_tx_date),
            }
        )
    return trades


def _window_subset(trades: List[Dict], days: int) -> List[Dict]:
    c = _cutoff(days)
    return [t for t in trades if t["date"] and t["date"] >= c]


def _window_roi(trades: List[Dict], days: int) -> Optional[float]:
    subset = _window_subset(trades, days)
    invested = sum(t["total_invested"] for t in subset)
    if invested <= 0:
        return None
    pnl = sum(t["profit_loss"] for t in subset)
    return _safe_div(pnl, invested) * 100


def _count_trades_last_days(trades: List[Dict], days: int) -> int:
    return len(_window_subset(trades, days))


def _trade_points(roi: float) -> float:
    # <0: -0.2 | 0-50: 0 | 50-100: 1 | 100-300: 2 | 300+: 3
    if roi < 0:
        return float(_cfg("TRADE_NEGATIVE_POINTS", -0.2))
    if roi < 50:
        return 0.0
    if roi < 100:
        return 1.0
    if roi < 300:
        return 2.0
    return 3.0


def _score_trade_component(rois: List[float]) -> float:
    if not rois:
        return 0.0
    avg_points = sum(_trade_points(roi) for roi in rois) / len(rois)
    max_points = float(_cfg("TRADE_POINTS_MAX", 3.0))
    weight = float(_cfg("WEIGHT_TRADE_SCORE", 40.0))
    normalized = min(max(_safe_div(avg_points, max_points), 0.0), 1.0)
    return normalized * weight


def _score_roi_12m_component(roi_12m: Optional[float]) -> float:
    if roi_12m is None:
        return 0.0
    cap = float(_cfg("ROI_12M_CAP", 200.0))
    weight = float(_cfg("WEIGHT_ROI_12M", 35.0))
    bounded = min(max(roi_12m, 0.0), cap)
    return _safe_div(bounded, cap) * weight


def _score_success_component(rois: List[float]) -> float:
    if not rois:
        return 0.0
    good_threshold = float(_cfg("GOOD_ROI_THRESHOLD", 50.0))
    good = sum(1 for roi in rois if roi >= good_threshold)
    hit_rate = _safe_div(good, len(rois))
    return hit_rate * float(_cfg("WEIGHT_SUCCESS_RATE", 25.0))


def _recency_multiplier(roi_1m: Optional[float]) -> float:
    if roi_1m is None:
        return float(_cfg("RECENCY_MULT_MISSING", 0.75))
    if roi_1m >= 50:
        return float(_cfg("RECENCY_MULT_STRONG", 1.0))
    if roi_1m >= 0:
        return float(_cfg("RECENCY_MULT_FLAT", 0.85))
    return float(_cfg("RECENCY_MULT_NEGATIVE", 0.60))


def _activity_multiplier(trades_30j: int, trades_90j: int) -> float:
    if trades_30j >= 3:
        return float(_cfg("ACTIVITY_MULT_30J_3PLUS", 1.0))
    if 1 <= trades_30j <= 2:
        return float(_cfg("ACTIVITY_MULT_30J_1_2", 0.8))
    if trades_30j == 0 and trades_90j >= 3:
        return float(_cfg("ACTIVITY_MULT_90J_3PLUS_NO30J", 0.5))
    return 0.0


def _assign_tier(score_final: float) -> int:
    if score_final >= _WS["TIER_THRESHOLDS"]["TIER1"]:
        return 1
    if score_final >= _WS["TIER_THRESHOLDS"]["TIER2"]:
        return 2
    return 3


def _watchlist_entry(wallet: str, nb_trades: int, reason: str) -> Dict:
    return {
        "wallet_address": wallet,
        "nb_trades": nb_trades,
        "watchlist": True,
        "watchlist_reason": reason,
    }


def score_wallet(wallet: str, conn: sqlite3.Connection) -> Optional[Dict]:
    trades = _get_wallet_trades(wallet, conn)
    if not trades:
        return None

    nb_trades = len(trades)
    min_trades = int(_cfg("MIN_TRADES_VALID", 10))
    if nb_trades < min_trades:
        return _watchlist_entry(wallet, nb_trades, "insufficient_trades")

    rois = [t["roi_pct"] for t in trades if t["roi_pct"] is not None]
    if len(rois) < min_trades:
        return _watchlist_entry(wallet, nb_trades, "insufficient_roi_data")

    trades_30j = _count_trades_last_days(trades, 30)
    trades_90j = _count_trades_last_days(trades, 90)

    # Hard filter #1: inactive wallet
    if trades_90j < int(_cfg("MIN_TRADES_90J_ACTIVE", 3)):
        return _watchlist_entry(wallet, nb_trades, "inactive_wallet")

    roi_1m = _window_roi(trades, 30)
    # Hard filter #2: poor recent performance
    if roi_1m is not None and roi_1m < float(_cfg("ROI_1M_HARD_FLOOR", -20.0)):
        return _watchlist_entry(wallet, nb_trades, "negative_recent_roi")

    roi_12m = _window_roi(trades, 365)

    # Hard filter #3: ROI minimum sur 1 mois
    if roi_1m is not None and roi_1m < float(_cfg("MIN_ROI_1M_ABSOLUTE", 50.0)):
        return _watchlist_entry(wallet, nb_trades, "insufficient_roi_1m")

    score_trade = _score_trade_component(rois)
    score_roi_12m = _score_roi_12m_component(roi_12m)
    score_success = _score_success_component(rois)

    base_score = score_trade + score_roi_12m + score_success
    recency = _recency_multiplier(roi_1m)
    activity = _activity_multiplier(trades_30j, trades_90j)
    score_final = round(base_score * recency * activity, 2)

    win_rate = _safe_div(sum(1 for roi in rois if roi > 0), len(rois))
    dates = [t["date"] for t in trades if t["date"]]
    anciennete = int(_safe_div((datetime.now(timezone.utc) - min(dates)).days, 30)) if dates else 0

    return {
        "wallet_address": wallet,
        "watchlist": False,
        "nb_trades": nb_trades,
        "win_rate": round(win_rate, 4),
        "anciennete_mois": anciennete,
        "trades_30j": trades_30j,
        "trades_90j": trades_90j,
        "roi_1m": roi_1m,
        "roi_12m": roi_12m,
        "score_trade": round(score_trade, 2),
        "score_roi_12m": round(score_roi_12m, 2),
        "score_success_rate": round(score_success, 2),
        "score_final": score_final,
    }


def _save_scores(scores: List[Dict], conn: sqlite3.Connection) -> int:
    today = datetime.now(timezone.utc).date().isoformat()
    cur = conn.cursor()
    cur.executemany(
        """
        INSERT OR REPLACE INTO wallet_scoring (
            wallet_address, tier, score_final,
            score_trade, score_roi_12m, score_success_rate,
            win_rate, nb_trades, anciennete_mois,
            trades_30j, trades_90j,
            roi_1m, roi_12m,
            date_scoring
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        [
            (
                s["wallet_address"],
                _assign_tier(s["score_final"]),
                s["score_final"],
                s["score_trade"],
                s["score_roi_12m"],
                s["score_success_rate"],
                s["win_rate"],
                s["nb_trades"],
                s["anciennete_mois"],
                s["trades_30j"],
                s["trades_90j"],
                s.get("roi_1m"),
                s.get("roi_12m"),
                today,
            )
            for s in scores
        ],
    )
    conn.commit()
    return cur.rowcount


def _save_watchlist(entries: List[Dict], conn: sqlite3.Connection) -> int:
    today = datetime.now(timezone.utc).date().isoformat()
    cur = conn.cursor()
    cur.executemany(
        """
        INSERT OR IGNORE INTO wallet_watchlist
            (wallet_address, date_entree, date_derniere_eval, nb_trades_actuel, raison, statut)
        VALUES (?, ?, ?, ?, ?, 'pending')
    """,
        [
            (e["wallet_address"], today, today, e["nb_trades"], e.get("watchlist_reason", "insufficient_trades"))
            for e in entries
        ],
    )
    cur.executemany(
        """
        UPDATE wallet_watchlist
        SET date_derniere_eval = ?, nb_trades_actuel = ?, raison = ?
        WHERE wallet_address = ? AND statut = 'pending'
    """,
        [
            (today, e["nb_trades"], e.get("watchlist_reason", "insufficient_trades"), e["wallet_address"])
            for e in entries
        ],
    )
    conn.commit()
    return cur.rowcount


def _remove_watchlisted_from_scores(entries: List[Dict], conn: sqlite3.Connection) -> int:
    if not entries:
        return 0
    cur = conn.cursor()
    cur.executemany("DELETE FROM wallet_scoring WHERE wallet_address = ?", [(e["wallet_address"],) for e in entries])
    conn.commit()
    return cur.rowcount


def _remove_under_threshold_scores(conn: sqlite3.Connection, min_wallet_value: float) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        DELETE FROM wallet_scoring
        WHERE wallet_address IN (
            SELECT ws.wallet_address
            FROM wallet_scoring ws
            LEFT JOIN wallets w ON w.wallet_address = ws.wallet_address
            WHERE COALESCE(w.total_portfolio_value, 0) < ?
        )
        """,
        (min_wallet_value,),
    )
    conn.commit()
    return cur.rowcount


def _ensure_wallet_scoring_schema(conn: sqlite3.Connection) -> None:
    desired_cols = [
        ("wallet_address", "TEXT PRIMARY KEY"),
        ("tier", "INTEGER"),
        ("score_final", "REAL"),
        ("score_trade", "REAL"),
        ("score_roi_12m", "REAL"),
        ("score_success_rate", "REAL"),
        ("win_rate", "REAL"),
        ("nb_trades", "INTEGER"),
        ("anciennete_mois", "INTEGER"),
        ("trades_30j", "INTEGER"),
        ("trades_90j", "INTEGER"),
        ("roi_1m", "REAL"),
        ("roi_12m", "REAL"),
        ("date_scoring", "DATE"),
    ]

    existing = conn.execute("PRAGMA table_info(wallet_scoring)").fetchall()
    existing_names = [r[1] for r in existing]
    desired_names = [c[0] for c in desired_cols]

    if existing_names == desired_names:
        return

    logger.info("Migration wallet_scoring -> schema allégé")
    conn.execute("BEGIN")
    conn.execute("DROP TABLE IF EXISTS wallet_scoring_new")
    conn.execute(
        "CREATE TABLE wallet_scoring_new (\n  " + ",\n  ".join(f"{n} {t}" for n, t in desired_cols) + "\n)"
    )

    # Map legacy columns to new schema when possible.
    legacy_map = {
        "score_trade": "confiance",
        "score_roi_12m": "performance_score",
        "score_success_rate": "consistance",
        "roi_12m": "roi_12m",
    }

    select_expr = []
    for name, _ in desired_cols:
        if name in existing_names:
            select_expr.append(name)
        elif name in legacy_map and legacy_map[name] in existing_names:
            select_expr.append(f"{legacy_map[name]} AS {name}")
        else:
            select_expr.append(f"NULL AS {name}")

    if existing_names:
        conn.execute(
            f"""
            INSERT INTO wallet_scoring_new ({', '.join(desired_names)})
            SELECT {', '.join(select_expr)}
            FROM wallet_scoring
            """
        )

    conn.execute("DROP TABLE IF EXISTS wallet_scoring")
    conn.execute("ALTER TABLE wallet_scoring_new RENAME TO wallet_scoring")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ws_score ON wallet_scoring(score_final DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ws_tier ON wallet_scoring(tier)")
    conn.commit()


def run_wallet_scoring() -> bool:
    try:
        conn = sqlite3.connect(DB_PATH)
        _ensure_wallet_scoring_schema(conn)
        min_wallet_value = float(WALLET_BALANCES["MIN_WALLET_VALUE_USD"])
        query = """
            SELECT DISTINCT ta.wallet_address
            FROM token_analytics ta
            INNER JOIN wallets w ON w.wallet_address = ta.wallet_address
            WHERE COALESCE(w.total_portfolio_value, 0) >= ?
        """
        wallets = [r[0] for r in conn.execute(query, (min_wallet_value,)).fetchall()]
    except Exception as e:
        logger.error("Erreur lecture wallets: %s", e)
        return False

    if not wallets:
        logger.warning("Aucun wallet dans token_analytics")
        conn.close()
        return False

    logger.info("Scoring de %s wallets (portfolio >= %s USD)", len(wallets), int(min_wallet_value))

    removed = _remove_under_threshold_scores(conn, min_wallet_value)
    if removed:
        logger.info("Suppression de %s wallets scorés sous le seuil portefeuille", removed)

    scored: List[Dict] = []
    watchlist: List[Dict] = []
    for wallet in wallets:
        result = score_wallet(wallet, conn)
        if result is None:
            continue
        if result.get("watchlist"):
            watchlist.append(result)
        else:
            scored.append(result)

    if scored:
        _save_scores(scored, conn)
        t1 = sum(1 for s in scored if _assign_tier(s["score_final"]) == 1)
        t2 = sum(1 for s in scored if _assign_tier(s["score_final"]) == 2)
        t3 = sum(1 for s in scored if _assign_tier(s["score_final"]) == 3)
        logger.info("Scores sauvegardes: %s wallets | T1=%s T2=%s T3=%s", len(scored), t1, t2, t3)

    if watchlist:
        _save_watchlist(watchlist, conn)
        _remove_watchlisted_from_scores(watchlist, conn)
        logger.info("Watchlist: %s wallets (filtres anti-chance non valides)", len(watchlist))

    conn.close()
    return True


if __name__ == "__main__":
    run_wallet_scoring()
