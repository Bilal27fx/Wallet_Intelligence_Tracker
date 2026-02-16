#!/bin/bash
# Sync DB VPS → local (safe: stop scheduler, VACUUM INTO, copy, restart)
set -e

VPS="root@46.224.0.146"
VPS_PASS="Billy123456@"
VPS_DB="/root/WIT_V1_perso/data/db/wit_database.db"
VPS_TMP="/tmp/wit_clean_vps.db"
LOCAL_DB="/Users/bilalmeziane/Desktop/Bilal_changement_statut/WIT_V1_perso/data/db/wit_database.db"
LOG="/Users/bilalmeziane/Desktop/Bilal_changement_statut/WIT_V1_perso/logs/sync_db.log"

mkdir -p "$(dirname "$LOG")"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Démarrage sync VPS → local" >> "$LOG"

sshpass -p "$VPS_PASS" ssh -o StrictHostKeyChecking=no "$VPS" "
  systemctl stop wit_scheduler
  rm -f $VPS_TMP
  sqlite3 $VPS_DB \"VACUUM INTO '$VPS_TMP'\"
  echo ok
" >> "$LOG" 2>&1

# Supprimer les WAL/SHM locaux avant de remplacer le fichier
rm -f "${LOCAL_DB}-wal" "${LOCAL_DB}-shm"
sshpass -p "$VPS_PASS" scp -o StrictHostKeyChecking=no "$VPS:$VPS_TMP" "$LOCAL_DB" >> "$LOG" 2>&1

sshpass -p "$VPS_PASS" ssh -o StrictHostKeyChecking=no "$VPS" "
  rm -f $VPS_TMP
  systemctl start wit_scheduler
" >> "$LOG" 2>&1

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Sync terminé: $(sqlite3 "$LOCAL_DB" 'SELECT COUNT(*) FROM wallets;') wallets, $(sqlite3 "$LOCAL_DB" 'SELECT COUNT(*) FROM transaction_history;') txs" >> "$LOG"
