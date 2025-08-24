#!/usr/bin/env bash
# Simple backup: zip data + OppWorks_Procurement into /persist/backups with timestamp
set -e
ROOT="${OPP_DATA_ROOT:-.}"
DEST="$ROOT/backups"
mkdir -p "$DEST"
TS=$(date +"%Y%m%d_%H%M%S")
zip -r "$DEST/oppworks_backup_$TS.zip" "$ROOT/data" "$ROOT/OppWorks_Procurement"  >/dev/null
echo "Backup written to $DEST/oppworks_backup_$TS.zip"
