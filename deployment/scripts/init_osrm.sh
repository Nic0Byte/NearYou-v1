#!/usr/bin/env bash
set -e

DATA_DIR=/data
PBF_FILE="${DATA_DIR}/milano.osm.pbf"
MIN_SIZE=10000000           # 10 MB soglia minima

# 1) Scarica (o riscarica) il PBF
if [ -f "${PBF_FILE}" ]; then
  actual_size=$(stat -c%s "${PBF_FILE}")
  if [ "${actual_size}" -lt "${MIN_SIZE}" ]; then
    echo "  PBF presente ma troppo piccolo (${actual_size} byte). Lo riscarico."
    rm -f "${PBF_FILE}"
  else
    echo "  PBF già presente e sembra integro, salto il download."
  fi
fi

if [ ! -f "${PBF_FILE}" ]; then
  echo " Scarico PBF di Milano da ${PBF_URL}…"
  curl -sSL "${PBF_URL}" -o "${PBF_FILE}"
  echo " Download completato."
fi

# 2) Preprocess OSRM
echo " Inizio preprocess OSRM…"
osrm-extract   -p /opt/profiles/bicycle.lua "${PBF_FILE}"
osrm-partition "${DATA_DIR}/milano.osrm"
osrm-customize "${DATA_DIR}/milano.osrm"

# 3) Avvio OSRM routing
echo " Avvio OSRM routing…"
exec osrm-routed --algorithm mld --port 5000 "${DATA_DIR}/milano.osrm"
