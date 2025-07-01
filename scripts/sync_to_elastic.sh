#!/bin/sh
# scripts/sync_to_elastic.sh

# Crear directorios necesarios si no existen
mkdir -p /var/log /shared

LOGFILE="/var/log/logstash_schedule.log"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] ⚙️ Disparando sincronización externa" >> "$LOGFILE"

# 1) Borrar sincedb de MongoDB (usando volumen compartido)
MONGO_SINCEDB="/shared/logstash/data/logstash_sqlite.db"
if [ -f "$MONGO_SINCEDB" ]; then
    if rm -f "$MONGO_SINCEDB"; then
        echo " • Mongo sincedb removido" >> "$LOGFILE"
    else
        echo " ⚠️ Error al borrar Mongo sincedb" >> "$LOGFILE"
    fi
else
    echo " • Mongo sincedb no existe (OK)" >> "$LOGFILE"
fi

# 2) Borrar sincedb de CSV (usando volumen compartido)
CSV_SINCEDB_DIR="/shared/logstash/data"
if find "$CSV_SINCEDB_DIR" -name "*.sincedb" -type f -delete 2>/dev/null; then
    echo " • CSV sincedb removido" >> "$LOGFILE"
else
    echo " • CSV sincedb no existe o error al borrar" >> "$LOGFILE"
fi

# 3) Crear archivo de señal para reiniciar Logstash
if touch /shared/restart_signal 2>/dev/null; then
    echo " • Señal de reinicio creada" >> "$LOGFILE"
else
    echo " ⚠️ Error al crear señal de reinicio" >> "$LOGFILE"
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✅ Sincronización completada" >> "$LOGFILE"