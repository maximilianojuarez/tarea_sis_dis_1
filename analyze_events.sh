#!/bin/bash
# filepath: /home/max/Escritorio/tarea_sis_dis_1/analyze_events.sh

echo "🚦 ESTADÍSTICAS SIMPLES - SISTEMA ETL TRÁFICO"
echo "=============================================="

# Función para mostrar ayuda
show_help() {
    echo "Uso: $0 [OPCIONES]"
    echo ""
    echo "OPCIONES:"
    echo "  -c, --comuna NOMBRE    Mostrar estadísticas de una comuna específica"
    echo "  -l, --list            Listar todas las comunas disponibles"
    echo "  -h, --help            Mostrar esta ayuda"
    echo ""
    echo "Ejemplos:"
    echo "  $0                    # Análisis completo"
    echo "  $0 -c Santiago        # Solo datos de Santiago"
    echo "  $0 -c \"Las Condes\"    # Comuna con espacios (usar comillas)"
    echo "  $0 -l                 # Listar todas las comunas"
}

# Función para listar comunas disponibles
list_comunas() {
    echo "🏘️ COMUNAS DISPONIBLES:"
    echo "──────────────────────────"
    if [ -d "etl/agg/by_comuna" ]; then
        for folder in etl/agg/by_comuna/*/; do
            if [ -d "$folder" ] && [ "$(basename "$folder")" != "_SUCCESS" ]; then
                name=$(basename "$folder")
                count=$(find "$folder" -type f ! -name "_SUCCESS" -exec cat {} \; 2>/dev/null | wc -l)
                printf "  %-20s %6d eventos\n" "$name" "$count"
            fi
        done | sort
    else
        echo "  ❌ No se encontraron datos de comunas"
    fi
    exit 0
}

# Función para analizar una comuna específica
analyze_comuna() {
    local comuna_name="$1"
    echo "🏙️ ANÁLISIS ESPECÍFICO: $comuna_name"
    echo "=============================================="
    
    # Buscar la comuna (insensible a mayúsculas/minúsculas)
    comuna_folder=""
    for folder in etl/agg/by_comuna/*/; do
        if [ -d "$folder" ] && [ "$(basename "$folder")" != "_SUCCESS" ]; then
            folder_name=$(basename "$folder")
            if [ "$(echo "$folder_name" | tr '[:upper:]' '[:lower:]')" = "$(echo "$comuna_name" | tr '[:upper:]' '[:lower:]')" ]; then
                comuna_folder="$folder"
                break
            fi
        fi
    done
    
    if [ -z "$comuna_folder" ]; then
        echo "❌ Comuna '$comuna_name' no encontrada."
        echo ""
        echo "💡 Usa '$0 -l' para ver las comunas disponibles"
        exit 1
    fi
    
    # Contar eventos en la comuna
    eventos_comuna=$(find "$comuna_folder" -type f ! -name "_SUCCESS" -exec cat {} \; 2>/dev/null | wc -l)
    
    echo "📊 Total de eventos: $eventos_comuna"
    echo ""
    
    # Análisis por tipo de incidente en esa comuna
    echo "🚨 TIPOS DE INCIDENTES EN $comuna_name:"
    echo "──────────────────────────"
    if [ $eventos_comuna -gt 0 ]; then
        find "$comuna_folder" -type f ! -name "_SUCCESS" -exec cat {} \; 2>/dev/null | cut -f2 | sort | uniq -c | sort -rn | while read count type; do
            case "$type" in
                "traffic_jam") emoji="🚗" desc="Congestión" ;;
                "hazard") emoji="⚠️" desc="Peligro" ;;
                "police") emoji="👮" desc="Policía" ;;
                "accident") emoji="💥" desc="Accidente" ;;
                "road_closed") emoji="🚧" desc="Vía Cerrada" ;;
                *) emoji="📍" desc="Otro" ;;
            esac
            percentage=$(echo "scale=1; $count * 100 / $eventos_comuna" | bc -l 2>/dev/null || echo "0")
            printf "  %s %-12s %4d eventos (%s%%)\n" "$emoji" "$desc" "$count" "$percentage"
        done
    else
        echo "  ⚠️ No hay eventos registrados"
    fi
    
    echo ""
    echo "⏰ DISTRIBUCIÓN POR HORAS EN $comuna_name:"
    echo "──────────────────────────"
    # Buscar en qué horas aparece esta comuna
    if [ -d "etl/agg/temporal/by_hour" ]; then
        for hour_folder in etl/agg/temporal/by_hour/*/; do
            if [ -d "$hour_folder" ]; then
                hour=$(basename "$hour_folder")
                count_in_hour=$(find "$hour_folder" -type f ! -name "_SUCCESS" -exec grep -c "$comuna_name" {} \; 2>/dev/null | awk '{sum+=$1} END {print sum+0}')
                if [ $count_in_hour -gt 0 ]; then
                    printf "  %02d:00    %4d eventos\n" "$hour" "$count_in_hour"
                fi
            fi
        done | sort -n
    fi
    
    exit 0
}

# Función simple para contar líneas en archivos
count_lines() {
    local dir=$1
    local title=$2
    local show_top=$3  # Parámetro opcional para limitar resultados
    
    echo -e "\n📊 $title:"
    echo "──────────────────────────"
    
    if [ -d "$dir" ]; then
        local total=0
        local top_total=0
        local temp_file=$(mktemp)
        
        # Recopilar todos los datos
        for folder in "$dir"/*/; do
            if [ -d "$folder" ] && [ "$(basename "$folder")" != "_SUCCESS" ]; then
                name=$(basename "$folder")
                count=$(find "$folder" -type f ! -name "_SUCCESS" -exec cat {} \; 2>/dev/null | wc -l)
                if [ $count -gt 0 ]; then
                    echo "$count $name" >> "$temp_file"
                    total=$((total + count))
                fi
            fi
        done
        
        # Mostrar resultados ordenados
        if [ -s "$temp_file" ]; then
            if [ ! -z "$show_top" ]; then
                # Mostrar solo el top N y calcular su suma
                sort -rn "$temp_file" | head -"$show_top" | while read count name; do
                    printf "  %-15s %6d eventos\n" "$name" "$count"
                done
                
                # Calcular suma solo del TOP N
                top_total=$(sort -rn "$temp_file" | head -"$show_top" | awk '{sum+=$1} END {print sum+0}')
                
                echo "──────────────────────────"
                printf "  %-15s %6d TOTAL (TOP %d)\n" "🔥 $title" "$top_total" "$show_top"
            else
                # Mostrar todos
                sort -rn "$temp_file" | while read count name; do
                    printf "  %-15s %6d eventos\n" "$name" "$count"
                done
                echo "──────────────────────────"
                printf "  %-15s %6d TOTAL\n" "🔥 $title" "$total"
            fi
        fi
        
        rm -f "$temp_file"
    else
        echo "  ❌ No existe: $dir"
    fi
}

# Procesar argumentos de línea de comandos
while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--comuna)
            if [ -z "$2" ]; then
                echo "❌ Error: Debes especificar el nombre de la comuna"
                echo "Ejemplo: $0 -c Santiago"
                exit 1
            fi
            analyze_comuna "$2"
            ;;
        -l|--list)
            list_comunas
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo "❌ Opción desconocida: $1"
            echo "Usa '$0 -h' para ver la ayuda"
            exit 1
            ;;
    esac
    shift
done

# ANÁLISIS COMPLETO (cuando no se especifican opciones)

# ESTADÍSTICAS RÁPIDAS
echo "📄 Eventos en CSV: $(tail -n +2 etl/events_raw.csv 2>/dev/null | wc -l)"
echo "📁 Archivos JSON: $(find data -name "*.json" 2>/dev/null | wc -l)"

# HORAS PICO (directo del archivo)
echo -e "\n⏰ RANKING HORAS PICO:"
echo "──────────────────────────"
if [ -f "etl/agg/temporal/peak_hours/part-r-00000" ]; then
    while IFS=$'\t' read -r hour count; do
        if [ ! -z "$hour" ]; then
            # Agregar emoji según la hora
            if [ "$hour" -ge 6 ] && [ "$hour" -lt 12 ]; then
                period="🌅"
            elif [ "$hour" -ge 12 ] && [ "$hour" -lt 18 ]; then
                period="☀️"
            elif [ "$hour" -ge 18 ] && [ "$hour" -lt 24 ]; then
                period="🌆"
            else
                period="🌙"
            fi
            printf "  %s %02d:00  %'6d eventos\n" "$period" "$hour" "$count"
        fi
    done < etl/agg/temporal/peak_hours/part-r-00000
fi

# CONTEOS POR CATEGORÍA
count_lines "etl/agg/temporal/by_hour" "POR HORA"
count_lines "etl/agg/by_comuna" "TOP 10 COMUNAS" 10  # Limitar a 10
count_lines "etl/agg/by_type" "POR TIPO"

# HORA PICO DETECTADA
echo -e "\n🎯 HORA PICO:"
if [ -d "etl/agg/temporal/peak_hours_analysis" ]; then
    peak_folder=$(find etl/agg/temporal/peak_hours_analysis -maxdepth 1 -name "hora_peek_*" -type d 2>/dev/null | head -1)
    if [ ! -z "$peak_folder" ]; then
        folder_name=$(basename "$peak_folder")
        peak_count=$(find "$peak_folder" -type f ! -name "_SUCCESS" -exec cat {} \; 2>/dev/null | wc -l)
        peak_hour=$(echo "$folder_name" | sed 's/hora_peek_\([0-9]*\)h/\1/')
        
        echo "  🎯 Carpeta: $folder_name"
        echo "  🎯 Hora: $peak_hour:00"
        echo "  📊 Eventos críticos: $peak_count"
        
        # Mostrar tipos de incidentes en hora pico
        if [ $peak_count -gt 0 ]; then
            echo "  📋 Tipos en hora pico:"
            find "$peak_folder" -name "*" -type f -exec cat {} \; 2>/dev/null | cut -f2 | sort | uniq -c | sort -rn | head -5 | while read count type; do
                case "$type" in
                    "traffic_jam") emoji="🚗" ;;
                    "hazard") emoji="⚠️" ;;
                    "police") emoji="👮" ;;
                    "accident") emoji="💥" ;;
                    "road_closed") emoji="🚧" ;;
                    *) emoji="📍" ;;
                esac
                printf "     %s %-12s %4d eventos\n" "$emoji" "$type" "$count"
            done
        fi
    fi
fi

# RESUMEN FINAL
echo -e "\n📈 RESUMEN EJECUTIVO:"
echo "──────────────────────────"
total_comunas=$(find etl/agg/by_comuna -maxdepth 1 -type d 2>/dev/null | grep -v "_SUCCESS" | grep -v "by_comuna$" | wc -l)
total_horas=$(find etl/agg/temporal/by_hour -maxdepth 1 -type d 2>/dev/null | grep -v "_SUCCESS" | grep -v "by_hour$" | wc -l)

echo "  🏘️  Comunas monitoreadas: $total_comunas"
echo "  ⏰ Horas con actividad: $total_horas"
echo "  🚨 Tipos de incidentes: 5"

echo -e "\n✨ Análisis completado - $(date '+%H:%M:%S')"
echo "🌎 Sistema ETL Región Metropolitana - Totalmente Operacional"