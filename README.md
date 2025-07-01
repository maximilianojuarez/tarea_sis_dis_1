# Sistema ETL de Análisis de Tráfico en Tiempo Real

Una plataforma distribuida para el análisis de tráfico en tiempo real en la Región Metropolitana, utilizando datos reales de la API de Waze para proporcionar información sobre patrones de tráfico, con capacidades de procesamiento ETL distribuido, análisis de datos avanzado y API REST para consultas en tiempo real.

## Tabla de Contenidos

- [Arquitectura](#arquitectura)
- [Componentes](#componentes)
- [Pipeline ETL](#pipeline-etl)
- [Estructura de Datos](#estructura-de-datos)
- [API REST](#api-rest)
- [Distribuciones de Tráfico](#distribuciones-de-tráfico)
- [Análisis de Datos](#análisis-de-datos)
- [Pipelines de Logstash](#pipelines-de-logstash)
- [Requisitos](#requisitos)
- [Instalación](#instalación)
- [Uso](#uso)
- [Interfaces Web](#interfaces-web)
- [Monitoreo](#monitoreo)
- [Características Avanzadas](#características-avanzadas)

## Arquitectura

El sistema sigue una arquitectura de flujo de datos modular con pipeline ETL distribuido y API REST:

1. **Recolección de Datos**: El scraper extrae datos de tráfico en tiempo real de la API de Waze
2. **Almacenamiento**: Los datos se almacenan en MongoDB con indexación UUID para una recuperación eficiente
3. **Pipeline ETL**: Procesamiento distribuido con Apache Pig para agregaciones y análisis
4. **Indexación Elasticsearch**: Pipelines Logstash procesan datos desde MongoDB y CSV hacia Elasticsearch
5. **API REST**: Sistema de consultas con caché inteligente Redis y múltiples políticas
6. **Análisis**: Sistema de estadísticas automatizado para insights de tráfico

## Componentes

### Scraper

- Ubicado en [scraper/scraper.py](scraper/scraper.py)
- Extrae incidentes de tráfico de la API de Waze (accidentes, congestión, peligros)
- Intervalos de sondeo configurables (5-10 minutos aleatorios)
- Formato de salida JSON y CSV
- Mapeo automático de coordenadas a comunas de la RM
- Validación y normalización de datos en tiempo real
- **Nuevas características**:
  - Cuadrantes geográficos para mejor cobertura de la RM
  - Distribución inteligente de eventos por comuna
  - Manejo mejorado de IDs de Waze y UUIDs personalizados

### Sistema de Almacenamiento

- Ubicado en [storage/data_loader.py](storage/data_loader.py)
- Almacenamiento persistente basado en MongoDB
- Indexación UUID para recuperación eficiente
- Indexación geoespacial para consultas basadas en ubicación
- Exportación automática a CSV para procesamiento ETL
- Deduplicación automática de eventos

### Pipeline ETL Distribuido

- Ubicado en [pig-scripts/processing.pig](pig-scripts/processing.pig)
- Procesamiento con Apache Pig en contenedor Docker personalizado
- Agregaciones múltiples:
  - **Por hora**: Distribución temporal de incidentes (00-23h)
  - **Por comuna**: Análisis geográfico detallado de la RM
  - **Por tipo**: Clasificación de tipos de incidentes con estadísticas
  - **Hora pico dinámica**: Detección automática de la hora con más actividad
- Filtrado avanzado con expresiones regulares
- Validación de timestamps y eventos duplicados
- **Script de filtrado adicional**: [pig-scripts/filter_homogenize.pig](pig-scripts/filter_homogenize.pig)

### API REST de Consultas Avanzada

- Ubicado en [cache/app.py](cache/app.py)
- **Migrado a FastAPI** para mejor rendimiento y documentación automática
- Sistema de caché inteligente Redis con múltiples políticas:
  - **LRU** (Least Recently Used)
  - **LFU** (Least Frequently Used)
  - **TTL** (Time To Live) configurable
- **Nuevos endpoints**:
  - `POST /api/v1/query` - Consultas avanzadas con tipos específicos
  - `GET /api/v1/health` - Health check del sistema
  - `POST /api/v1/cache/configure` - Configuración dinámica del caché
  - `GET /query` - Endpoint de compatibilidad con sistema anterior
  - `GET /stats` - Estadísticas del caché y sistema
- **Integración con Elasticsearch**:
  - Consultas por incidentes, flujo de tráfico y agregadas
  - Respaldo automático desde MongoDB
  - Cache miss handling con fallback inteligente
- Métricas detalladas de rendimiento y hit/miss ratio

### Sistema de Análisis Inteligente

- Ubicado en [analyze_events.sh](analyze_events.sh)
- Script bash avanzado con múltiples modos de operación
- Análisis automatizado de estadísticas ETL
- Reportes detallados con visualización ASCII
- Detección de horas pico y comunas críticas
- **Análisis por comuna específica** con búsqueda insensible a mayúsculas
- **Listado completo de comunas** disponibles
- **Métricas ejecutivas** del sistema

### Sincronización de Datos

- Ubicado en [scripts/sync_to_elastic.sh](scripts/sync_to_elastic.sh)
- Script automatizado para sincronización con Elasticsearch
- Limpieza de sincedb files para reindexación completa
- Señales de reinicio para Logstash
- Logging detallado de operaciones de sincronización

## Pipeline ETL

### Procesamiento de Datos Avanzado

El pipeline ETL procesa los datos en las siguientes etapas optimizadas:

1. **Extracción**: 
   - Carga datos desde `etl/events_raw.csv`
   - Filtrado automático de headers
   - Validación de formato de entrada

2. **Transformación**: 
   - Filtrado de eventos válidos con expresiones regulares
   - Extracción de patrones temporales precisos
   - Normalización de datos geográficos
   - Clasificación automática de tipos de incidentes
   - Validación de timestamps con formato ISO 8601

3. **Carga**: 
   - Generación de agregaciones en directorios organizados
   - Particionamiento automático por criterios múltiples
   - Creación de índices para consultas rápidas

### Agregaciones Generadas

```
etl/agg/
├── temporal/
│   ├── by_hour/           # Eventos agrupados por hora (00-23)
│   │   ├── 00/           # Eventos de medianoche
│   │   ├── 01/           # Eventos de 1:00 AM
│   │   └── ...           # Hasta 23:00
│   ├── peak_hours/        # Ranking completo de horas por volumen
│   └── peak_hours_analysis/
│       └── hora_peek_XXh/ # Análisis detallado de hora pico detectada
├── by_comuna/             # Eventos agrupados por las 36+ comunas de RM
│   ├── Santiago/          # Comuna de Santiago
│   ├── Las_Condes/        # Las Condes (espacios reemplazados)
│   └── ...               # Todas las comunas disponibles
└── by_type/               # Eventos clasificados por tipo de incidente
    ├── traffic_jam/       # Congestión vehicular
    ├── hazard/           # Peligros en la vía
    ├── police/           # Presencia policial
    ├── accident/         # Accidentes
    └── road_closed/      # Cierres de vías
```

### Scripts ETL Disponibles

#### Script Principal de Procesamiento
- **Archivo**: [pig-scripts/processing.pig](pig-scripts/processing.pig)
- **Función**: Agregaciones principales por hora, comuna, tipo y detección de hora pico
- **Uso**: Procesamiento completo de datos ETL

#### Script de Filtrado y Homogeneización
- **Archivo**: [pig-scripts/filter_homogenize.pig](pig-scripts/filter_homogenize.pig)
- **Función**: Limpieza de datos, validación y homogeneización
- **Características**:
  - Separación de eventos válidos e inválidos
  - Almacenamiento de datos inválidos en `/etl/fail`
  - Extracción de fechas y normalización de campos
  - Guardado de datos limpios en `/etl/clean`

### Detección Automática de Hora Pico

El sistema detecta dinámicamente la hora con mayor actividad:
- **Análisis Temporal**: Procesamiento de todas las 24 horas del día
- **Ranking Automático**: Ordenamiento por volumen de incidentes
- **Carpeta Dinámica**: Creación automática de `hora_peek_XXh/`
- **Ejemplo**: Si las 22:00 es la hora pico → `hora_peek_22h/`
- **Adaptativo**: Actualización automática cuando cambian los patrones
- **Contenido Detallado**: Análisis completo con tipos y comunas afectadas

## Estructura de Datos

Los eventos de tráfico siguen esta estructura normalizada:

```json
{
  "uuid": "waze_alert-123456789/abc-def-ghi",
  "type": "traffic_jam",
  "location": "-33.4489,-70.6693",
  "location_desc": "Santiago Centro",
  "description": "Congestión en Alameda con Matucana",
  "timestamp": "2025-06-01T21:00:29.123456",
  "source": "waze_api",
  "length_meters": 500,
  "speed": 15,
  "congestion_level": 3,
  "delay_seconds": 120,
  "waze_id": "waze_alert-123456789"
}
```

### Tipos de Incidentes Monitoreados

- **traffic_jam** 🚗: Congestión vehicular con niveles de intensidad
- **hazard** ⚠️: Peligros en la vía (objetos, condiciones climáticas)
- **police** 👮: Presencia policial y controles de tráfico
- **accident** 💥: Accidentes de tránsito con gravedad variable
- **road_closed** 🚧: Cierres temporales o permanentes de calles

### Comunas Monitoreadas (36+ Comunas RM)

El sistema cubre la totalidad de la Región Metropolitana:
- **Zona Centro**: Santiago, Providencia, Ñuñoa, Recoleta
- **Zona Oriente**: Las Condes, Vitacura, Lo Barnechea, La Reina
- **Zona Sur**: San Bernardo, Puente Alto, La Florida, Maipú
- **Zona Norte**: Colina, Lampa, Quilicura, Huechuraba
- **Zona Poniente**: Melipilla, Talagante, Padre Hurtado, Peñaflor
- Y muchas más comunas con cobertura completa

## API REST

### Nuevos Endpoints FastAPI

#### Consulta Avanzada de Datos de Tráfico
```bash
# Consulta con tipos específicos (INCIDENTS, TRAFFIC_FLOW, AGGREGATED)
POST /api/v1/query
Content-Type: application/json

{
  "query_type": "INCIDENTS",
  "filters": {
    "comuna": "Santiago",
    "incident_type": "traffic_jam",
    "time_range": {
      "start": "2025-01-01T00:00:00",
      "end": "2025-01-31T23:59:59"
    }
  }
}
```

#### Health Check del Sistema
```bash
# Verificar estado de todos los servicios
GET /api/v1/health

# Respuesta incluye:
# - Estado de MongoDB, Redis, Elasticsearch
# - Tiempos de respuesta de cada servicio
# - Métricas de disponibilidad
```

#### Configuración Dinámica del Caché
```bash
# Configurar parámetros del caché en tiempo real
POST /api/v1/cache/configure
Content-Type: application/json

{
  "policy": "LFU",
  "max_size": 2000,
  "default_ttl": 1800
}
```

### Endpoints de Compatibilidad

#### Consulta de Eventos (Legacy)
```bash
# Consultar evento específico por UUID (compatible con sistema anterior)
GET /query?id=waze_alert-123456789

# Estadísticas del sistema (formato original)
GET /stats
```

### Configuraciones de Caché Inteligente

- **Caché Multinivel**: Redis + caché en memoria para mayor rendimiento
- **Políticas Adaptativas**: LRU, LFU con cambio dinámico según patrones de uso
- **TTL Configurable**: Time To Live por tipo de consulta
- **Fallback Inteligente**: MongoDB → Elasticsearch → Error graceful
- **Métricas Avanzadas**: Hit/miss ratio, latencia promedio, memoria utilizada

## Análisis de Datos

### Script de Análisis Automatizado

El sistema incluye un script de análisis potente y flexible:

```bash
# Ejecutar análisis completo (modo por defecto)
./analyze_events.sh

# Analizar una comuna específica
./analyze_events.sh -c Santiago
./analyze_events.sh --comuna "Las Condes"

# Listar todas las comunas disponibles
./analyze_events.sh -l
./analyze_events.sh --list

# Ver ayuda completa y opciones
./analyze_events.sh -h
./analyze_events.sh --help
```

### Modos de Análisis Avanzados

#### 🏙️ Análisis por Comuna Específica
Análisis detallado y exhaustivo de una comuna individual:

- **Total de eventos** registrados en la comuna
- **Distribución por tipos** de incidentes con porcentajes detallados
- **Distribución temporal** por todas las horas del día
- **Búsqueda inteligente** insensible a mayúsculas/minúsculas
- **Métricas comparativas** con otras comunas

Ejemplo de salida detallada:
```
🏙️ ANÁLISIS ESPECÍFICO: Santiago
==============================================
📊 Total de eventos: 480

🚨 TIPOS DE INCIDENTES EN Santiago:
──────────────────────────
  🚗 Congestión      350 eventos (72.9%)
  ⚠️ Peligro         80 eventos (16.7%)
  👮 Policía         30 eventos (6.3%)
  🚧 Vía Cerrada     15 eventos (3.1%)
  💥 Accidente       5 eventos (1.0%)

⏰ DISTRIBUCIÓN POR HORAS EN Santiago:
──────────────────────────
  18:00      15 eventos
  19:00      22 eventos
  20:00      25 eventos
  21:00     180 eventos ★ PICO
  22:00     200 eventos ★★ MAYOR PICO
  23:00      75 eventos
  00:00      8 eventos
```

#### 📋 Listado Completo de Comunas
Directorio alfabético con estadísticas por comuna:

```
🏘️ COMUNAS DISPONIBLES (36 comunas):
──────────────────────────
  Cerrillos             45 eventos
  Colina               786 eventos ★★★
  Las Condes           234 eventos ★★
  Maipú                478 eventos ★★★
  Santiago             480 eventos ★★★
  Vitacura             156 eventos ★
  ...
──────────────────────────
TOTAL SISTEMA: 7,973 eventos
```

### Información Generada (Análisis Completo)

El análisis completo proporciona un dashboard ejecutivo:

- **📊 Estadísticas Generales**: Conteos totales y distribuciones
- **⏰ Ranking de Horas Pico**: Las 24 horas ordenadas por actividad
- **🏙️ Top 10 Comunas**: Ranking de comunas más activas
- **🚨 Distribución por Tipo**: Porcentajes precisos de cada incidente
- **🎯 Hora Pico Detectada**: Análisis profundo de la hora crítica
- **📈 Resumen Ejecutivo**: KPIs del sistema y métricas operativas

## Pipelines de Logstash

### Pipeline de Datos CSV

- **Archivo**: [logstash/pipelines/logstash_csv.conf](logstash/pipelines/logstash_csv.conf)
- **Función**: Procesa eventos desde archivos CSV hacia Elasticsearch
- **Características**:
  - **Input**: Lectura de `/etl/events_raw.csv`
  - **Filtros**:
    - Filtrado de headers y líneas vacías
    - Parsing CSV con manejo de errores
    - Validación de campos requeridos (uuid, event_type, timestamp)
    - Extracción de información temporal (hora, día de semana)
    - Detección de horas pico (21:00-23:00)
    - Conversión de tipos de datos (float, integer)
    - Limpieza de campos vacíos
  - **Output**: Indexación en Elasticsearch con patrón `traffic-events-csv-YYYY.MM.dd`

### Pipeline de Datos MongoDB

- **Archivo**: [logstash/pipelines/logstash_raw.conf](logstash/pipelines/logstash_raw.conf)
- **Función**: Sincroniza datos desde MongoDB hacia Elasticsearch
- **Características**:
  - **Input**: Conexión directa a MongoDB (`mongodb://mongodb:27017/traffic_db`)
  - **Configuración**:
    - Colección: `events`
    - Batch size: 1000 documentos
    - Placeholder database para estado de sincronización
  - **Filtros**:
    - Renombrado de campos (_id → mongo_id, type → incident_type)
    - Procesamiento de timestamps ISO8601
    - Extracción temporal con Ruby scripting
    - Detección de horas pico (7-9 AM, 5-7 PM)
    - Conversión de tipos de datos
    - Limpieza de campos vacíos
  - **Output**: Indexación en Elasticsearch con patrón `traffic-events-raw-YYYY.MM.dd`

### Configuración de Contenedor Logstash

- **Archivo**: [logstash/Dockerfile](logstash/Dockerfile)
- **Base**: Elasticsearch oficial 7.10.2
- **Plugins**: logstash-input-mongodb instalado
- **Permisos**: Configuración correcta de directorios y usuario logstash
- **Persistencia**: Volúmenes compartidos para datos de estado

### Sincronización Automatizada

- **Script**: [scripts/sync_to_elastic.sh](scripts/sync_to_elastic.sh)
- **Función**: Fuerza resincronización completa
- **Operaciones**:
  - Limpieza de archivos sincedb (MongoDB y CSV)
  - Creación de señales de reinicio
  - Logging detallado de operaciones
  - Manejo de errores y validaciones

## Requisitos

### Hardware Mínimo
- **RAM**: 6GB mínimo, 8GB recomendado para procesamiento ETL + Elasticsearch
- **CPU**: 2 cores mínimo, 4 cores recomendado
- **Almacenamiento**: 8GB espacio libre (3GB para datos + 5GB para contenedores)
- **Red**: Conexión estable a Internet para API de Waze

### Software
- **Docker**: 20.10+ y Docker Compose 2.0+
- **Python**: 3.8 o superior (para desarrollo local)
- **Sistema Operativo**: Linux (preferido), macOS, Windows con WSL2

### Recursos de Red
- Conexión a Internet para acceso a la API de Waze
- Puertos disponibles: 8000, 5000, 6379, 8081, 8082, 9200, 9300, 27017

## Instalación

### Instalación Rápida

1. **Clonar el repositorio**:
   ```bash
   git clone https://github.com/maximilianojuarez/tarea_sis_dis_1
   cd tarea_sis_dis_1
   git checkout tarea3  # Cambiar a la branch con las nuevas funcionalidades
   ```

2. **Configurar permisos**:
   ```bash
   chmod +x analyze_events.sh
   chmod +x scripts/*.sh
   ```

3. **Construir e iniciar la plataforma**:
   ```bash
   docker-compose build
   docker-compose up -d
   ```

4. **Verificar el estado de los servicios**:
   ```bash
   docker-compose ps
   # Todos los servicios deben mostrar "Up"
   ```

### Verificación de la Instalación

```bash
# Verificar nueva API FastAPI
curl http://localhost:8000/api/v1/health

# Verificar API de compatibilidad
curl http://localhost:8000/stats

# Verificar MongoDB
curl http://localhost:8081

# Verificar Elasticsearch
curl http://localhost:9200

# Verificar índices de Logstash
curl http://localhost:9200/_cat/indices?v
```

## Uso

### Iniciar la Plataforma Completa

```bash
# Iniciar todos los servicios en background
docker-compose up -d

# Iniciar servicios específicos (incluyendo logstash)
docker-compose up -d scraper cache mongodb elasticsearch logstash

# Ver logs en tiempo real de todos los servicios
docker-compose logs -f

# Ver logs de servicios específicos
docker-compose logs -f cache
docker-compose logs -f logstash
```

### Pipeline ETL Completo

```bash
# Ejecutar procesamiento ETL principal
sudo docker-compose run --rm --entrypoint "/bin/sh" pig-processing -c "rm -rf /etl/agg && mkdir -p /etl/agg && pig -x local /scripts/processing.pig"

# Ejecutar filtrado y homogeneización
sudo docker-compose run --rm --entrypoint "/bin/sh" pig-processing -c "pig -x local /scripts/filter_homogenize.pig"

# Sincronizar datos con Elasticsearch
./scripts/sync_to_elastic.sh

# Verificar agregaciones generadas
ls -la etl/agg/
ls etl/agg/by_comuna/ | wc -l    # Contar comunas
ls etl/agg/by_type/              # Ver tipos de incidentes
ls etl/agg/temporal/by_hour/     # Ver distribución temporal

# Verificar datos limpios
ls -la etl/clean/
ls -la etl/fail/
```

### API REST - Ejemplos de Uso Avanzados

```bash
# Consultar estadísticas del sistema (nuevo endpoint)
curl -s http://localhost:8000/api/v1/health | python -m json.tool

# Consulta avanzada por tipo de incidente
curl -X POST -H "Content-Type: application/json" \
     -d '{"query_type":"INCIDENTS","filters":{"incident_type":"traffic_jam"}}' \
     http://localhost:8000/api/v1/query

# Configurar caché dinámicamente
curl -X POST -H "Content-Type: application/json" \
     -d '{"policy":"LFU","max_size":2000,"default_ttl":1800}' \
     http://localhost:8000/api/v1/cache/configure

# Consulta de compatibilidad (formato anterior)
curl http://localhost:8000/query?id=waze_alert-123456789

# Estadísticas en formato anterior
curl http://localhost:8000/stats
```

### Análisis de Datos Avanzado

```bash
# Análisis completo del sistema
./analyze_events.sh

# Análisis de comuna específica (insensible a mayúsculas)
./analyze_events.sh -c "santiago"
./analyze_events.sh --comuna "LAS CONDES"
./analyze_events.sh -c "padre hurtado"

# Listar todas las comunas disponibles
./analyze_events.sh --list

# Ver ayuda completa
./analyze_events.sh --help

# Comandos de verificación rápida
cat etl/agg/temporal/peak_hours/part-* | head -10  # Top horas
find etl/agg/by_comuna -name "*" | wc -l          # Contar archivos por comuna
cat etl/agg/by_type/traffic_jam/part-* | wc -l    # Contar congestiones
```

### Monitoreo de Logstash

```bash
# Ver logs de Logstash
docker-compose logs -f logstash

# Verificar índices creados por Logstash
curl http://localhost:9200/_cat/indices?v

# Ver documentos indexados
curl http://localhost:9200/traffic-events-csv-*/_count
curl http://localhost:9200/traffic-events-raw-*/_count

# Forzar resincronización
./scripts/sync_to_elastic.sh
```

### Detener la Plataforma

```bash
# Detener todos los servicios (mantiene datos)
docker-compose down

# Detener y eliminar volúmenes (elimina todos los datos)
docker-compose down -v

# Detener servicios específicos
docker-compose stop scraper cache logstash
```

## Interfaces Web

### Interfaces de Administración

- **MongoDB Express**: [http://localhost:8081](http://localhost:8081)
  - **Usuario**: `admin`
  - **Contraseña**: `pass123`
  - **Funciones**: Ver y administrar contenidos de la base de datos
  - **Uso**: Explorar eventos almacenados, ver estadísticas de colecciones

- **Elasticsearch**: [http://localhost:9200](http://localhost:9200)
  - **Función**: Motor de búsqueda y análisis
  - **Uso**: Consultas avanzadas y agregaciones
  - **Índices**: `traffic-events-csv-*`, `traffic-events-raw-*`

### Servicios de Backend

- **API REST FastAPI**: [http://localhost:8000](http://localhost:8000)
  - **Documentación interactiva**: [http://localhost:8000/docs](http://localhost:8000/docs)
  - **Redoc**: [http://localhost:8000/redoc](http://localhost:8000/redoc)
  - **Endpoints**: `/api/v1/query`, `/api/v1/health`, `/api/v1/cache/configure`
  - **Compatibilidad**: `/stats`, `/query`

- **Healthcheck Endpoint**: [http://localhost:8000/api/v1/health](http://localhost:8000/api/v1/health)
  - **Estado de servicios**: MongoDB, Redis, Elasticsearch
  - **Métricas de latencia**: Tiempo de respuesta de cada servicio
  - **Disponibilidad**: Porcentaje de uptime

## Monitoreo

### Estadísticas del Sistema de Caché Avanzado

```bash
# Obtener métricas completas del nuevo sistema
curl -s http://localhost:8000/api/v1/health | python -m json.tool

# Estadísticas de compatibilidad
curl -s http://localhost:8000/stats | python -m json.tool

# Configurar caché dinámicamente
curl -X POST -H "Content-Type: application/json" \
     -d '{"policy":"LFU","max_size":1500}' \
     http://localhost:8000/api/v1/cache/configure
```

### Monitoreo de Pipelines Logstash

```bash
# Estado de Logstash
docker-compose ps logstash

# Logs de Logstash con filtrado
docker-compose logs logstash | grep -E "(ERROR|WARN)"

# Verificar índices de Elasticsearch
curl http://localhost:9200/_cat/indices?v | grep traffic-events

# Contar documentos por índice
curl http://localhost:9200/traffic-events-*/_count

# Ver mapping de índices
curl http://localhost:9200/traffic-events-csv-*/_mapping
curl http://localhost:9200/traffic-events-raw-*/_mapping

# Verificar sincronización
ls -la /var/log/logstash_schedule.log 2>/dev/null || echo "Archivo de log no existe"
```

### Estadísticas ETL y Agregaciones

```bash
# Análisis completo automatizado
./analyze_events.sh

# Análisis específico por comuna
./analyze_events.sh -c Santiago

# Verificar integridad de agregaciones
find etl/agg -name "*.txt" -o -name "part-*" | wc -l

# Ver eventos de la hora pico
hora_pico=$(ls etl/agg/temporal/peak_hours_analysis/)
cat etl/agg/temporal/peak_hours_analysis/$hora_pico/part-*

# Verificar datos limpios vs sucios
echo "Eventos limpios:" && find etl/clean -name "part-*" | wc -l
echo "Eventos fallidos:" && find etl/fail -name "part-*" 2>/dev/null | wc -l

# Estadísticas por tipo de incidente
for tipo in traffic_jam hazard police accident road_closed; do
  echo "=== $tipo ==="
  cat etl/agg/by_type/$tipo/part-* 2>/dev/null | wc -l
done

# Top 5 comunas por número de eventos
for comuna in etl/agg/by_comuna/*; do
  nombre=$(basename "$comuna")
  eventos=$(cat "$comuna"/* 2>/dev/null | wc -l)
  echo "$eventos $nombre"
done | sort -nr | head -5
```

### Logs de Servicios

```bash
# Ver logs del nuevo sistema de caché FastAPI
docker-compose logs -f cache

# Ver logs del scraper con distribución mejorada
docker-compose logs -f scraper

# Ver logs de Logstash con pipelines duales
docker-compose logs -f logstash

# Ver logs del procesamiento ETL
docker-compose logs pig-processing

# Ver logs de MongoDB
docker-compose logs -f mongodb

# Ver logs de Elasticsearch
docker-compose logs -f elasticsearch

# Ver todos los logs con timestamps
docker-compose logs -f -t

# Ver solo errores de todos los servicios
docker-compose logs | grep -E "(ERROR|error|Error)"
```

### Monitoreo de Recursos

```bash
# Estado de contenedores
docker-compose ps

# Uso de recursos por contenedor
docker stats

# Espacio en disco utilizado
du -sh etl/
du -sh data/
du -sh logstash/

# Verificar conectividad de servicios nuevos
docker-compose exec cache curl -s http://localhost:8000/api/v1/health
docker-compose exec mongodb mongo --eval "db.runCommand('ping')"
docker-compose exec elasticsearch curl -s http://localhost:9200/_cluster/health

# Verificar pipelines de Logstash
curl -s http://localhost:9200/_cat/indices?v | grep traffic-events
```

## Características Avanzadas

### Detección Automática de Patrones

- **Horas Pico Dinámicas**: 
  - Análisis de las 24 horas del día
  - Detección automática de la hora con mayor actividad
  - Creación dinámica de carpetas `hora_peek_XXh`
  - Actualización automática cuando cambian los patrones

- **Análisis Geográfico Inteligente**: 
  - Identificación de comunas con mayor concentración de incidentes
  - Mapeo automático de coordenadas a comunas RM
  - Análisis de distribución espacial de eventos
  - Cuadrantes geográficos para mejor cobertura

- **Clasificación Automática de Incidentes**: 
  - Distribución estadística por tipos de eventos
  - Análisis de tendencias temporales por tipo
  - Correlación entre tipos de incidentes y ubicaciones

### Nuevo Sistema de Caché Inteligente

- **Caché Multinivel**: 
  - Redis como caché persistente distribuido
  - Caché en memoria para consultas frecuentes
  - Fallback inteligente: Cache → MongoDB → Elasticsearch

- **Políticas Adaptativas**: 
  - LRU y LFU con métricas en tiempo real
  - Cambio dinámico basado en patrones de uso
  - TTL configurable por tipo de consulta

- **Integración con Elasticsearch**: 
  - Respaldo automático para cache miss
  - Consultas complejas con agregaciones
  - Indexación automática desde múltiples fuentes

### Pipelines de Datos Duales

- **Pipeline CSV**: 
  - Procesamiento directo de archivos ETL
  - Validación y limpieza en tiempo real
  - Detección de horas pico personalizable

- **Pipeline MongoDB**: 
  - Sincronización en tiempo real desde base de datos
  - Manejo de cambios incrementales
  - Preservación de metadatos originales

- **Sincronización Automatizada**: 
  - Scripts de resincronización forzada
  - Limpieza de estados para reindexación completa
  - Logging detallado de operaciones

### API REST Robusta con FastAPI

- **Documentación Automática**: 
  - OpenAPI/Swagger en `/docs`
  - Redoc en `/redoc`
  - Validación automática de esquemas

- **Endpoints Avanzados**: 
  - Consultas tipadas (INCIDENTS, TRAFFIC_FLOW, AGGREGATED)
  - Health checks con métricas de servicios
  - Configuración dinámica de parámetros

- **Compatibilidad Backward**: 
  - Endpoints legacy para sistema anterior
  - Migración sin interrupciones
  - Respuestas en formatos originales

### Análisis Flexible y Potente

- **Consultas por Comuna Específica**: 
  - Análisis detallado con estadísticas temporales y por tipo
  - Comparación con promedios del sistema
  - Identificación de patrones únicos por comuna

- **Búsqueda Inteligente**: 
  - Búsqueda insensible a mayúsculas/minúsculas
  - Manejo de espacios y caracteres especiales
  - Sugerencias automáticas para nombres similares

- **Múltiples Formatos de Salida**: 
  - Análisis completo con dashboard ejecutivo
  - Análisis por comuna individual
  - Listado alfabético de comunas
  - Ayuda integrada con ejemplos

### Escalabilidad y Rendimiento

- **Procesamiento Distribuido**: 
  - Apache Pig con scripts modulares
  - Separación de limpieza y agregaciones
  - Paralelización automática de operaciones ETL

- **Indexación Avanzada**: 
  - Elasticsearch con múltiples índices
  - Particionamiento temporal automático
  - Optimización de consultas complejas

- **Sistema de Caché Optimizado**: 
  - Múltiples políticas de expulsión
  - Métricas detalladas de rendimiento
  - Ajuste dinámico basado en patrones de uso

### Flexibilidad de Configuración

- **Intervalos de Sondeo Configurables**: 
  - Ajuste dinámico de frecuencia del scraper
  - Balance entre freshness y carga del sistema
  - Randomización para evitar patrones predecibles

- **Políticas de Caché Intercambiables**: 
  - Cambio en tiempo real entre LRU y LFU
  - Configuración de TTL por tipo de consulta
  - Ajuste automático de tamaño basado en memoria

- **Pipelines Logstash Modulares**: 
  - Configuraciones independientes por fuente de datos
  - Filtros personalizables y extensibles
  - Output hacia múltiples destinos

- **Pipeline ETL Modificable**: 
  - Scripts Pig editables para nuevas agregaciones
  - Filtros y transformaciones personalizables
  - Extensibilidad para nuevos tipos de análisis

### Robustez y Confiabilidad

- **Manejo de Fallos Mejorado**: 
  - Reinicio automático de servicios
  - Recuperación de estado desde persistencia
  - Validación de integridad de datos en múltiples niveles

- **Monitoreo Integrado Avanzado**: 
  - Health checks automáticos de todos los servicios
  - Métricas operativas en tiempo real
  - Logging estructurado con niveles de severidad

- **Deduplicación y Limpieza**: 
  - Prevención de eventos duplicados por UUID
  - Separación automática de datos válidos/inválidos
  - Limpieza automática de datos obsoletos

---

**Tecnologías**: Docker, Apache Pig, MongoDB, Redis, Python, FastAPI, Elasticsearch, Logstash, Bash, API Waze


**Versión**: 3.0.0 (Tarea 3)  
**Autor**: Maximiliano Juárez  