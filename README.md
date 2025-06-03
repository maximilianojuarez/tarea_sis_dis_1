
```markdown
# Sistema ETL de Análisis de Tráfico en Tiempo Real

Una plataforma distribuida para el análisis de tráfico en tiempo real en la Región Metropolitana, utilizando datos reales de la API de Waze para proporcionar información sobre patrones de tráfico, con capacidades de procesamiento ETL distribuido y análisis de datos avanzado.

## Tabla de Contenidos

- [Arquitectura](#arquitectura)
- [Componentes](#componentes)
- [Pipeline ETL](#pipeline-etl)
- [Estructura de Datos](#estructura-de-datos)
- [Distribuciones de Tráfico](#distribuciones-de-tráfico)
- [Análisis de Datos](#análisis-de-datos)
- [Requisitos](#requisitos)
- [Instalación](#instalación)
- [Uso](#uso)
- [Interfaces Web](#interfaces-web)
- [Monitoreo](#monitoreo)

## Arquitectura

El sistema sigue una arquitectura de flujo de datos modular con pipeline ETL distribuido:

1. **Recolección de Datos**: El scraper extrae datos de tráfico en tiempo real de la API de Waze
2. **Almacenamiento**: Los datos se almacenan en MongoDB con indexación UUID para una recuperación eficiente
3. **Pipeline ETL**: Procesamiento distribuido con Apache Pig para agregaciones y análisis
4. **Simulación de Consultas**: El generador de tráfico produce patrones de consulta estadísticos
5. **Caché**: El caché basado en Redis optimiza las consultas frecuentes con políticas configurables
6. **Análisis**: Sistema de estadísticas automatizado para insights de tráfico

## Componentes

### Scraper

- Ubicado en [scraper/scraper.py](scraper/scraper.py)
- Extrae incidentes de tráfico de la API de Waze (accidentes, congestión, peligros)
- Intervalos de sondeo configurables
- Formato de salida JSON
- Mapeo automático de coordenadas a comunas de la RM

### Sistema de Almacenamiento

- Ubicado en [storage/data_loader.py](storage/data_loader.py)
- Almacenamiento persistente basado en MongoDB
- Indexación UUID para recuperación eficiente
- Indexación geoespacial para consultas basadas en ubicación
- Exportación automática a CSV para procesamiento ETL

### Pipeline ETL Distribuido

- Ubicado en [pig-scripts/processing.pig](pig-scripts/processing.pig)
- Procesamiento con Apache Pig en contenedor Docker
- Agregaciones múltiples:
  - **Por hora**: Distribución temporal de incidentes
  - **Por comuna**: Análisis geográfico de la RM
  - **Por tipo**: Clasificación de tipos de incidentes
  - **Hora pico dinámica**: Detección automática de la hora con más actividad

### Generador de Tráfico

- Ubicado en [traffic-generator/generator.py](traffic-generator/generator.py)
- Simula patrones de consulta utilizando distribuciones estadísticas:
  - Distribución de Poisson para tráfico regular
  - Distribución Normal para picos de tráfico
- Parámetros configurables para la intensidad del tráfico

### Sistema de Caché

- Ubicado en [cache/app.py](cache/app.py)
- Capa de caché basada en Redis
- Múltiples políticas de expulsión (LRU, LFU)
- Dimensionamiento adaptativo del caché basado en proporciones de aciertos/fallos

### Sistema de Análisis

- Ubicado en [analyze_events.sh](analyze_events.sh)
- Análisis automatizado de estadísticas ETL
- Reportes detallados de patrones de tráfico
- Detección de horas pico y comunas críticas
- **Análisis por comuna específica**
- **Listado de todas las comunas disponibles**

## Pipeline ETL

### Procesamiento de Datos

El pipeline ETL procesa los datos en las siguientes etapas:

1. **Extracción**: Carga datos desde `etl/events_raw.csv`
2. **Transformación**: 
   - Filtrado de eventos válidos
   - Extracción de patrones temporales
   - Normalización de datos geográficos
   - Clasificación de tipos de incidentes
3. **Carga**: Generación de agregaciones en directorios organizados

### Agregaciones Generadas


etl/agg/
├── temporal/
│   ├── by_hour/           # Eventos agrupados por hora (00, 01, 02, ...)
│   ├── peak_hours/        # Ranking de horas por volumen de incidentes
│   └── peak_hours_analysis/
│       └── hora_peek_XXh/ # Análisis detallado de la hora pico detectada
├── by_comuna/             # Eventos agrupados por comuna de la RM
└── by_type/               # Eventos agrupados por tipo de incidente


### Detección Automática de Hora Pico

El sistema detecta dinámicamente la hora con mayor actividad y crea una carpeta específica:
- **Ejemplo**: Si las 22:00 es la hora pico → `hora_peek_22h/`
- **Adaptativo**: Si cambia a las 15:00 → `hora_peek_15h/`
- **Contenido**: Análisis detallado con tipos de incidentes y comunas afectadas

## Estructura de Datos

Los eventos de tráfico siguen esta estructura:

```json
{
  "uuid": "waze_alert-123456789/abc-def-ghi",
  "type": "traffic_jam",
  "location": "-33.4489,-70.6693",
  "location_desc": "Santiago Centro",
  "description": "Congestión en Alameda",
  "timestamp": "2025-06-01T21:00:29.123456",
  "source": "waze_api",
  "length_meters": 500,
  "speed": 15,
  "congestion_level": 3,
  "delay_seconds": 120,
  "waze_id": "waze_alert-123456789"
}
```

### Tipos de Incidentes

- **traffic_jam** 🚗: Congestión vehicular
- **hazard** ⚠️: Peligros en la vía
- **police** 👮: Presencia policial
- **accident** 💥: Accidentes de tránsito
- **road_closed** 🚧: Cierres de calles

### Comunas Monitoreadas

El sistema cubre 36+ comunas de la Región Metropolitana:
- Santiago, Las Condes, Providencia, Vitacura
- Maipú, Puente Alto, San Bernardo, La Florida
- Colina, Melipilla, Talagante, Padre Hurtado
- Y muchas más...

## Distribuciones de Tráfico

El sistema simula diferentes patrones de consulta:

- **Distribución de Poisson (λ = 10/60)**
  - Simula aproximadamente 10 consultas por minuto
  - Distribución uniforme a lo largo del período de tiempo
  - Modela condiciones de tráfico regular

- **Distribución Normal (μ = 5s, σ = 2s)**
  - Simula tiempo variable entre consultas
  - Modela períodos de tráfico pico
  - Agrupa consultas en ráfagas

El sistema alterna automáticamente entre distribuciones cada 10 minutos para simular patrones de tráfico del mundo real.

## Análisis de Datos

### Script de Análisis Automatizado

El sistema incluye un script de análisis potente y flexible con múltiples modos de operación:

```bash
# Ejecutar análisis completo (modo por defecto)
./analyze_events.sh

# Analizar una comuna específica
./analyze_events.sh -c Santiago
./analyze_events.sh --comuna "Las Condes"

# Listar todas las comunas disponibles
./analyze_events.sh -l
./analyze_events.sh --list

# Ver ayuda y opciones
./analyze_events.sh -h
./analyze_events.sh --help
```

### Modos de Análisis

#### 🏙️ Análisis por Comuna Específica
Proporciona análisis detallado de una comuna individual:

- **Total de eventos** en la comuna
- **Distribución por tipos de incidentes** con porcentajes
- **Distribución temporal** por horas del día
- **Búsqueda insensible a mayúsculas/minúsculas**

Ejemplo de salida:
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
  20:00      25 eventos
  21:00     180 eventos
  22:00     200 eventos
  23:00      75 eventos
```

#### 📋 Listado de Comunas
Muestra todas las comunas disponibles con sus totales de eventos, ordenadas alfabéticamente:

```
🏘️ COMUNAS DISPONIBLES:
──────────────────────────
  Cerrillos             45 eventos
  Colina               786 eventos
  Las Condes           234 eventos
  Maipú                478 eventos
  Santiago             480 eventos
  ...
```

### Información Generada (Análisis Completo)

- **📊 Estadísticas Generales**: Total de eventos, comunas monitoreadas, tipos de incidentes
- **⏰ Ranking de Horas Pico**: Distribución temporal con emojis por período del día
- **🏙️ Top 10 Comunas**: Comunas con mayor actividad de incidentes
- **🚨 Distribución por Tipo**: Porcentajes de cada tipo de incidente
- **🎯 Hora Pico**: Análisis detallado de la hora más crítica
- **📈 Resumen Ejecutivo**: Métricas clave del sistema

### Ejemplo de Salida Completa

```
🚦 ESTADÍSTICAS SIMPLES - SISTEMA ETL TRÁFICO
==============================================
📄 Eventos en CSV: 7,973
📁 Archivos JSON: 191

⏰ RANKING HORAS PICO:
──────────────────────────
  🌆 22:00   1,790 eventos
  🌆 21:00   1,590 eventos
  🌙 23:00   1,422 eventos

📊 TOP 10 COMUNAS:
──────────────────────────
  Colina             786 eventos
  Padre Hurtado      551 eventos
  Santiago           480 eventos
  ...
──────────────────────────
  🔥 TOP 10 COMUNAS   4386 TOTAL (TOP 10)

🎯 HORA PICO:
  🎯 Carpeta: hora_peek_22h
  📋 Tipos en hora pico:
     🚗 traffic_jam     1,200 eventos
     ⚠️ hazard          300 eventos

📈 RESUMEN EJECUTIVO:
──────────────────────────
  🏘️  Comunas monitoreadas: 36
  ⏰ Horas con actividad: 6
  🚨 Tipos de incidentes: 5
```

## Requisitos

- Docker y Docker Compose
- Python 3.8 o superior 
- Conexión a Internet para acceso a la API de Waze
- Mínimo 4GB RAM para procesamiento ETL
- 2GB espacio en disco para datos agregados

## Instalación

1. Clonar el repositorio:
   ```bash
   git clone https://github.com/maximilianojuarez/tarea_sis_dis_1
   cd tarea_sis_dis_1
   ```

2. Construir e iniciar todos los servicios:
   ```bash
   docker-compose build
   docker-compose up -d
   ```

3. Verificar que todos los servicios estén funcionando:
   ```bash
   docker-compose ps
   ```

4. Dar permisos al script de análisis:
   ```bash
   chmod +x analyze_events.sh
   ```

## Uso

### Iniciar la Plataforma

```bash
# Iniciar todos los servicios
docker-compose up -d

# Iniciar servicios específicos
docker-compose up -d scraper cache

# Ver logs en tiempo real
docker-compose logs -f
```

### Pipeline ETL

```bash
# Ejecutar procesamiento ETL completo
sudo docker-compose run --rm --entrypoint "/bin/sh" pig-processing -c "rm -rf /etl/agg && mkdir -p /etl/agg && pig -x local /scripts/processing.pig"

# Verificar agregaciones generadas
ls etl/agg/by_comuna/
ls etl/agg/by_type/
ls etl/agg/temporal/by_hour/
```

### Análisis de Datos

```bash
# Ejecutar análisis completo
./analyze_events.sh

# Analizar comuna específica
./analyze_events.sh -c "Las Condes"

# Listar todas las comunas
./analyze_events.sh --list

# Comandos rápidos
ls etl/agg/by_comuna/ | wc -l  # Contar comunas
cat etl/agg/temporal/peak_hours/part-* # Ver ranking de horas
```

### Detener la Plataforma

```bash
# Detener todos los servicios
docker-compose down

# Detener y eliminar volúmenes
docker-compose down -v
```

## Interfaces Web

- **MongoDB Express**: [http://localhost:8081](http://localhost:8081)
  - Usuario: `admin`
  - Contraseña: `pass123`
  - Ver y administrar contenidos de la base de datos

- **Redis Admin**: [http://localhost:8082](http://localhost:8082)
  - Ver entradas en caché

## Monitoreo

### Estadísticas del Caché

```bash
# Obtener estadísticas del caché
curl http://localhost:5000/stats

# Cambiar política de caché
curl -X POST -H "Content-Type: application/json" -d '{"policy":"LFU"}' http://localhost:5000/policy

# Limpiar caché
curl -X DELETE http://localhost:5000/cache
```

### Estadísticas ETL

```bash
# Análisis completo del sistema
./analyze_events.sh

# Análisis de comuna específica
./analyze_events.sh -c Santiago

# Verificar estado de agregaciones
find etl/agg -name "*" -type f | wc -l

# Ver eventos por comuna específica
cat etl/agg/by_comuna/Santiago/Santiago-*
```

### Logs de Servicios

```bash
# Ver logs del caché
docker-compose logs -f cache

# Ver logs del generador de tráfico
docker-compose logs -f traffic-generator

# Ver logs del procesamiento ETL
docker-compose logs pig-processing

# Ver todos los logs
docker-compose logs -f
```

## Características Avanzadas

### Detección Automática de Patrones

- **Horas Pico Dinámicas**: El sistema detecta automáticamente la hora con mayor actividad
- **Análisis Geográfico**: Identificación de comunas con mayor concentración de incidentes
- **Clasificación de Incidentes**: Distribución estadística por tipos de eventos

### Análisis Flexible

- **Consultas por Comuna**: Análisis detallado de comunas específicas con estadísticas temporales y por tipo
- **Búsqueda Inteligente**: Búsqueda insensible a mayúsculas/minúsculas para nombres de comunas
- **Múltiples Formatos**: Análisis completo, por comuna, listado, y ayuda integrada

### Escalabilidad

- **Procesamiento Local**: Apache Pig en modo local para datasets medianos
- **Preparado para Hadoop**: Arquitectura lista para migrar a clúster Hadoop distribuido
- **Agregaciones Eficientes**: Estructura de datos optimizada para consultas rápidas

### Flexibilidad de Configuración

- **Intervalos de Sondeo**: Configurables en el scraper
- **Políticas de Caché**: LRU, LFU intercambiables
- **Distribuciones de Tráfico**: Poisson y Normal configurables
- **Análisis Personalizable**: Script de análisis modificable con múltiples modos

---


**Tecnologías**: Docker, Apache Pig, MongoDB, Redis, Python, Bash, API Waze

