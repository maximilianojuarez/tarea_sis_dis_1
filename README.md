
Una plataforma distribuida para el análisis de tráfico en tiempo real en la Región Metropolitana, utilizando datos reales de la API de Waze para proporcionar información sobre patrones de tráfico.

## Tabla de Contenidos

- [Arquitectura](#arquitectura)
- [Componentes](#componentes)
- [Estructura de Datos](#estructura-de-datos)
- [Distribuciones de Tráfico](#distribuciones-de-tráfico)
- [Requisitos](#requisitos)
- [Instalación](#instalación)
- [Uso](#uso)
- [Interfaces Web](#interfaces-web)
- [Monitoreo](#monitoreo)
- [Desarrollo](#desarrollo)

## Arquitectura

El sistema sigue una arquitectura de flujo de datos modular:

1. **Recolección de Datos**: El scraper extrae datos de tráfico en tiempo real de la API de Waze
2. **Almacenamiento**: Los datos se almacenan en MongoDB con indexación UUID para una recuperación eficiente
3. **Simulación de Consultas**: El generador de tráfico produce patrones de consulta estadísticos
4. **Caché**: El caché basado en Redis optimiza las consultas frecuentes con políticas configurables


## Componentes

### Scraper

- Ubicado en [scraper/scraper.py](scraper/scraper.py)
- Extrae incidentes de tráfico de la API de Waze (accidentes, congestión, peligros)
- Intervalos de sondeo configurables
- Formato de salida JSON

### Sistema de Almacenamiento

- Ubicado en [storage/data_loader.py](storage/data_loader.py)
- Almacenamiento persistente basado en MongoDB
- Indexación UUID para recuperación eficiente
- Indexación geoespacial para consultas basadas en ubicación

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

## Estructura de Datos

Los eventos de tráfico siguen esta estructura:

```json
{
  "uuid": "waze_123456789",
  "type": "traffic_jam",
  "location": "-33.4489,-70.6693",
  "location_desc": "Santiago Centro",
  "description": "Congestión en Alameda",
  "timestamp": "2025-04-21T21:00:29",
  "source": "waze_api",
  "severity": 3,
  "estimated_duration": 15
}
```

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

## Requisitos

- Docker y Docker Compose
- Python 3.8 o superior 
- Conexión a Internet para acceso a la API de Waze

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

### Logs de Servicios

```bash
# Ver logs del caché
docker-compose logs -f cache

# Ver logs del generador de tráfico
docker-compose logs -f traffic-generator

# Ver todos los logs
docker-compose logs -f
```
