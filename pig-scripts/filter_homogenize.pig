-- Script de filtrado y homogeneización de datos de tráfico

-- Cargar datos desde CSV
raw_events = LOAD '/etl/events_raw.csv' USING PigStorage(',') AS 
    (uuid:chararray, type:chararray, location:chararray, 
     location_desc:chararray, description:chararray, timestamp:chararray, 
     source:chararray, length_meters:int, speed:double, congestion_level:int, 
     delay_seconds:int, waze_id:chararray);

-- SPLIT en base a validez (solo verificamos que tenga uuid)
SPLIT raw_events INTO 
    valid_events IF uuid IS NOT NULL, 
    invalid_events OTHERWISE;

-- Guardar datos inválidos en carpeta fail
STORE invalid_events INTO '/etl/fail' USING PigStorage(',');

-- Para los datos válidos, extraer fecha del timestamp cuando existe
with_date = FOREACH valid_events GENERATE
    uuid, 
    (type IS NULL ? 'unknown' : type) AS type,
    location,
    location_desc,
    description,
    timestamp,
    source,
    length_meters,
    speed,
    congestion_level,
    delay_seconds,
    waze_id,
    (timestamp IS NOT NULL ? SUBSTRING(timestamp, 0, 10) : 'unknown_date') AS date:chararray;

-- Agrupar eventos similares, pero sin filtrar (para no perder datos)
grouped_events = GROUP with_date BY (uuid, type, location);

-- Homogeneizar datos manteniendo el más reciente de cada grupo
homogenized_events = FOREACH grouped_events {
    -- Crear un campo temporal para ordenación
    with_sort_key = FOREACH with_date GENERATE *,
        (timestamp IS NULL ? '' : timestamp) AS sort_key;
    -- Ordenar por ese campo
    sorted = ORDER with_sort_key BY sort_key DESC;
    -- Tomar el primero (más reciente)
    limited = LIMIT sorted 1;
    GENERATE FLATTEN(limited);
};

-- Seleccionar y reorganizar columnas finales
final_events = FOREACH homogenized_events GENERATE 
    uuid AS id,
    type AS incident_type,
    location AS coordinates,
    location_desc AS comuna,
    description,
    timestamp,
    source,
    length_meters,
    speed,
    congestion_level,
    delay_seconds,
    waze_id;

rmf /etl/fail;
rmf /etl/clean;

-- Guardar resultados procesados en formato CSV
STORE final_events INTO '/etl/clean' USING PigStorage(',');