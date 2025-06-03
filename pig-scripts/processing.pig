-- filepath: /home/max/Escritorio/tarea_sis_dis_1/pig-scripts/processing.pig
REGISTER '/pig/piggybank.jar';

-- CARGAR COMO LÍNEAS DE TEXTO
raw_lines = LOAD '/etl/events_raw.csv' USING TextLoader();

-- Filtrar header
data_lines = FILTER raw_lines BY $0 != 'uuid,type,location,location_desc,description,timestamp,source,length_meters,speed,congestion_level,delay_seconds,waze_id';

-- Extraer timestamp usando REGEX
events_with_timestamp = FOREACH data_lines GENERATE
    REGEX_EXTRACT($0, '^([^,]+)', 1) AS uuid,
    REGEX_EXTRACT($0, '(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d+)', 1) AS timestamp;

-- Filtrar eventos válidos
events_valid = FILTER events_with_timestamp BY timestamp IS NOT NULL AND SIZE(timestamp) > 19;

-- Extraer hora DIRECTAMENTE DEL TIMESTAMP
events_with_hour = FOREACH events_valid GENERATE
    uuid,
    timestamp,
    REGEX_EXTRACT(timestamp, 'T(\\d{2}):', 1) AS hour;

-- Filtrar eventos con hora válida
events_hour_valid = FILTER events_with_hour BY hour IS NOT NULL AND SIZE(hour) == 2;

-- BY_HOUR (MANTENER IGUAL)
events_for_hour = FOREACH events_hour_valid GENERATE
    hour AS partition_key,
    uuid, timestamp;

STORE events_for_hour INTO '/etl/agg/temporal/by_hour' 
USING org.apache.pig.piggybank.storage.MultiStorage('/etl/agg/temporal/by_hour', '0', 'none');

-- PEAK_HOURS ESTADÍSTICAS (MANTENER IGUAL)
hour_group = GROUP events_hour_valid BY hour;
peak_hours = FOREACH hour_group GENERATE 
    group AS hora,
    COUNT(events_hour_valid) AS total;

peak_hours_sorted = ORDER peak_hours BY total DESC;
STORE peak_hours_sorted INTO '/etl/agg/temporal/peak_hours' USING PigStorage('\t');

-- ========================================
-- AGREGACIONES CON TIPO DE INCIDENTE
-- ========================================

-- BY_COMUNA (AGREGANDO TIPO)
events_with_comuna = FOREACH data_lines GENERATE
    REGEX_EXTRACT($0, '^([^,]+)', 1) AS uuid,
    REGEX_EXTRACT($0, '(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d+)', 1) AS timestamp,
    REGEX_EXTRACT($0, '[^,]+,([^,]+)', 1) AS type,
    REGEX_EXTRACT($0, '[^,]+,[^,]+,"[^"]+",([^,]+)', 1) AS comuna;

events_comuna_valid = FILTER events_with_comuna BY comuna IS NOT NULL AND timestamp IS NOT NULL;

events_for_comuna = FOREACH events_comuna_valid GENERATE
    comuna AS partition_key,
    type, uuid, timestamp;

STORE events_for_comuna INTO '/etl/agg/by_comuna' 
USING org.apache.pig.piggybank.storage.MultiStorage('/etl/agg/by_comuna', '0', 'none');

-- BY_TYPE (AGREGANDO COMUNA Y MÁS INFO)
events_with_type = FOREACH data_lines GENERATE
    REGEX_EXTRACT($0, '^([^,]+)', 1) AS uuid,
    REGEX_EXTRACT($0, '(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d+)', 1) AS timestamp,
    REGEX_EXTRACT($0, '[^,]+,([^,]+)', 1) AS type,
    REGEX_EXTRACT($0, '[^,]+,[^,]+,"[^"]+",([^,]+)', 1) AS comuna;

events_type_valid = FILTER events_with_type BY type IS NOT NULL AND timestamp IS NOT NULL;

events_for_type = FOREACH events_type_valid GENERATE
    type AS partition_key,
    type, uuid, timestamp, comuna;

STORE events_for_type INTO '/etl/agg/by_type' 
USING org.apache.pig.piggybank.storage.MultiStorage('/etl/agg/by_type', '0', 'none');

-- ========================================
-- PEAK_HOURS DETALLADO CON TIPO
-- ========================================

-- Extraer información completa para peak_hours detallado
events_complete = FOREACH data_lines GENERATE
    REGEX_EXTRACT($0, '^([^,]+)', 1) AS uuid,
    REGEX_EXTRACT($0, '(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d+)', 1) AS timestamp,
    REGEX_EXTRACT($0, '[^,]+,([^,]+)', 1) AS type,
    REGEX_EXTRACT($0, '[^,]+,[^,]+,"[^"]+",([^,]+)', 1) AS comuna,
    REGEX_EXTRACT($0, '[^,]+,[^,]+,[^,]+,[^,]+,([^,]+),', 1) AS description;

-- Filtrar eventos válidos con timestamp
events_complete_valid = FILTER events_complete BY timestamp IS NOT NULL AND SIZE(timestamp) > 19;

-- Extraer hora
events_complete_with_hour = FOREACH events_complete_valid GENERATE
    uuid,
    timestamp,
    type,
    comuna,
    description,
    REGEX_EXTRACT(timestamp, 'T(\\d{2}):', 1) AS hour;

-- Filtrar solo eventos con hora válida
events_complete_hour_valid = FILTER events_complete_with_hour BY hour IS NOT NULL AND SIZE(hour) == 2;

-- DETECTAR DINÁMICAMENTE LA HORA PICO
peak_hour_first = LIMIT peak_hours_sorted 1;
peak_hour_only = FOREACH peak_hour_first GENERATE $0 AS peak_hour;

-- JOIN para obtener eventos de la hora pico
events_with_peak_hour = JOIN events_complete_hour_valid BY hour, peak_hour_only BY peak_hour;

-- Crear análisis específico de la hora pico con TIPO DE INCIDENTE
events_peak_analysis = FOREACH events_with_peak_hour GENERATE
    CONCAT('hora_peek_', events_complete_hour_valid::hour, 'h') AS categoria,
    events_complete_hour_valid::type AS incident_type,
    events_complete_hour_valid::uuid AS uuid,
    events_complete_hour_valid::timestamp AS timestamp,
    events_complete_hour_valid::comuna AS comuna,
    events_complete_hour_valid::description AS description,
    events_complete_hour_valid::hour AS hour;

-- Guardar análisis de hora pico con información completa
events_peak_for_storage = FOREACH events_peak_analysis GENERATE
    categoria AS partition_key,
    incident_type,
    uuid,
    timestamp,
    comuna,
    description,
    hour;

STORE events_peak_for_storage INTO '/etl/agg/temporal/peak_hours_analysis' 
USING org.apache.pig.piggybank.storage.MultiStorage('/etl/agg/temporal/peak_hours_analysis', '0', 'none');