import os
import json
import time
import logging
from pymongo import MongoClient
import datetime
import shutil
import csv

# Configuración de logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('data_loader')

def get_mongo_client():
    """Establece conexión con MongoDB con reintentos"""
    max_retries = 10
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            client = MongoClient('mongodb://mongodb:27017/')
            # Verificar la conexión
            client.admin.command('ping')
            logger.info("Conectado exitosamente a MongoDB")
            return client
        except Exception as e:
            retry_count += 1
            wait_time = retry_count * 5  
            logger.error(f"Error conectando a MongoDB (intento {retry_count}/{max_retries}): {e}")
            logger.info(f"Reintentando en {wait_time} segundos...")
            time.sleep(wait_time)
    
    logger.critical("Falló la conexión a MongoDB después de múltiples intentos")
    raise Exception("No se pudo establecer conexión con MongoDB")

def initialize_db(client):
    """Inicializa la base de datos y colecciones necesarias"""
    db = client['traffic_db']
    collection = db['traffic_events']
    
    # Crear índices para optimizar consultas
    collection.create_index("uuid", unique=True)
    collection.create_index("timestamp")
    collection.create_index("location_desc")
    collection.create_index("type")
    
    logger.info("Base de datos y colecciones inicializadas correctamente")
    return db, collection

def process_file(filepath, collection):
    """Procesa un archivo JSON y carga sus eventos en MongoDB"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            events = json.load(f)
            
        if not events:
            logger.warning(f"Archivo vacío o con formato incorrecto: {filepath}")
            return 0
        
        # Añadir timestamp de procesamiento
        processed_events = []
        for event in events:
            event['processed_at'] = datetime.datetime.now().isoformat()
            processed_events.append(event)
        
        # Contador para eventos procesados correctamente
        successful_events = 0
        
        # Procesar eventos uno por uno para manejar duplicados
        for event in processed_events:
            try:
                # Intentar upsert (actualizar si existe, insertar si no)
                result = collection.update_one(
                    {"uuid": event["uuid"]},  
                    {"$set": event},          
                    upsert=True             
                )
                successful_events += 1
            except Exception as e:
                logger.error(f"Error procesando evento: {e}")
        
        # Mover archivo a carpeta de procesados
        processed_dir = os.path.join(os.path.dirname(filepath), "processed")
        os.makedirs(processed_dir, exist_ok=True)
        
        processed_filepath = os.path.join(
            processed_dir, 
            os.path.basename(filepath)
        )
        
        shutil.move(filepath, processed_filepath)
        logger.info(f"Archivo movido a {processed_filepath}")
        
        logger.info(f"Total de {successful_events} eventos insertados o actualizados en este ciclo")
        return successful_events
        
    except json.JSONDecodeError:
        logger.error(f"Error decodificando JSON en {filepath}")
        return 0
    except Exception as e:
        logger.error(f"Error procesando archivo {filepath}: {e}")
        return 0

def check_event_count(collection):
    """Verifica el número total de eventos en la base de datos"""
    count = collection.count_documents({})
    logger.info(f"Total de eventos almacenados: {count}")
    return count

def export_to_csv(events_data, csv_path="/etl/events_raw.csv"):
    """Exporta eventos a CSV en el formato requerido por Pig"""
    try:
        # Asegurar que el directorio existe
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        
        # DEBUG: Verificar que los eventos tengan timestamps
        logger.info(f"DEBUG: Exportando {len(events_data)} eventos a CSV")
        
        # Verificar los primeros 3 eventos
        for i, event in enumerate(events_data[:3]):
            logger.info(f"DEBUG Evento {i+1}:")
            logger.info(f"  UUID: {event.get('uuid', 'MISSING')}")
            logger.info(f"  Timestamp: {event.get('timestamp', 'MISSING')}")
            logger.info(f"  Location_desc: {event.get('location_desc', 'MISSING')}")
            logger.info(f"  Description: {event.get('description', 'MISSING')}")
        
        # Definir los campos en el orden EXACTO esperado por el script Pig
        fields = [
            "uuid", "type", "location", "location_desc", "description", 
            "timestamp", "source", "length_meters", "speed", 
            "congestion_level", "delay_seconds", "waze_id"
        ]
        
        # Abrir archivo para escritura
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
            
            # Escribir encabezados
            writer.writeheader()
            
            # Escribir datos con validación de timestamp
            valid_events = 0
            for event in events_data:
                # VERIFICACIÓN CRÍTICA: Asegurar que timestamp no esté vacío
                if not event.get('timestamp'):
                    logger.warning(f"Evento {event.get('uuid', 'unknown')} sin timestamp, asignando timestamp actual")
                    event['timestamp'] = datetime.datetime.now().isoformat()
                
                # Asegurar que cada evento tiene todos los campos necesarios
                row = {}
                for field in fields:
                    value = event.get(field, '')
                    # Limpiar valores None o vacíos problemáticos
                    if value is None:
                        value = ''
                    elif field == 'location' and isinstance(value, str) and ',' not in value:
                        # Si location no tiene formato "lat,lon", usar coordenadas por defecto
                        value = '-33.4489,-70.6693'  # Santiago Centro por defecto
                    row[field] = str(value)
                
                writer.writerow(row)
                valid_events += 1
                
        logger.info(f"✅ {valid_events} eventos exportados a CSV: {csv_path}")
        
        # DEBUG: Verificar que el CSV se escribió correctamente
        with open(csv_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            logger.info(f"DEBUG: CSV tiene {len(lines)} líneas (incluyendo header)")
            if len(lines) > 1:
                # Mostrar la primera línea de datos
                first_data_line = lines[1].strip()
                logger.info(f"DEBUG: Primera línea de datos: {first_data_line}")
        
        return True
    except Exception as e:
        logger.error(f"❌ Error exportando a CSV: {e}")
        return False

def main():
    """Función principal del cargador de datos"""
    logger.info("Iniciando servicio de carga de datos")
    
    # Esperar a que MongoDB esté disponible
    time.sleep(10)
    
    # Inicializar conexión y base de datos
    client = get_mongo_client()
    db, collection = initialize_db(client)
    
    while True:
        try:
            # Buscar archivos JSON nuevos en el directorio de datos
            data_dir = "/data"
            files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) 
                    if f.endswith('.json') and os.path.isfile(os.path.join(data_dir, f))
                    and not f.startswith('.')]
            
            if files:
                logger.info(f"Encontrados {len(files)} archivos para procesar")
                total_inserted = 0
                
                for filepath in files:
                    inserted = process_file(filepath, collection)
                    total_inserted += inserted
                
                if total_inserted > 0:
                    logger.info(f"Total de {total_inserted} eventos insertados en este ciclo")
                    
                    # EXPORTAR TODOS LOS EVENTOS A CSV DESPUÉS DE CADA INSERCIÓN
                    logger.info("Exportando todos los eventos a CSV...")
                    all_events = list(collection.find({}, {'_id': 0}))
                    
                    if all_events:
                        export_to_csv(all_events)
                        logger.info(f"CSV actualizado con {len(all_events)} eventos totales")
                    else:
                        logger.warning("No hay eventos en la base de datos para exportar")
                    
                    # Verificar cantidad total de eventos
                    current_count = check_event_count(collection)
                    
                    # Si no hay suficientes eventos, podríamos generar alertas
                    if current_count < 1000:
                        logger.warning(f"Solo hay {current_count} eventos en la base de datos")
            else:
                logger.info("No se encontraron nuevos archivos para procesar")
                
                # Aún así, exportar los datos existentes cada minuto para mantener CSV actualizado
                all_events = list(collection.find({}, {'_id': 0}))
                if all_events:
                    export_to_csv(all_events)
                    logger.info(f"CSV mantenido actualizado con {len(all_events)} eventos")
                
            # Esperar antes del próximo ciclo
            time.sleep(60)
            
        except Exception as e:
            logger.error(f"Error en ciclo principal: {e}")
            time.sleep(30)

if __name__ == "__main__":
    main()