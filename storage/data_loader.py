import os
import json
import time
import logging
from pymongo import MongoClient
import datetime
import shutil

# Configuración de logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('data_loader')

# Conexión a MongoDB
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
    collection.create_index('timestamp')
    collection.create_index('type')
    collection.create_index('location_desc')
    collection.create_index('uuid', unique=True) 
    
    logger.info("Base de datos e índices inicializados")
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
                    
                # Verificar cantidad total de eventos
                current_count = check_event_count(collection)
                
                # Si no hay suficientes eventos, podríamos generar alertas o incrementar la frecuencia de scraping
                if current_count < 1000:
                    logger.warning(f"Solo hay {current_count} eventos en la base de datos, se necesitan al menos 10,000")
            else:
                logger.info("No se encontraron nuevos archivos para procesar")
                
            # Esperar antes del próximo ciclo
            time.sleep(60)
            
        except Exception as e:
            logger.error(f"Error en ciclo principal: {e}")
            time.sleep(30)

if __name__ == "__main__":
    main()