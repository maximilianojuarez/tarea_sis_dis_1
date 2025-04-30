import time
import random
import logging
import numpy as np
import requests
import json
from pymongo import MongoClient
from bson.objectid import ObjectId
import datetime
import threading
import redis


# Configuración de logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('traffic_generator')

# Configuración de la caché
CACHE_URL = "http://cache:5000/query"
CACHE_TTL = 300  # Tiempo de vida de la caché en segundos

# Configuración de distribuciones
DISTRIBUTION_PARAMS = {
    "normal": {
        "mean": 1,       #5   
        "std_dev": 0.5,      #2 
        "description": "Distribución Normal para simular picos de tráfico"
    },
    "zipf": {
        "s": 2, #1.5
        "description": "Distribución Zipf para popularidad de consultas"
    }
}

# Variables para estadísticas
stats = {
    "normal": {"queries": 0, "hits": 0, "misses": 0, "errors": 0, "response_time_sum": 0},
    "zipf": {"queries": 0, "hits": 0, "misses": 0, "errors": 0, "response_time_sum": 0},
    "total_queries": 0,
    "start_time": time.time()
}

# Configuración de Redis
redis_client = redis.Redis(host='redis', port=6379, db=0)


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
            wait_time = retry_count * 5  # Espera incremental
            logger.error(f"Error conectando a MongoDB (intento {retry_count}/{max_retries}): {e}")
            logger.info(f"Reintentando en {wait_time} segundos...")
            time.sleep(wait_time)
    
    logger.critical("Falló la conexión a MongoDB después de múltiples intentos")
    raise Exception("No se pudo establecer conexión con MongoDB")

def get_normal_event_id(collection, mean=5, std_dev=2):
    """
    Obtiene un ID de evento usando una distribución Normal truncada.
    Simula que algunos eventos son más populares (cercanos al centro de la lista).
    """
    all_events = list(collection.find({"uuid": {"$regex": "^waze_"}}, {"uuid": 1, "_id": 0}))
    n = len(all_events)
    if n == 0:
        logger.warning("No hay eventos en la base de datos para Normal")
        return None
    # Genera un índice según una normal centrada en la mitad de la lista
    center = n // 2
    while True:
        idx = int(np.random.normal(loc=center, scale=n//6))  
        if 0 <= idx < n:
            break
    event = all_events[idx]
    logger.info(f"Seleccionado evento (Normal) con UUID: {event['uuid']}")
    return event["uuid"]

def get_zipf_event_id(collection, s=1.5):
    """Obtiene un ID de evento usando distribución de Zipf"""
    all_events = list(collection.find({"uuid": {"$regex": "^waze_"}}, {"uuid": 1, "_id": 0}))
    n = len(all_events)
    if n == 0:
        logger.warning("No hay eventos en la base de datos para Zipf")
        return None
    # Genera un índice según Zipf (puede ser mayor que n, por eso el while)
    while True:
        idx = np.random.zipf(s) - 1 
        if idx < n:
            break
    event = all_events[idx]
    logger.info(f"Seleccionado evento (Zipf) con UUID: {event['uuid']}")
    return event["uuid"]

def normal_distribution(mean, std_dev):
    """Genera intervalos de tiempo siguiendo distribución Normal"""
    # Tiempo entre llegadas sigue una distribución normal
    interval = np.random.normal(mean, std_dev)
    return max(0.1, interval)  

def get_random_ttl(min_ttl=60, max_ttl=900):
    """
    Genera un TTL aleatorio entre min_ttl y max_ttl segundos.
    Por defecto: entre 1 minuto (60s) y 15 minutos (900s).
    """
    return random.randint(min_ttl, max_ttl)

def send_query(event_id, distribution_type):
    """Envía una consulta al servicio de caché"""
    if not event_id:
        return False
    
    try:
        # Generar un TTL aleatorio para cada consulta
        ttl = get_random_ttl()
        url = f"{CACHE_URL}?id={event_id}&distribution={distribution_type}&ttl={ttl}"
        logger.info(f"Enviando consulta: {url} (TTL={ttl}s)")
        
        start_time = time.time()
        response = requests.get(url)
        elapsed = time.time() - start_time
        
        # Actualizar estadísticas
        stats[distribution_type]["queries"] += 1
        stats["total_queries"] += 1
        stats[distribution_type]["response_time_sum"] += elapsed
        
        if response.status_code == 200:
            data = response.json()
            source = data.get('source', 'unknown')
            
            # Actualizar hit/miss en las estadísticas
            if source == "cache":
                stats[distribution_type]["hits"] += 1
            
            logger.info(f"Respuesta recibida desde {source} en {elapsed:.3f}s para {event_id}")
            return True
        else:
            stats[distribution_type]["errors"] += 1
            logger.warning(f"Error en consulta: {response.status_code}, {response.text}")
            return False
    except Exception as e:
        stats[distribution_type]["errors"] += 1
        logger.error(f"Error en consulta: {e}")
        return False

def generate_traffic(distribution_type, collection):
    """Genera tráfico según la distribución especificada"""
    logger.debug(f"Generando tráfico con distribución {distribution_type}")
    if distribution_type == "normal":
        params = DISTRIBUTION_PARAMS["normal"]
        interval = normal_distribution(params["mean"], params["std_dev"])
        event_id = get_normal_event_id(collection, params["mean"], params["std_dev"])
    elif distribution_type == "zipf":
        interval = 0.1 #poner1
        event_id = get_zipf_event_id(collection)
    else:
        logger.error(f"Distribución desconocida: {distribution_type}")
        return False

    logger.debug(f"Intervalo generado: {interval:.2f} segundos")
    time.sleep(interval)
    if event_id:
        return send_query(event_id, distribution_type)
    return False

def print_stats_periodically():
    """Imprime estadísticas periódicamente en un hilo separado"""
    while True:
        time.sleep(60)  # Cada minuto
        
        elapsed = time.time() - stats["start_time"]
        total_queries = stats["total_queries"]
        
        if total_queries == 0:
            continue
            
        rate = total_queries / elapsed
        
        # Estadísticas por distribución
        for dist in ["normal", "zipf"]:
            if stats[dist]["queries"] > 0:
                hit_rate = (stats[dist]["hits"] / stats[dist]["queries"]) * 100
                avg_time = stats[dist]["response_time_sum"] / stats[dist]["queries"] * 1000
                
                logger.info(f"Distribución {dist.upper()}: {stats[dist]['queries']} consultas, "
                            f"Hit rate: {hit_rate:.2f}%, "
                            f"Tiempo medio: {avg_time:.2f}ms")
        
        # Estadísticas globales
        logger.info(f"Total: {total_queries} consultas generadas, "
                    f"Tasa promedio: {rate:.2f} consultas/seg, "
                    f"Tiempo total: {elapsed:.1f} segundos")


def main():
    """Función principal del generador de tráfico"""
    logger.info("Iniciando servicio generador de tráfico")
    
    # Esperar a que los otros servicios estén disponibles
    time.sleep(30)
    
    # Conectar a MongoDB
    client = get_mongo_client()
    db = client['traffic_db']
    global collection
    collection = db['traffic_events']
    global current_distribution
    
    # Esperar a que haya datos en la base
    while collection.count_documents({}) < 10:
        logger.warning("No hay suficientes datos en la base, esperando...")
        time.sleep(60)
    
    logger.info(f"Base de datos tiene {collection.count_documents({})} eventos")
    
    # Variables para alternar entre distribuciones
    distribuciones = ["normal", "zipf"]
    idx = 0
    
    # Iniciar hilo para imprimir estadísticas periódicamente
    stats_thread = threading.Thread(target=print_stats_periodically, daemon=True)
    stats_thread.start()
    
    # Generar tráfico continuamente
    query_count = 0
    start_time = time.time()
    current_distribution = redis_client.get("current_distribution")
    if current_distribution:
        current_distribution = current_distribution.decode()
    else:
        current_distribution = "unknown"
    
    while True:
        try:
            # Verificar si es momento de cambiar la distribución
            if time.time() >= distribution_switch_time:
                idx = (idx + 1) % len(distribuciones)
                current_distribution = distribuciones[idx]
                logger.info(f"Cambiando a distribución {current_distribution}")
                distribution_switch_time = time.time() + 600  
                
                # Actualizar la distribución actual en Redis
                redis_client.set("current_distribution", current_distribution)
            
            # Generar una consulta
            if generate_traffic(current_distribution, collection):
                query_count += 1
            
            # Registrar estadísticas cada 100 consultas
            if query_count % 100 == 0 and query_count > 0:
                elapsed = time.time() - start_time
                rate = query_count / elapsed
                logger.info(f"Estadísticas: {query_count} consultas generadas, tasa promedio: {rate:.2f} consultas/seg")
                
        except Exception as e:
            logger.error(f"Error en el generador de tráfico: {e}")
            time.sleep(5) 

if __name__ == "__main__":
    main()