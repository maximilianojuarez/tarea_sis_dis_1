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


# Configuración de logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('traffic_generator')

# Configuración de la caché
CACHE_URL = "http://cache:5000/query"
CACHE_TTL = 300  # Tiempo de vida de la caché en segundos

# Configuración de distribuciones
DISTRIBUTION_PARAMS = {
    "poisson": {
        "lambda_rate": 10/60,  # 10 consultas por minuto en promedio
        "description": "Distribución de Poisson para simular llegadas aleatorias uniformes"
    },
    "normal": {
        "mean": 5,          # Media de 5 segundos entre consultas
        "std_dev": 2,       # Desviación estándar de 2 segundos
        "description": "Distribución Normal para simular picos de tráfico"
    }
}

# Variables para estadísticas
stats = {
    "poisson": {"queries": 0, "hits": 0, "misses": 0, "errors": 0, "response_time_sum": 0},
    "normal": {"queries": 0, "hits": 0, "misses": 0, "errors": 0, "response_time_sum": 0},
    "total_queries": 0,
    "start_time": time.time()
}


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

def get_random_event_id(collection):
    """Obtiene un ID aleatorio de eventos de Waze existentes en MongoDB"""
    try:
        # Obtener una lista de todos los UUIDs existentes
        all_events = list(collection.find({}, {"uuid": 1, "_id": 0}))
        
        if not all_events:
            logger.warning("No hay eventos en la base de datos")
            return None
            
        # Filtrar solo los que comienzan con waze_
        waze_events = [e for e in all_events if e.get('uuid', '').startswith('waze_')]
        
        if not waze_events:
            logger.warning("No hay eventos de Waze en la base de datos")
            return None
        
        # Seleccionar un evento aleatorio
        event = random.choice(waze_events)
        logger.info(f"Seleccionado evento con UUID: {event['uuid']}")
        
        return event["uuid"]
    except Exception as e:
        logger.error(f"Error obteniendo evento aleatorio: {e}")
        return None

def poisson_distribution(lambda_rate):
    """Genera intervalos de tiempo siguiendo distribución de Poisson"""
    # Tiempo entre llegadas sigue una distribución exponencial
    interval = np.random.exponential(1.0 / lambda_rate)
    return max(0.1, interval)  # Evitar intervalos muy pequeños

def normal_distribution(mean, std_dev):
    """Genera intervalos de tiempo siguiendo distribución Normal"""
    # Tiempo entre llegadas sigue una distribución normal
    interval = np.random.normal(mean, std_dev)
    return max(0.1, interval)  # Asegurar intervalos positivos

def send_query(event_id, distribution_type):
    """Envía una consulta al servicio de caché"""
    if not event_id:
        return False
    
    try:
        url = f"{CACHE_URL}?id={event_id}&distribution={distribution_type}"
        logger.info(f"Enviando consulta: {url}")
        
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
    
    if distribution_type == "poisson":
        params = DISTRIBUTION_PARAMS["poisson"]
        interval = poisson_distribution(params["lambda_rate"])
    else:
        params = DISTRIBUTION_PARAMS["normal"]
        interval = normal_distribution(params["mean"], params["std_dev"])
    
    logger.debug(f"Intervalo generado: {interval:.2f} segundos")
    time.sleep(interval)
    
    event_id = get_random_event_id(collection)
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
        for dist in ["poisson", "normal"]:
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
    
    # Esperar a que haya datos en la base
    while collection.count_documents({}) < 10:
        logger.warning("No hay suficientes datos en la base, esperando...")
        time.sleep(60)
    
    logger.info(f"Base de datos tiene {collection.count_documents({})} eventos")
    
    # Variables para alternar entre distribuciones
    current_distribution = "poisson"
    distribution_switch_time = time.time() + 600 
    
    # Iniciar hilo para imprimir estadísticas periódicamente
    stats_thread = threading.Thread(target=print_stats_periodically, daemon=True)
    stats_thread.start()
    
    # Generar tráfico continuamente
    query_count = 0
    start_time = time.time()
    
    while True:
        try:
            # Verificar si es momento de cambiar la distribución
            if time.time() >= distribution_switch_time:
                current_distribution = "normal" if current_distribution == "poisson" else "poisson"
                logger.info(f"Cambiando a distribución {current_distribution}")
                distribution_switch_time = time.time() + 600  
            
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