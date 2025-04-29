from flask import Flask, request, jsonify
import redis
import pymongo
import os
import json
import logging
import time
import traceback
from bson import ObjectId
import random

app = Flask(__name__)

# Configurar logging con más detalles
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Conexiones a Redis y MongoDB
try:
    redis_client = redis.Redis(host='redis', port=6379, db=0)
    logger.info("Conexión a Redis inicializada")
    
    mongo_client = pymongo.MongoClient('mongodb://mongodb:27017/')
    db = mongo_client['traffic_db']
    collection = db['traffic_events']
    logger.info("Conexión a MongoDB inicializada")
except Exception as e:
    logger.error(f"Error inicializando conexiones: {str(e)}")
    traceback.print_exc()

# Estadísticas de caché
cache_stats = {
    "hits": 0,
    "misses": 0
}

# Política de caché (LRU o LFU)
cache_policy = "LRU"  
cache_ttl = 300  

# En cache/app.py
CACHE_TTL = 600  

# Tamaño máximo de la caché
MAX_CACHE_SIZE = 100  

# Para LFU: contador de hits por clave
cache_hits_counter = {}

# Para LRU: timestamp del último uso
cache_usage_time = {}

def get_random_ttl(min_ttl=300, max_ttl=900):
    """
    Genera un TTL aleatorio entre el mínimo y máximo especificados.
    Por defecto: entre 5 minutos (300s) y 15 minutos (900s)
    """
    return random.randint(min_ttl, max_ttl)

@app.route('/health')
def health():
    return jsonify({"status": "OK"})

@app.route('/test-redis', methods=['GET'])
def test_redis():
    """Endpoint para probar la conexión a Redis"""
    try:
        # Intenta escribir algo en Redis
        test_value = f"test-{time.time()}"
        redis_client.set("test-key", test_value)
        
        # Intenta leer lo que escribiste
        retrieved = redis_client.get("test-key")
        
        if retrieved and retrieved.decode('utf-8') == test_value:
            # La escritura y lectura funcionaron
            return jsonify({
                "status": "success", 
                "message": "Redis funcionando correctamente",
                "redis_info": {
                    "size": redis_client.dbsize(),
                    "memory": redis_client.info("memory")
                }
            })
        else:
            return jsonify({
                "status": "error",
                "message": f"Fallo en la verificación: escrito='{test_value}', leído='{retrieved}'"
            }), 500
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Error conectando a Redis: {str(e)}"
        }), 500

@app.route('/test-mongodb', methods=['GET'])
def test_mongodb():
    """Endpoint para probar la conexión a MongoDB y ver datos disponibles"""
    try:
        # Verificar conexión
        db_names = mongo_client.list_database_names()
        
        # Contar documentos en la colección
        count = collection.count_documents({})
        
        # Obtener un ejemplo
        sample = None
        if count > 0:
            sample = collection.find_one()
            if sample and "_id" in sample and isinstance(sample["_id"], ObjectId):
                sample["_id"] = str(sample["_id"])
        
        return jsonify({
            "status": "success",
            "databases": db_names,
            "collection_count": count,
            "sample_document": sample
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Error conectando a MongoDB: {str(e)}"
        }), 500

@app.route('/force-cache', methods=['GET'])
def force_cache():
    """Endpoint para forzar la inserción de un elemento en la caché"""
    try:
        # Crear un elemento de prueba
        test_id = f"test-{int(time.time())}"
        test_data = {
            "_id": test_id,
            "type": "test",
            "location": "-33.4489,-70.6693",
            "location_desc": "Santiago Centro (TEST)",
            "description": "Elemento de prueba para caché",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")
        }
        
        # Guardar en Redis
        cache_key = f"event:{test_id}"
        random_ttl = get_random_ttl()
        result = redis_client.setex(cache_key, random_ttl, json.dumps(test_data))
        logger.info(f"Guardando en cache: key={cache_key}, TTL={random_ttl}s, length={len(json.dumps(test_data))}")
        
        # Verificar si se guardó
        cached = redis_client.get(cache_key)
        
        if cached:
            return jsonify({
                "status": "success",
                "message": "Elemento guardado en caché correctamente",
                "id": test_id,
                "cached_data": json.loads(cached)
            })
        else:
            return jsonify({
                "status": "error",
                "message": "Elemento no se guardó en caché"
            }), 500
            
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Error al forzar caché: {str(e)}"
        }), 500

@app.route('/query')
def query_event():
    """Endpoint para consultar eventos de tráfico"""
    event_id = request.args.get('id')
    distribution_type = request.args.get('distribution', 'uniform')
    
    if event_id:
        # Clave consistente para eventos por UUID
        cache_key = f"event:{event_id}"
        logger.info(f"Consulta por ID: {event_id}")
        
        # Verificar cache primero
        cached_result = redis_client.get(cache_key)
        
        if cached_result:
            # Cache hit - actualizar estadísticas y registros para LRU/LFU
            update_stats("hit", distribution_type)
            logger.info(f"Cache HIT para ID: {event_id}")
            
            current_time = time.time()
            cache_usage_time[cache_key] = current_time  # Para LRU
            
            if cache_key not in cache_hits_counter:
                cache_hits_counter[cache_key] = 0
            cache_hits_counter[cache_key] += 1  # Para LFU
            
            return jsonify({"events": json.loads(cached_result), "source": "cache"})
        
        # Cache miss
        update_stats("miss", distribution_type)
        logger.info(f"Cache MISS para ID: {event_id}")
        
        # Buscar en MongoDB
        try:
            # Buscar por UUID (como los genera el scraper)
            event = collection.find_one({"uuid": event_id})
            
            if not event and event_id.startswith("waze_"):
                # Búsqueda alternativa si el ID es de formato Waze
                base_id = event_id[5:]  
                event = collection.find_one({"waze_id": base_id})
            
            if event:
                # Convertir ObjectId a string
                if "_id" in event and isinstance(event["_id"], ObjectId):
                    event["_id"] = str(event["_id"])
                
                # Guardar en caché con TTL adecuado
                try:
                    # Si se excede el tamaño máximo, aplicar política de remoción
                    if redis_client.dbsize() >= MAX_CACHE_SIZE:
                        logger.info(f"Caché llena ({redis_client.dbsize()}/{MAX_CACHE_SIZE}), aplicando política {cache_policy}")
                        evict_from_cache()
                    
                    random_ttl = get_random_ttl() 
                    result = redis_client.setex(cache_key, random_ttl, json.dumps(event))
                    logger.info(f"Guardado en cache: {cache_key}, TTL: {random_ttl}s, resultado: {result}")
                except Exception as e:
                    logger.error(f"Error guardando en cache: {e}")
                
                return jsonify({"events": event, "source": "database"})
            else:
                logger.warning(f"Evento no encontrado: {event_id}")
                return jsonify({"error": "Event not found"}), 404
        except Exception as e:
            logger.error(f"Error buscando evento: {e}")
            return jsonify({"error": str(e)}), 500

@app.route('/stats', methods=['GET'])
def get_stats():
    """Endpoint para obtener estadísticas de caché"""
    try:
        total_queries = cache_stats["hits"] + cache_stats["misses"]
        hit_rate = 0
        if total_queries > 0:
            hit_rate = (cache_stats["hits"] / total_queries) * 100
        
        # Obtener información adicional de Redis para diagnóstico
        redis_info = {}
        try:
            redis_info = {
                "dbsize": redis_client.dbsize(),
                "memory": redis_client.info("memory").get("used_memory_human", "desconocido"),
                "keys": [k.decode('utf-8') for k in redis_client.keys("*")][:10]  
            }
        except Exception as e:
            redis_info = {"error": str(e)}
        
        stats = {
            "hits": cache_stats["hits"],
            "misses": cache_stats["misses"],
            "total_queries": total_queries,
            "hit_rate": f"{hit_rate:.2f}%",
            "cache_policy": cache_policy,
            "cache_size": redis_client.dbsize(),
            "redis_info": redis_info,
            "status": "Service running"
        }
        
        return jsonify(stats)
    except Exception as e:
        logger.error(f"Error en get_stats: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/policy', methods=['POST'])
def set_policy():
    """Endpoint para cambiar la política de caché (LRU o LFU)"""
    policy = request.json.get('policy', '').upper()
    
    if policy not in ['LRU', 'LFU']:
        return jsonify({"error": "Invalid policy. Use 'LRU' or 'LFU'"}), 400
    
    global cache_policy
    cache_policy = policy
    logger.info(f"Cache policy changed to {policy}")
    
    return jsonify({"message": f"Cache policy changed to {policy}"})

@app.route('/clear', methods=['POST'])
def clear_cache():
    """Endpoint para limpiar la caché"""
    redis_client.flushdb()
    logger.info("Cache cleared")
    return jsonify({"message": "Cache cleared successfully"})



# Agregar esta función para validar el ID del evento
def valid_event_id(event_id):
    """Valida si el ID del evento tiene un formato aceptable"""
    if not event_id:
        return False
    
    # Verificar si es un UUID válido
    import re
    uuid_pattern = re.compile(
        r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', 
        re.IGNORECASE
    )
    
    # Verificar si es un ID de Waze
    waze_pattern = re.compile(r'^waze_.*$')
    
    # Verificar si es un ObjectId válido de MongoDB
    object_id_pattern = re.compile(r'^[0-9a-f]{24}$')
    
    return bool(uuid_pattern.match(event_id) or object_id_pattern.match(event_id) or waze_pattern.match(event_id))

def is_valid_object_id(id_str):
    """Verifica si una cadena es un ObjectId válido"""
    import re
    return bool(re.match(r'^[0-9a-f]{24}$', id_str))

def update_stats(result_type, distribution_type="unknown"):
    """Actualiza estadísticas de cache por tipo de distribución"""
    global cache_stats
    if result_type == "hit":
        cache_stats["hits"] += 1
    elif result_type == "miss":
        cache_stats["misses"] += 1

def evict_from_cache():
    """Elimina elementos según la política de caché configurada"""
    try:
        if cache_policy == "LRU":
            # Encuentra la clave menos recientemente usada
            if cache_usage_time:
                oldest_key = min(cache_usage_time.items(), key=lambda x: x[1])[0]
                redis_client.delete(oldest_key)
                del cache_usage_time[oldest_key]
                logger.info(f"LRU: Eliminado {oldest_key} de la caché")
                
        elif cache_policy == "LFU":
            # Encuentra la clave menos frecuentemente usada
            if cache_hits_counter:
                least_used_key = min(cache_hits_counter.items(), key=lambda x: x[1])[0]
                redis_client.delete(least_used_key)
                del cache_hits_counter[least_used_key]
                logger.info(f"LFU: Eliminado {least_used_key} de la caché")
    except Exception as e:
        logger.error(f"Error en evicción de caché: {e}")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
