from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
import redis
from elasticsearch import Elasticsearch
import pymongo
from bson import ObjectId
import json
import hashlib
import time
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import logging
from contextlib import asynccontextmanager
import os
from dataclasses import dataclass
import heapq
from threading import Lock


# Clase MongoDBService
class MongoDBService:
    def __init__(self, host: str = "localhost", port: int = 27017, db_name: str = "traffic_db"):
        self.client = pymongo.MongoClient(host=host, port=port, serverSelectionTimeoutMS=3000)
        self.db = self.client[db_name]

    def is_connected(self) -> bool:
        """Verifica la conexión a MongoDB"""
        try:
            # Lanza una excepción si no puede conectarse
            self.client.admin.command("ping")
            return True
        except Exception:
            return False

    async def find_event_by_id(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Busca un evento en MongoDB por su ID (como string)"""
        try:
            collection = self.db["events"]
            # Convertir a ObjectId si es válido
            query = {"_id": ObjectId(event_id)} if ObjectId.is_valid(event_id) else {"event_id": event_id}
            result = collection.find_one(query)
            if result:
                result["_id"] = str(result["_id"])  # Convertir ObjectId a string para serialización JSON
                return result
            return None
        except Exception as e:
            logger.error(f"Error al consultar MongoDB: {e}")
            return None


# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Enums y modelos
class CachePolicy(str, Enum):
    LRU = "lru"
    LFU = "lfu"

class QueryType(str, Enum):
    INCIDENTS = "incidents"
    TRAFFIC_FLOW = "traffic_flow"
    AGGREGATED = "aggregated"

@dataclass
class CacheStats:
    hits: int = 0
    misses: int = 0
    total_requests: int = 0
    
    @property
    def hit_rate(self) -> float:
        return (self.hits / self.total_requests) if self.total_requests > 0 else 0.0

# Modelos Pydantic
class CacheConfig(BaseModel):
    policy: CachePolicy = CachePolicy.LRU
    ttl_seconds: int = Field(default=300, ge=1, le=86400)  # 5 min - 24 horas
    max_size: int = Field(default=1000, ge=10, le=100000)

class QueryRequest(BaseModel):
    query_type: QueryType
    comuna: Optional[str] = None
    incident_type: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    limit: Optional[int] = Field(default=100, ge=1, le=1000)
    aggregation_level: Optional[str] = None  # hour, day, week

class QueryResponse(BaseModel):
    data: List[Dict[str, Any]]
    total_results: int
    cached: bool
    query_time_ms: float
    source: str  # "cache" o "elasticsearch"

class HealthResponse(BaseModel):
    status: str
    redis_connected: bool
    elasticsearch_connected: bool
    cache_stats: Dict[str, Any]
    uptime_seconds: float

# Implementación del caché inteligente
class IntelligentCache:
    def __init__(self, redis_client: redis.Redis, policy: CachePolicy = CachePolicy.LRU, 
                 ttl: int = 300, max_size: int = 1000):
        self.redis = redis_client
        self.policy = policy
        self.ttl = ttl
        self.max_size = max_size
        self.stats = CacheStats()
        self.lock = Lock()
        
        # Para LFU: mantener contadores de frecuencia
        self.frequency_heap = []
        self.key_frequencies = {}
        
    def _generate_cache_key(self, query_request: QueryRequest) -> str:
        """Genera una clave única para la consulta"""
        query_dict = query_request.dict()
        query_str = json.dumps(query_dict, sort_keys=True)
        return f"traffic_cache:{hashlib.md5(query_str.encode()).hexdigest()}"
    
    async def get(self, query_request: QueryRequest) -> Optional[Dict[str, Any]]:
        """Obtiene datos del caché"""
        cache_key = self._generate_cache_key(query_request)
        
        try:
            # Intentar obtener del caché
            cached_data = self.redis.get(cache_key)
            
            with self.lock:
                self.stats.total_requests += 1
                
                if cached_data:
                    self.stats.hits += 1
                    self._update_access_policy(cache_key)
                    logger.info(f"Cache HIT para clave: {cache_key}")
                    return json.loads(cached_data)
                else:
                    self.stats.misses += 1
                    logger.info(f"Cache MISS para clave: {cache_key}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error al acceder al caché: {e}")
            with self.lock:
                self.stats.total_requests += 1
                self.stats.misses += 1
            return None
    
    async def set(self, query_request: QueryRequest, data: Dict[str, Any]) -> bool:
        """Almacena datos en el caché"""
        cache_key = self._generate_cache_key(query_request)
        
        try:
            # Verificar límite de tamaño del caché
            current_size = self.redis.dbsize()
            if current_size >= self.max_size:
                await self._evict_keys()
            
            # Almacenar con TTL
            serialized_data = json.dumps(data)
            success = self.redis.setex(cache_key, self.ttl, serialized_data)
            
            if success:
                self._update_storage_policy(cache_key)
                logger.info(f"Datos almacenados en caché: {cache_key}")
                return True
            return False
            
        except Exception as e:
            logger.error(f"Error al almacenar en caché: {e}")
            return False
    
    def _update_access_policy(self, cache_key: str):
        """Actualiza las métricas según la política de caché"""
        if self.policy == CachePolicy.LFU:
            self.key_frequencies[cache_key] = self.key_frequencies.get(cache_key, 0) + 1
        # Para LRU, Redis maneja automáticamente el orden de acceso
    
    def _update_storage_policy(self, cache_key: str):
        """Actualiza las métricas al almacenar"""
        if self.policy == CachePolicy.LFU:
            self.key_frequencies[cache_key] = 1
    
    async def _evict_keys(self):
        """Desaloja claves según la política configurada"""
        try:
            if self.policy == CachePolicy.LRU:
                # Redis con política LRU automática
                self.redis.config_set('maxmemory-policy', 'allkeys-lru')
            elif self.policy == CachePolicy.LFU:
                # Implementar LFU manual
                if self.key_frequencies:
                    # Encontrar las claves menos frecuentes
                    sorted_keys = sorted(self.key_frequencies.items(), key=lambda x: x[1])
                    keys_to_remove = [key for key, _ in sorted_keys[:10]]  # Remover 10 claves
                    
                    for key in keys_to_remove:
                        self.redis.delete(key)
                        del self.key_frequencies[key]
                        
        except Exception as e:
            logger.error(f"Error al desalojar claves del caché: {e}")

# Servicio de Elasticsearch
class ElasticsearchService:
    def __init__(self, host: str = "localhost", port: int = 9200):
        self.client = Elasticsearch([f"http://{host}:{port}"])
        
    def is_connected(self) -> bool:
        """Verifica la conexión con Elasticsearch"""
        try:
            return self.client.ping()
        except:
            return False
    
    async def search_incidents(self, query_request: QueryRequest) -> Dict[str, Any]:
        """Busca incidentes en Elasticsearch"""
        query_body = self._build_incidents_query(query_request)
        
        try:
            response = self.client.search(
                index="traffic_incidents",
                body=query_body,
                size=query_request.limit
            )
            
            return {
                "data": [hit["_source"] for hit in response["hits"]["hits"]],
                "total_results": response["hits"]["total"]["value"],
                "query_time_ms": response["took"]
            }
        except Exception as e:
            logger.error(f"Error en búsqueda de incidentes: {e}")
            raise HTTPException(status_code=500, detail=f"Error en Elasticsearch: {str(e)}")
    
    async def search_traffic_flow(self, query_request: QueryRequest) -> Dict[str, Any]:
        """Busca datos de flujo de tráfico"""
        query_body = self._build_traffic_flow_query(query_request)
        
        try:
            response = self.client.search(
                index="traffic_flow",
                body=query_body,
                size=query_request.limit
            )
            
            return {
                "data": [hit["_source"] for hit in response["hits"]["hits"]],
                "total_results": response["hits"]["total"]["value"],
                "query_time_ms": response["took"]
            }
        except Exception as e:
            logger.error(f"Error en búsqueda de flujo de tráfico: {e}")
            raise HTTPException(status_code=500, detail=f"Error en Elasticsearch: {str(e)}")
    
    async def search_aggregated(self, query_request: QueryRequest) -> Dict[str, Any]:
        """Busca datos agregados"""
        query_body = self._build_aggregated_query(query_request)
        
        try:
            response = self.client.search(
                index="traffic_aggregated",
                body=query_body,
                size=0  # Solo agregaciones
            )
            
            return {
                "data": self._process_aggregations(response.get("aggregations", {})),
                "total_results": len(response.get("aggregations", {})),
                "query_time_ms": response["took"]
            }
        except Exception as e:
            logger.error(f"Error en búsqueda agregada: {e}")
            raise HTTPException(status_code=500, detail=f"Error en Elasticsearch: {str(e)}")
    
    def _build_incidents_query(self, query_request: QueryRequest) -> Dict[str, Any]:
        """Construye query para incidentes"""
        query = {"query": {"bool": {"must": []}}}
        
        if query_request.comuna:
            query["query"]["bool"]["must"].append({
                "term": {"comuna.keyword": query_request.comuna}
            })
        
        if query_request.incident_type:
            query["query"]["bool"]["must"].append({
                "term": {"type.keyword": query_request.incident_type}
            })
        
        if query_request.start_time and query_request.end_time:
            query["query"]["bool"]["must"].append({
                "range": {
                    "timestamp": {
                        "gte": query_request.start_time,
                        "lte": query_request.end_time
                    }
                }
            })
        
        # Agregar ordenamiento por timestamp
        query["sort"] = [{"timestamp": {"order": "desc"}}]
        
        return query
    
    def _build_traffic_flow_query(self, query_request: QueryRequest) -> Dict[str, Any]:
        """Construye query para flujo de tráfico"""
        query = {"query": {"bool": {"must": []}}}
        
        if query_request.comuna:
            query["query"]["bool"]["must"].append({
                "term": {"comuna.keyword": query_request.comuna}
            })
        
        if query_request.start_time and query_request.end_time:
            query["query"]["bool"]["must"].append({
                "range": {
                    "timestamp": {
                        "gte": query_request.start_time,
                        "lte": query_request.end_time
                    }
                }
            })
        
        query["sort"] = [{"timestamp": {"order": "desc"}}]
        return query
    
    def _build_aggregated_query(self, query_request: QueryRequest) -> Dict[str, Any]:
        """Construye query para datos agregados"""
        aggs = {}
        
        if query_request.aggregation_level == "hour":
            aggs["by_hour"] = {
                "date_histogram": {
                    "field": "timestamp",
                    "calendar_interval": "1h"
                },
                "aggs": {
                    "avg_speed": {"avg": {"field": "speed"}},
                    "incident_count": {"value_count": {"field": "incident_id"}}
                }
            }
        elif query_request.aggregation_level == "comuna":
            aggs["by_comuna"] = {
                "terms": {"field": "comuna.keyword", "size": 50},
                "aggs": {
                    "avg_speed": {"avg": {"field": "speed"}},
                    "incident_count": {"value_count": {"field": "incident_id"}}
                }
            }
        
        query = {
            "query": {"bool": {"must": []}},
            "aggs": aggs
        }
        
        if query_request.start_time and query_request.end_time:
            query["query"]["bool"]["must"].append({
                "range": {
                    "timestamp": {
                        "gte": query_request.start_time,
                        "lte": query_request.end_time
                    }
                }
            })
        
        return query
    
    def _process_aggregations(self, aggregations: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Procesa las agregaciones de Elasticsearch"""
        results = []
        
        for agg_name, agg_data in aggregations.items():
            if "buckets" in agg_data:
                for bucket in agg_data["buckets"]:
                    result = {"key": bucket["key"], "doc_count": bucket["doc_count"]}
                    # Agregar métricas adicionales
                    for metric_name, metric_data in bucket.items():
                        if isinstance(metric_data, dict) and "value" in metric_data:
                            result[metric_name] = metric_data["value"]
                    results.append(result)
        
        return results

# Variables globales
cache_service: Optional[IntelligentCache] = None
elasticsearch_service: Optional[ElasticsearchService] = None
mongodb_service: Optional[MongoDBService] = None
app_start_time = time.time()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global cache_service, elasticsearch_service, mongodb_service
    
    # Configuración desde variables de entorno
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_db = int(os.getenv("REDIS_DB", "0"))
    es_host = os.getenv("ELASTICSEARCH_HOST", "localhost")
    es_port = int(os.getenv("ELASTICSEARCH_PORT", "9200"))
    mongo_host = os.getenv("MONGODB_HOST", "localhost")
    mongo_port = int(os.getenv("MONGODB_PORT", "27017"))
    mongo_db = os.getenv("MONGODB_DB", "traffic_db")
    
    # Inicializar servicios
    try:
        redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db, 
                                 decode_responses=True, socket_timeout=5)
        redis_client.ping()  # Verificar conexión
        
        cache_service = IntelligentCache(redis_client)
        elasticsearch_service = ElasticsearchService(es_host, es_port)
        mongodb_service = MongoDBService(mongo_host, mongo_port, mongo_db)
        
        logger.info("Servicios iniciados correctamente")
    except Exception as e:
        logger.error(f"Error al inicializar servicios: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("Cerrando servicios...")
    if mongodb_service:
        mongodb_service.client.close()

# Inicializar FastAPI
app = FastAPI(
    title="Microservicio de Caché de Tráfico",
    description="API REST para consultas de tráfico con caché inteligente Redis + Elasticsearch",
    version="1.0.0",
    lifespan=lifespan
)

# Middleware CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dependencias
def get_cache_service() -> IntelligentCache:
    if cache_service is None:
        raise HTTPException(status_code=503, detail="Servicio de caché no disponible")
    return cache_service

def get_elasticsearch_service() -> ElasticsearchService:
    if elasticsearch_service is None:
        raise HTTPException(status_code=503, detail="Servicio de Elasticsearch no disponible")
    return elasticsearch_service

def get_mongodb_service() -> MongoDBService:
    if mongodb_service is None:
        raise HTTPException(status_code=503, detail="Servicio de MongoDB no disponible")
    return mongodb_service

# Endpoints principales
@app.post("/api/v1/query", response_model=QueryResponse)
async def query_traffic_data(
    query_request: QueryRequest,
    cache: IntelligentCache = Depends(get_cache_service),
    es: ElasticsearchService = Depends(get_elasticsearch_service)
):
    """Endpoint principal para consultas de datos de tráfico"""
    start_time = time.time()
    
    # Intentar obtener del caché
    cached_result = await cache.get(query_request)
    
    if cached_result:
        query_time_ms = (time.time() - start_time) * 1000
        return QueryResponse(
            data=cached_result["data"],
            total_results=cached_result["total_results"],
            cached=True,
            query_time_ms=query_time_ms,
            source="cache"
        )
    
    # Cache miss - consultar Elasticsearch
    try:
        if query_request.query_type == QueryType.INCIDENTS:
            es_result = await es.search_incidents(query_request)
        elif query_request.query_type == QueryType.TRAFFIC_FLOW:
            es_result = await es.search_traffic_flow(query_request)
        elif query_request.query_type == QueryType.AGGREGATED:
            es_result = await es.search_aggregated(query_request)
        else:
            raise HTTPException(status_code=400, detail="Tipo de consulta no válido")
        
        # Almacenar en caché
        await cache.set(query_request, es_result)
        
        query_time_ms = (time.time() - start_time) * 1000
        
        return QueryResponse(
            data=es_result["data"],
            total_results=es_result["total_results"],
            cached=False,
            query_time_ms=query_time_ms,
            source="elasticsearch"
        )
        
    except Exception as e:
        logger.error(f"Error en consulta: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint de compatibilidad con tu sistema actual
@app.get("/query")
async def legacy_query_event(
    id: str = Query(..., description="ID del evento"),
    cache: IntelligentCache = Depends(get_cache_service),
    mongo: MongoDBService = Depends(get_mongodb_service)
):
    """Endpoint de compatibilidad con tu sistema Flask actual"""
    start_time = time.time()
    
    # Generar clave de caché consistente
    cache_key = f"event:{id}"
    
    # Intentar obtener del caché
    try:
        cached_result = cache.redis.get(cache_key)
        if cached_result:
            logger.info(f"Cache HIT para ID: {id}")
            return {
                "events": json.loads(cached_result),
                "source": "cache",
                "query_time_ms": (time.time() - start_time) * 1000
            }
    except Exception as e:
        logger.warning(f"Error al acceder al caché: {e}")
    
    # Cache miss - buscar en MongoDB
    logger.info(f"Cache MISS para ID: {id}")
    event = await mongo.find_event_by_id(id)
    
    if event:
        # Guardar en caché con TTL
        try:
            ttl = 300 + (time.time() % 300)  # TTL variable entre 300-600s
            cache.redis.setex(cache_key, int(ttl), json.dumps(event))
            logger.info(f"Evento guardado en caché: {cache_key}")
        except Exception as e:
            logger.error(f"Error guardando en caché: {e}")
        
        return {
            "events": event,
            "source": "database",
            "query_time_ms": (time.time() - start_time) * 1000
        }
    else:
        raise HTTPException(status_code=404, detail="Event not found")

# Endpoint de estadísticas (compatible con tu sistema)
@app.get("/stats")
async def legacy_stats(cache: IntelligentCache = Depends(get_cache_service)):
    """Endpoint de estadísticas compatible con tu sistema Flask"""
    try:
        redis_info = {
            "dbsize": cache.redis.dbsize(),
            "memory": cache.redis.info("memory").get("used_memory_human", "unknown"),
            "keys": [k for k in cache.redis.keys("*")][:10]
        }
        
        return {
            "hits": cache.stats.hits,
            "misses": cache.stats.misses,
            "total_queries": cache.stats.total_requests,
            "hit_rate": f"{cache.stats.hit_rate * 100:.2f}%",
            "cache_policy": cache.policy.value,
            "cache_size": cache.redis.dbsize(),
            "redis_info": redis_info,
            "status": "Service running"
        }
    except Exception as e:
        logger.error(f"Error en stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/cache/configure")
async def configure_cache(
    config: CacheConfig,
    cache: IntelligentCache = Depends(get_cache_service)
):
    """Configura la política y parámetros del caché"""
    try:
        cache.policy = config.policy
        cache.ttl = config.ttl_seconds
        cache.max_size = config.max_size
        
        logger.info(f"Caché reconfigurado: {config}")
        return {"message": "Configuración de caché actualizada", "config": config}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al configurar caché: {str(e)}")

@app.delete("/api/v1/cache/clear")
async def clear_cache(cache: IntelligentCache = Depends(get_cache_service)):
    """Limpia todo el caché"""
    try:
        cache.redis.flushdb()
        cache.stats = CacheStats()  # Resetear estadísticas
        logger.info("Caché limpiado completamente")
        return {"message": "Caché limpiado exitosamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al limpiar caché: {str(e)}")

@app.get("/api/v1/cache/stats")
async def get_cache_stats(cache: IntelligentCache = Depends(get_cache_service)):
    """Obtiene estadísticas del caché"""
    try:
        redis_info = cache.redis.info()
        return {
            "hit_rate": cache.stats.hit_rate,
            "total_hits": cache.stats.hits,
            "total_misses": cache.stats.misses,
            "total_requests": cache.stats.total_requests,
            "cache_policy": cache.policy,
            "ttl_seconds": cache.ttl,
            "max_size": cache.max_size,
            "current_keys": cache.redis.dbsize(),
            "memory_usage": redis_info.get("used_memory_human", "N/A"),
            "uptime": redis_info.get("uptime_in_seconds", 0)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener estadísticas: {str(e)}")

@app.get("/api/v1/health", response_model=HealthResponse)
async def health_check(
    cache: IntelligentCache = Depends(get_cache_service),
    es: ElasticsearchService = Depends(get_elasticsearch_service)
):
    """Health check del microservicio"""
    uptime = time.time() - app_start_time
    
    try:
        redis_connected = cache.redis.ping()
    except:
        redis_connected = False
    
    elasticsearch_connected = es.is_connected()
    
    status = "healthy" if redis_connected and elasticsearch_connected else "unhealthy"
    
    return HealthResponse(
        status=status,
        redis_connected=redis_connected,
        elasticsearch_connected=elasticsearch_connected,
        cache_stats={
            "hit_rate": cache.stats.hit_rate,
            "total_requests": cache.stats.total_requests
        },
        uptime_seconds=uptime
    )

@app.get("/")
async def root():
    """Endpoint raíz con información del servicio"""
    return {
        "service": "Microservicio de Caché de Tráfico",
        "version": "1.0.0",
        "description": "API REST para consultas de tráfico con caché inteligente",
        "endpoints": {
            "query": "/api/v1/query",
            "configure_cache": "/api/v1/cache/configure",
            "clear_cache": "/api/v1/cache/clear",
            "cache_stats": "/api/v1/cache/stats",
            "health": "/api/v1/health",
            "docs": "/docs"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)