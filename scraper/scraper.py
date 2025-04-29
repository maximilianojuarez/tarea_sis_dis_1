import os
import json
import time
import logging
import datetime
import random
import requests
import uuid
from requests.exceptions import RequestException



USED_UUIDS = set()

def get_unique_uuid():
    """Genera un UUID garantizado único"""
    while True:
        new_uuid = str(uuid.uuid4())
        if new_uuid not in USED_UUIDS:
            USED_UUIDS.add(new_uuid)
            return new_uuid

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('scraper')

# Definición del bounding box de la Región Metropolitana
# Estas coordenadas forman un rectángulo que cubre toda la RM
RM_BOUNDING_BOX = {
    "min_lat": -33.83,  # Sur
    "max_lat": -33.15,  # Norte
    "min_lon": -71.08,  # Oeste
    "max_lon": -70.41   # Este
}

# División en cuadrantes para consultas más efectivas
def generate_quadrants(bbox, grid_size=2):
    """Divide el área en cuadrantes para consultas más pequeñas"""
    quadrants = []
    
    lat_step = (bbox["max_lat"] - bbox["min_lat"]) / grid_size
    lon_step = (bbox["max_lon"] - bbox["min_lon"]) / grid_size
    
    for i in range(grid_size):
        for j in range(grid_size):
            min_lat = bbox["min_lat"] + (i * lat_step)
            max_lat = bbox["min_lat"] + ((i + 1) * lat_step)
            min_lon = bbox["min_lon"] + (j * lon_step)
            max_lon = bbox["min_lon"] + ((j + 1) * lon_step)
            
            # Calcular el centro del cuadrante
            center_lat = (min_lat + max_lat) / 2
            center_lon = (min_lon + max_lon) / 2
            
            quadrant = {
                "min_lat": min_lat,
                "max_lat": max_lat,
                "min_lon": min_lon,
                "max_lon": max_lon,
                "lat": center_lat,    
                "lon": center_lon,    
                "name": f"RM Cuadrante {i+1}-{j+1}"
            }
            quadrants.append(quadrant)
    
    logger.info(f"Área dividida en {len(quadrants)} cuadrantes para consultas")
    return quadrants

# Generar cuadrantes para toda la región
RM_QUADRANTS = generate_quadrants(RM_BOUNDING_BOX, grid_size=3)

# Agregar después de las definiciones de RM_QUADRANTS

# Mapeo de cuadrantes a comunas
QUADRANT_TO_COMUNA = {
    "1-1": "Maipú/Padre Hurtado",
    "1-2": "San Bernardo/Buin",
    "1-3": "Puente Alto/La Florida",
    "2-1": "Pudahuel/Cerro Navia",
    "2-2": "Santiago Centro/Estación Central",
    "2-3": "Las Condes/Providencia",
   
}

# Función para determinar la comuna basada en coordenadas
def get_comuna_from_coordinates(lat, lon):
    """Determina la comuna basada en coordenadas geográficas"""
   
    
    # Comunas del sector oriente
    if lat > -33.42 and lon > -70.58:
        return "Las Condes"
    elif lat > -33.45 and lon > -70.60:
        return "Providencia"
    elif lat > -33.51 and lon > -70.58:
        return "La Florida"
    elif lat < -33.55 and lon > -70.60:
        return "Puente Alto"
    elif lat < -33.60 and lon < -70.70:
        return "San Bernardo"
    elif lat < -33.50 and lon < -70.75:
        return "Maipú"
    elif lat > -33.45 and lon < -70.68:
        return "Santiago Centro"
  
    return "Región Metropolitana"

# User-Agents para rotar
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/112.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
]

def get_random_user_agent():
    """Devuelve un User-Agent aleatorio de la lista"""
    return random.choice(USER_AGENTS)

def get_traffic_data_for_quadrant(quadrant):
    """Obtiene datos de tráfico para un cuadrante usando la API de Waze"""
    try:
        logger.info(f"Obteniendo datos de tráfico para {quadrant['name']}...")
        
        # URL de la API de Waze LiveMap 
        url = f"https://www.waze.com/live-map/api/georss?bottom={quadrant['min_lat']}&left={quadrant['min_lon']}&top={quadrant['max_lat']}&right={quadrant['max_lon']}&env=row&ma=600&types=alerts,traffic"
        
        headers = {
            "User-Agent": get_random_user_agent(),
            "Referer": "https://www.waze.com/live-map",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            "Origin": "https://www.waze.com",
            "Cache-Control": "no-cache"
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        
        if response.status_code == 200:
            logger.info(f"Datos obtenidos exitosamente para {quadrant['name']}")
            return process_waze_data(response.json(), quadrant)
        else:
            logger.warning(f"Error al obtener datos para {quadrant['name']}: Código {response.status_code}")
            return []
            
    except Exception as e:
        logger.error(f"Error obteniendo datos para {quadrant['name']}: {e}")
        return []

def process_waze_data(data, quadrant):
    """Procesa los datos obtenidos de la API de Waze"""
    events = []
    
    # Procesar alertas (accidentes, peligros, policía, etc.)
    if "alerts" in data and isinstance(data["alerts"], list):
        for alert in data["alerts"]:
            try:
                # Obtener tipo de alerta
                alert_type = map_type(alert.get("type", ""))
                
                # Obtener coordenadas
                lat = alert.get("location", {}).get("y", quadrant["lat"])
                lon = alert.get("location", {}).get("x", quadrant["lon"])
                
                # Determinar la comuna basada en coordenadas
                comuna = get_comuna_from_coordinates(lat, lon)
                
                # Crear descripción adecuada
                if "reportDescription" in alert and alert["reportDescription"]:
                    description = alert["reportDescription"]
                elif "street" in alert and alert["street"]:
                    description = f"{alert_type.title()} en {alert['street']}"
                else:
                    description = f"{alert_type.title()} en {comuna}"
                
                # Usar el ID de Waze, o generar uno si no existe
                waze_id = alert.get("id", None) or alert.get("uuid", None)
                if not waze_id:
                    # Si Waze no proporciona ID, generamos uno
                    waze_id = str(uuid.uuid4())
                else:
                    # Añadir prefijo para indicar que es un ID de Waze
                    waze_id = f"waze_{waze_id}"
                
                # Crear evento
                event = {
                    "uuid": waze_id,
                    "type": alert_type,
                    "location": f"{lat},{lon}",
                    "location_desc": comuna,
                    "description": description,
                    "timestamp": datetime.datetime.now().isoformat(),
                    "source": "waze_api",
                    "waze_id": alert.get("id", "")  
                }
                
                events.append(event)
                
            except Exception as e:
                logger.error(f"Error procesando alerta: {e}")
    
    # Procesar atascos/congestiones de manera similar
    if "jams" in data and isinstance(data["jams"], list):
        for jam in data["jams"]:
            try:
                # Obtener coordenadas
                if "line" in jam and isinstance(jam["line"], list) and len(jam["line"]) > 0:
                    lat = jam["line"][0].get("y", quadrant["lat"])
                    lon = jam["line"][0].get("x", quadrant["lon"])
                else:
                    lat, lon = quadrant["lat"], quadrant["lon"]
                
                # Determinar comuna basada en coordenadas
                comuna = get_comuna_from_coordinates(lat, lon)
                
                # Crear descripción
                if "street" in jam and jam["street"]:
                    description = f"Congestión en {jam['street']}"
                else:
                    description = f"Congestión de tráfico en {comuna}"
                
                # Usar el ID de Waze, o generar uno si no existe
                waze_id = jam.get("uuid", None) or jam.get("jamId", None)
                if not waze_id:
                    waze_id = str(uuid.uuid4())
                else:
                    waze_id = f"waze_{waze_id}"
                
                # Crear evento
                event = {
                    "uuid": waze_id,
                    "type": "traffic_jam",
                    "location": f"{lat},{lon}",
                    "location_desc": comuna,
                    "description": description,
                    "timestamp": datetime.datetime.now().isoformat(),
                    "source": "waze_api",
                    "length_meters": jam.get("length", 0),
                    "speed": jam.get("speed", 0),
                    "congestion_level": jam.get("level", 0),
                    "delay_seconds": jam.get("delay", -1),
                    "waze_id": jam.get("jamId", "")  
                }
                
                events.append(event)
                
            except Exception as e:
                logger.error(f"Error procesando congestión: {e}")
    
    # Determinar la comuna representativa del cuadrante
    comuna_representativa = get_comuna_from_coordinates(quadrant["lat"], quadrant["lon"])

    # Alternativa: extraer el ID del cuadrante y usar el mapeo predefinido
    quadrant_id = quadrant["name"].replace("RM Cuadrante ", "")
    comuna_mapeo = QUADRANT_TO_COMUNA.get(quadrant_id, comuna_representativa)

    logger.info(f"Extraídos {len(events)} eventos para {comuna_mapeo}")

    # Obtener distribución de comunas
    if events:
        comuna_distribution = get_comunas_distribution(events)
        main_comuna = comuna_distribution[0][0] if comuna_distribution else "Desconocido"
        logger.info(f"Extraídos {len(events)} eventos para {main_comuna} y otras comunas")
        # Opcional: mostrar detalle de distribución
        for comuna, count in comuna_distribution[:3]:  # Mostrar top 3
            logger.info(f"  - {comuna}: {count} eventos")
    else:
        logger.info(f"No se extrajeron eventos para este cuadrante")

    return events

# Lista de comunas para asignar ubicaciones a zonas conocidas
COMUNAS_RM = [
    {"name": "Santiago Centro", "lat": -33.4489, "lon": -70.6693},
    {"name": "Providencia", "lat": -33.4314, "lon": -70.6093},
    {"name": "Las Condes", "lat": -33.4145, "lon": -70.5838},
    {"name": "Vitacura", "lat": -33.3857, "lon": -70.5719},
    {"name": "Lo Barnechea", "lat": -33.3504, "lon": -70.5182},
    {"name": "Ñuñoa", "lat": -33.4542, "lon": -70.5981},
    {"name": "La Reina", "lat": -33.4422, "lon": -70.5356},
    {"name": "Macul", "lat": -33.4895, "lon": -70.5978},
    {"name": "Peñalolén", "lat": -33.4836, "lon": -70.5325},
    {"name": "Huechuraba", "lat": -33.3473, "lon": -70.6505},
    {"name": "Conchalí", "lat": -33.3833, "lon": -70.6667},
    {"name": "Independencia", "lat": -33.4167, "lon": -70.6667},
    {"name": "Recoleta", "lat": -33.4077, "lon": -70.6389},
    {"name": "Quilicura", "lat": -33.3569, "lon": -70.7250},
    {"name": "Renca", "lat": -33.4017, "lon": -70.7067},
    {"name": "Colina", "lat": -33.2000, "lon": -70.6750},
    {"name": "San Joaquín", "lat": -33.4935, "lon": -70.6236},
    {"name": "La Granja", "lat": -33.5383, "lon": -70.6236},
    {"name": "La Pintana", "lat": -33.5831, "lon": -70.6340},
    {"name": "San Ramón", "lat": -33.5417, "lon": -70.6456},
    {"name": "El Bosque", "lat": -33.5667, "lon": -70.6750},
    {"name": "San Bernardo", "lat": -33.5921, "lon": -70.7000},
    {"name": "Puente Alto", "lat": -33.6119, "lon": -70.5756},
    {"name": "La Florida", "lat": -33.5524, "lon": -70.5976},
    {"name": "Cerrillos", "lat": -33.4833, "lon": -70.7000},
    {"name": "Estación Central", "lat": -33.4653, "lon": -70.7022},
    {"name": "Quinta Normal", "lat": -33.4425, "lon": -70.7019},
    {"name": "Lo Prado", "lat": -33.4325, "lon": -70.7280},
    {"name": "Pudahuel", "lat": -33.4275, "lon": -70.7667},
    {"name": "Cerro Navia", "lat": -33.4228, "lon": -70.7203},
    {"name": "Maipú", "lat": -33.5167, "lon": -70.7667}
]

def get_nearest_comuna(lat, lon):
    """Encuentra la comuna más cercana a las coordenadas dadas"""
    if not lat or not lon:
        return "Región Metropolitana"
        
    min_distance = float('inf')
    nearest_comuna = "Región Metropolitana"
    
    for comuna in COMUNAS_RM:
        # Cálculo simple de distancia euclidiana (suficiente para comparación)
        distance = ((comuna["lat"] - lat) ** 2 + (comuna["lon"] - lon) ** 2) ** 0.5
        if distance < min_distance:
            min_distance = distance
            nearest_comuna = comuna["name"]
    
    return nearest_comuna

def map_type(waze_type):
    """Mapea tipos de Waze a nuestras categorías"""
    type_mapping = {
        "ACCIDENT": "accident",
        "JAM": "traffic_jam",
        "WEATHERHAZARD": "hazard",
        "HAZARD": "hazard",
        "ROAD_CLOSED": "road_closed",
        "CONSTRUCTION": "road_closed",
        "MISC": "hazard",
        "POLICE": "police"
    }
    
    # Normalizar a mayúsculas para la búsqueda
    normalized_type = waze_type.upper() if isinstance(waze_type, str) else ""
    
    # Devolver el tipo mapeado o "hazard" por defecto
    return type_mapping.get(normalized_type, "hazard")

def save_to_file(events):
    """Guarda los eventos en un archivo JSON con verificación de UUID"""
    if not events:
        logger.warning("No hay eventos para guardar")
        return
    
    try:
        # Verificar que todos los eventos tienen UUID
        for event in events:
            if "uuid" not in event:
                logger.warning(f"Evento sin UUID detectado, asignando uno: {event.get('description', 'Sin descripción')}")
                event["uuid"] = get_unique_uuid()
        
        # Crear directorio si no existe
        os.makedirs('/data', exist_ok=True)
        
        # Crear nombre de archivo con timestamp
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"/data/events_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(events, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Guardados {len(events)} eventos en {filename}")
        
    except Exception as e:
        logger.error(f"Error guardando archivo: {e}")

def get_comunas_distribution(events):
    """Obtiene la distribución de comunas en los eventos"""
    comunas = {}
    for event in events:
        comuna = event.get("location_desc", "Desconocido")
        comunas[comuna] = comunas.get(comuna, 0) + 1
    
    # Ordenar por frecuencia
    sorted_comunas = sorted(comunas.items(), key=lambda x: x[1], reverse=True)
    return sorted_comunas

def main():
    """Función principal del scraper"""
    logger.info("Iniciando servicio de obtención de datos de tráfico para la Región Metropolitana con bounding box")
    
    while True:
        try:
            all_events = []
            
            # Obtener datos de cada cuadrante de la región
            for quadrant in RM_QUADRANTS:
                events = get_traffic_data_for_quadrant(quadrant)
                if events:
                    all_events.extend(events)
            
            # Si no se obtuvieron eventos reales, registrar un error y continuar
            if not all_events:
                logger.error("No se obtuvieron datos reales en este ciclo.")
                continue
            
            # Verificar que cada evento tenga UUID, pero no sobrescribir los de Waze
            for event in all_events:
                if "uuid" not in event:
                    event["uuid"] = str(uuid.uuid4())
                
                # Eliminar campos no deseados si existen
                if "subtype" in event:
                    del event["subtype"]
                if "report_count" in event:
                    del event["report_count"]
            
            # Guardar los eventos en un archivo
            save_to_file(all_events)
            
            # Esperar para el próximo ciclo (entre 5 y 10 minutos)
            wait_time = random.randint(300, 600)
            logger.info(f"Esperando {wait_time} segundos hasta el próximo ciclo...")
            time.sleep(wait_time)
        
        except Exception as e:
            logger.error(f"Error en el ciclo principal: {e}")
            logger.info("Reintentando en 60 segundos...")
            time.sleep(60)

if __name__ == "__main__":
    main()