# Proyecto final Deep Learning y Big Data

## Fernando Salazar Arguedas

## Objetivos:
- Crear un pipeline desde cero para capturar y almacenar datos en tiempo real usando herramientas de Big Data.
- Crear un pipeline de Machine Learning que use un modelo de Deep Learning para analizar el sentimiento de texto.
- Crear un dashboard donde se visualice y se analice los datos predichos por el modelo de Deep Learning.

## Descripcion:
Como proyecto de curso haremos una pequeña simulación en un ambiente local controlado donde usaremos herramientas open source de Big Data como Kafka y MongoDB para capturar y almacenar en tiempo real comentarios hechos por clientes para analizar y clasificar el sentimiento del comentario para su posterior análisis. La clasificación del texto se hará usando un modelo de Deep Learning proporcionado por el profesor, este obtendrá los datos capturados pendientes de análisis de la base de datos para hacer la predicción y guardar los datos y sus resultados en una base de datos extra que será consumida por un dashboard para su visualización y análisis.

El texto a analizar será simulando comentarios de Twitter hechos por el usuario.

## Estructura del proyecto
- `bots/`: Contiene los scripts de Python para el productor, consumidor y modelo de Deep Learning.
- `docker-compose.yml`: Archivo de configuración para levantar los servicios necesarios con Docker.
- `requirements.txt`: Lista de dependencias de Python necesarias para el proyecto.
- `README.md`: Este archivo con las instrucciones del proyecto.


## Instrucciones para montar y ejecutar el proyecto

Sigue estos pasos para configurar y ejecutar el proyecto en tu máquina local:

### 1. Requisitos previos
Asegúrate de tener instalados los siguientes componentes en tu sistema:
- **Python 3.8 o superior**: Para ejecutar los scripts del proyecto.
- **Docker u Orsbstack y Docker Compose**: Para levantar servicios como Kafka, MongoDB, MySQL y Meta.
- **Git**: Para clonar el repositorio si aún no lo has descargado.

### 2. Clonar el repositorio
Clona el repositorio en tu máquina local (si no lo has hecho ya):
```bash
git clone <https://github.com/FerSala/Deep_Learning_Big_D/tree/main/Proyecto_Final>
cd proyecto_final
```
### 3. Configurar el entorno virtual de Python
Crea y activa un entorno virtual para gestionar las dependencias del proyecto:
```bash
python -m venv venv
source venv/bin/activate  # En Windows usa: venv\Scripts\activate
```
### 4. Instalar dependencias
Instala las dependencias necesarias usando pip:
```bash
pip install -r requirements.txt
```
### 5. Configurar y levantar servicios con Docker
Asegúrate de tener Docker y Docker Compose instalados. Luego, en la raíz del proyecto, ejecuta:
```bash
docker-compose up -d
```
Esto levantará los servicios de Kafka, MongoDB, MySQL y Metabase necesarios para el proyecto.

### 6. Ejecutar los scripts de Python
Abre una terminal y navega a la carpeta del proyecto. Ejecuta los siguientes scripts en orden:
1. **Consumidor de Kafka**: Este script consume mensajes de Kafka y los almacena en MongoDB.
   ```bash
   python bots/consumer.py
   ```
2. **Productor de Kafka**: Este script simula la generación de comentarios y los envía a Kafka.
   ```bash
   python bots/client_1.py
   python bots/client_2.py
   python bots/client_3.py
   ```
3. **Modelo de Deep Learning**: Este script procesa los datos almacenados en MongoDB, realiza el análisis de sentimiento y almacena los resultados en MySQL.
   ```bash
   python bots/model.py
   ```
### 7. Acceder al dashboard de Metabase
Abre tu navegador web y accede a Metabase en la siguiente URL:
```
http://localhost:3000
```
Configura Metabase siguiendo las instrucciones en pantalla y conecta la base de datos MySQL para visualizar los datos analizados.
Utiliza las credenciales de MySQL definidas en el archivo `docker-compose.yml`:
- Usuario : proyecto
- Contraseña: deeplearning
- Nombre de la base de datos: mi_basedatos

### 8. Detener los servicios
Cuando hayas terminado de trabajar con el proyecto, puedes detener los servicios de Docker ejecutando:
```bash
docker-compose down
```
### Notas adicionales
- Asegúrate de que los puertos necesarios (9092 para Kafka, 27017 para MongoDB, 3306 para MySQL y 3000 para Metabase) estén libres en tu máquina.
- Si encuentras algún problema, revisa los logs de los contenedores de Docker para diagnosticar el problema:
```bash
docker-compose logs
```
### 9. Limpieza de datos en MySQL
El script `model.py` ahora incluye una función para limpiar la tabla de MySQL antes de subir nuevos datos, asegurando que no haya duplicados. Esto se realiza llamando al método `clean_table()` antes de subir los nuevos datos.

## Concliusiones

Este proyecto demuestra cómo integrar herramientas de Big Data con modelos de Deep Learning para crear un pipeline completo de captura, análisis y visualización de datos en tiempo real. 

Utilizando Kafka para la mensajería, MongoDB para el almacenamiento inicial y MySQL para los resultados del análisis, junto con un dashboard en Metabase, se puede obtener una solución robusta y escalable para el análisis de sentimientos en texto.