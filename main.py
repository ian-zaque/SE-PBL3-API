import paho.mqtt.client as mqtt
import datetime
from pymongo import MongoClient
import uuid
import base64
import pywhatkit as kit
import os
import asyncio

# Configurações do broker com autenticação
BROKER = "192.168.204.101"      # Altere para o endereço do seu broker
PORT = 1883                    # Porta do broker (normalmente 1883 para conexões sem TLS)
TOPICS = ["/temperature", "/humidity", "/luminosity", "/gas", "/environment", "/control", "/photo"]
CLIENT_ID = "API_CLIENT"  # ID único do cliente
USERNAME = "LUCAS"         # Nome de usuário fornecido pelo broker
PASSWORD = "3301"           # Senha correspondente
DATA = { "environment": None, "luminosity": None, "humidity": None, "gas": None, "temperature": None, "timestamp": None, "_id": None, "photo": None, }

# Database configuration
DB_CONNECTION = MongoClient("mongodb://localhost:27017/")
DATABASE = DB_CONNECTION["database"]
COLLECTION = DATABASE["sensors"]

# File Configuration
FILEPATH_IMG = os.path.dirname(os.path.abspath(__file__)) + "/decoded_image.jpg"
BASE64_IMG = ""
OUTPUT_IMG = None

# Whatsapp Configuration
PHONE = "+5575988060006"
ZAP_MESSAGE = ""

# Função chamada ao conectar
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Inscrito nos tópicos:")
        for topico in TOPICS:
            client.subscribe(topico)
            print(f"{topico}")
    else:
        print(f"Erro na conexão. Código de retorno: {rc}")

# Função chamada quando uma mensagem é recebida
def on_message(client, userdata, msg):
    # clear_data()
    message = msg.payload.decode()
    topic = msg.topic

    if message:
        if 'luminosity' in topic:
            DATA['luminosity'] = message
        elif 'humidity' in topic:
            DATA['humidity'] = message
        elif 'gas' in topic:
            DATA['gas'] = message
        elif 'temperature' in topic:
            DATA['temperature'] = message
        elif 'environment' in topic:
            DATA['environment'] = message
        elif 'photo' in topic:
            BASE64_IMG = message
            DATA['photo'] = message
        elif 'control' in topic:
            print(f"Parada Solicitada!!!")

        if((DATA['luminosity'] != None) and (DATA['humidity'] != None) and (DATA['gas'] != None) and (DATA['temperature'] != None) and (DATA['environment'] != None)):
            DATA['timestamp'] = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
            DATA['_id'] = str(uuid.uuid4())
            print(f"Dados formatados: ", DATA)
            db_put()

def db_put():
    last_record = DATA.copy()
    last_record.pop("photo")
    COLLECTION.insert_one(last_record)  # Salva uma cópia dos dados atuais
    last_record = COLLECTION.find_one(sort=[("timestamp", -1)])

    if (DATA['photo'] != None):
        send_zap(last_record)

    clear_data()

def clear_data():
    # Limpa os campos para evitar duplicatas baseadas em dados antigos
    for key in ["luminosity", "humidity", "gas", "temperature", "environment", "photo"]:
        DATA[key] = None

def send_zap(item):
    ZAP_MESSAGE = f"*Ambiente*: {item['environment']}\n *Horário*: {item['timestamp']}\n *Luminosidade*: {item['luminosity']}\n *Umidade*:{item['humidity']}\n *Temperatura*: {item['temperature']}\n *Gás*: {item['gas']}"
    
    with open(FILEPATH_IMG, 'wb') as OUTPUT_IMG:
        OUTPUT_IMG.write(base64.b64decode(DATA['photo']))

    kit.sendwhats_image(receiver=PHONE, img_path=FILEPATH_IMG, caption=ZAP_MESSAGE, wait_time=45, tab_close=True)

def connect_to_broker():
    # Cria o cliente MQTT com client_id
    client = mqtt.Client(client_id=CLIENT_ID)

    # Define as credenciais de login
    client.username_pw_set(USERNAME, PASSWORD)

    # Define as funções de callback
    client.on_connect = on_connect
    client.on_message = on_message

    # Conecta ao broker
    client.connect(BROKER, PORT, keepalive=60)

    # Inicia o loop de escuta
    client.loop_forever()

connect_to_broker()