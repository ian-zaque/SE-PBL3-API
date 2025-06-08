import paho.mqtt.client as mqtt
import datetime
from pymongo import MongoClient
import json
import uuid

# Configurações do broker com autenticação
BROKER = "192.168.62.7"      # Altere para o endereço do seu broker
PORT = 1883                    # Porta do broker (normalmente 1883 para conexões sem TLS)
TOPICS = ["/temperature", "/humidity", "/luminosity", "/gas", "/environment", "/control"]
CLIENT_ID = "API_CLIENT"  # ID único do cliente
USERNAME = "LUCAS"         # Nome de usuário fornecido pelo broker
PASSWORD = "3301"           # Senha correspondente
DATA = { "environment": None, "luminosity": None, "humidity": None, "gas": None, "temperature": None, "timestamp": None, "_id": None }
STOP = False

DB_CONNECTION = MongoClient("mongodb://localhost:27017/")
DATABASE = DB_CONNECTION["database"]
COLLECTION = DATABASE["sensors"]

# Função chamada ao conectar
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("📡 Inscrito nos tópicos:")
        for topico in TOPICS:
            client.subscribe(topico)
            print(f"   • {topico}")
    else:
        print(f"Erro na conexão. Código de retorno: {rc}")

# Função chamada quando uma mensagem é recebida
def on_message(client, userdata, msg):
    # print(f"Mensagem recebida de {msg.topic}: {msg.payload.decode()}")
    message = msg.payload.decode()
    topic = msg.topic

    if message:
        if 'luminosity' in topic:
            DATA['luminosity'] = message
            # print(f"Luminosidade: ", message, type(DATA['luminosity']), len(DATA['luminosity']) > 0)
        elif 'humidity' in topic:
            # print(f"Umidade: ", message)
            DATA['humidity'] = message
        elif 'gas' in topic:
            # print(f"Gas: ", message)
            DATA['gas'] = message
        elif 'temperature' in topic:
            # print(f"Temperatura: ", message)
            DATA['temperature'] = message
        elif 'environment' in topic:
            DATA['environment'] = message
        elif 'control' in topic:
            print(f"Parada Solicitada!!!")

        if((DATA['luminosity'] != None) and (DATA['humidity'] != None) and (DATA['gas'] != None) and (DATA['temperature'] != None) and (DATA['environment'] != None)):
            DATA['timestamp'] = datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
            DATA['_id'] = str(uuid.uuid4())
            print(f"Dados formatados: ", DATA)
            db_put()

def db_put():
    data = COLLECTION.insert_one(DATA)

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
