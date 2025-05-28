import json
import time
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
import os 
load_dotenv()

BROKER = "100.78.181.75"
PORT = 1883
USERNAME =  os.getenv("MQTT_USERNAME")
PASSWORD = os.getenv("MQTT_PASSWORD") 

# Callback connection
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(" Connected to broker MQTT")
        # Iscrizione ai topic downlink
        client.subscribe("downlink/gateway")
        client.subscribe("downlink/cu")
        client.subscribe("downlink/mu")
    else:
        print(" Connection  failed, error code:", rc)

# Callback recived message
def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode()
    print(f"from {topic}: {payload}")

    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        print("Error, message not JSON")
        return

    # Mapping topic downlink → uplink
    if topic == "downlink/gateway":
        uplink_topic = "uplink/gateway"
    elif topic == "downlink/cu":
        uplink_topic = "uplink/cu"
    elif topic == "downlink/mu":
        uplink_topic = "uplink/mu"
    else:
        print("Error not available Topic")
        return

    # Here there is the edit of the response 
    response = {
        "status": "ack",
        "received": data
    }

    # send topic
    message = json.dumps(response)
    client.publish(uplink_topic, message, qos=1, retain=False)
    print(f"Sent to {uplink_topic}: {message}")

# MQTT init
client = mqtt.Client()
client.username_pw_set(USERNAME, PASSWORD)
client.on_connect = on_connect
client.on_message = on_message

#  loop
client.connect(BROKER, PORT, 60)
client.loop_forever()
