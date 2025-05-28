import json
import time
import paho.mqtt.client as mqtt

BROKER = "172.20.0.50"
PORT = 1883
USERNAME = "measurestream"
PASSWORD = "measurestream"

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
    print(f"📤 Inviato a {uplink_topic}: {message}")

# MQTT init
client = mqtt.Client()
client.username_pw_set(USERNAME, PASSWORD)
client.on_connect = on_connect
client.on_message = on_message

#  loop
client.connect(BROKER, PORT, 60)
client.loop_forever()
