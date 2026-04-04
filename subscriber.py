import paho.mqtt.client as mqtt

BROKER = "localhost"
PORT = 1883
TOPICS = ["temp_topic", "level_topic"]


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to broker")
        for topic in TOPICS:
            client.subscribe(topic)
            print(f"Subscribed to: {topic}")
    else:
        print(f"Connection failed with code {rc}")


def on_message(client, userdata, msg):
    print(f"[{msg.topic}] {msg.payload.decode()}")


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT)
client.loop_forever()
