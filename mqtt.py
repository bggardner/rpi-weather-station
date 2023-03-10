#!/usr/bin/env python3
import argparse
import datetime
import time

import paho.mqtt.client as mqtt

import ws

data = {}
timestamp = None

def listener(readings: dict):
    global data, timestamp
    for key, reading in readings.items():
        if key not in data.keys():
            data[key] = ws.SensorReading(reading.value, timestamp=reading.timestamp, unit=reading.unit)
        elif data[key].value is not None and data[key].value is not None:
            if timestamp is None:
                timestamp = reading.timestamp
            elif timestamp < reading.timestamp:
                timestamp = reading.timestamp
            if reading.value == data[key].value:
                continue
            data[key].update(reading.value, reading.timestamp)
        client.publish(f"""ws/{key}""", data[key].value, retain=True)
        if timestamp is not None:
            client.publish("ws/dt", int(timestamp.timestamp()), retain=True)
            timestamp = timestamp + datetime.timedelta(seconds=1)

def on_connect(client, userdata, flags, rc):
    client.publish("ws/$state", "init", retain=True)
    w = ws.WeatherStation(listener)
    for key, reading in w.readings.items():
        client.publish(f"""ws/{key}/$unit""", reading.unit, retain=True)
    client.publish("ws/$state", "ready", retain=True)

    # Debugging only, remove later
    client.subscribe("ws/#")


def on_message(client, userdata, msg):
    print(f"{msg.topic}: {msg.payload.decode()}")


parser = argparse.ArgumentParser()
parser.add_argument("host", help="MQTT broker hostname or IP address")
parser.add_argument("-u", "--username", help="MQTT client username")
parser.add_argument("-P", "--password", help="MQTT client password")
args = parser.parse_args()

client = mqtt.Client()
if args.username is not None and args.password is not None:
    client.username_pw_set(args.username, args.password)
client.on_connect = on_connect
client.on_message = on_message
client.will_set("ws/$state", payload="lost", retain=True)
client.connect(args.host)
client.loop_forever()

#TODO Add other sensors
