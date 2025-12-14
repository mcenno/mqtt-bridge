#!/usr/bin/env python
# -*- coding: utf-8 -*-

# forwarder.py - forwards IoT sensor data from MQTT to InfluxDB
#
# Copyright (C) 2016 Michael Haas <haas@computerlinguist.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software Foundation,
# Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA

import argparse
import json
import logging
import sys

import influxdb_client
import paho.mqtt.client as mqtt
import requests.exceptions
from influxdb_client.client.write_api import SYNCHRONOUS

bucket = "home"
org = "docs"
token = "askjuerztbqi3u65bfcocazugab=="
# Store the URL of your InfluxDB instance
url = "http://influxdb2:8086"


class MessageStore(object):
    def store_msg(self, node_name, measurement_name, value):
        raise NotImplementedError()


class InfluxStore(MessageStore):
    logger = logging.getLogger("forwarder.InfluxStore")

    def __init__(self, host, port, username, password_file, database):
        # password = open(password_file).read().strip()
        #        self.influx_client = InfluxDBClient(
        #            host=host, port=port, username=username, password=password, database=database)
        self.influx_client = influxdb_client.InfluxDBClient(
            url=url, token=token, org=org
        )
        self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)
        # influx_client.create_database('sensors')

    def store_msg(self, node_name, measurement_name, data):
        if not isinstance(data, dict):
            raise ValueError("data must be given as dict!")
        influx_msg = {
            "measurement": measurement_name,
            "tags": {
                "sensor_node": node_name,
            },
            "fields": data,
        }
        self.logger.debug("Writing InfluxDB point: %s", influx_msg)
        try:
            # self.influx_client.write_points([influx_msg])
            dict_structure = {}
            dict_structure["measurement"] = "my_measurement"
            dict_structure["location"] = "Bochum"
            dict_structure["tags"] = {"device": node_name}
            dict_structure["fields"] = data

            p = influxdb_client.Point.from_dict(dict_structure)
            self.write_api.write(bucket=bucket, org=org, record=p)

        except (requests.exceptions.ConnectionError, ValueError) as e:
            self.logger.exception(e)


class MessageSource(object):
    def register_store(self, store):
        if not hasattr(self, "_stores"):
            self._stores = []
        self._stores.append(store)

    @property
    def stores(self):
        # return copy
        return list(self._stores)


class MQTTSource(MessageSource):
    logger = logging.getLogger("forwarder.MQTTSource")

    def __init__(self, host, port, node_names, stringify_values_for_measurements):
        self.host = host
        self.port = port
        self.node_names = node_names
        self.stringify = stringify_values_for_measurements
        self._setup_handlers()

    def _setup_handlers(self):
        self.client = mqtt.Client()

        def on_connect(client, userdata, flags, rc):
            self.logger.info("Connected with result code  %s", rc)
            # subscribe to /node_name/wildcard
            for node_name in self.node_names:
                topic = f"{node_name}/#".format(node_name=node_name)
                self.logger.info(
                    "Subscribing to topic %s for node_name %s", topic, node_name
                )
                client.subscribe(topic)

        def on_message(client, userdata, msg):
            self.logger.debug(
                "Received MQTT message for topic %s with payload %s",
                msg.topic,
                msg.payload,
            )
            tasmota_simple_tokens = [
                "Z1_curr_w",
                "Z1_total_kwh",
                "Z1_total_kwh_out",
                "Z3_curr_w",
                "Z3_total_kwh",
                "Z3_total_kwh_out",
            ]
            simple_tokens = ["fhz", "wh", "car", "amp", "frc"] + tasmota_simple_tokens
            complex_tokens = ["nrg", "isv"]
            tokens = simple_tokens + complex_tokens + tasmota_simple_tokens

            node_name, measurement_name = msg.topic.rsplit("/", 1)
            if measurement_name not in tokens:
                return

            value = msg.payload
            self.logger.debug("Processing measurement %s with value %s", measurement_name, value)
            if measurement_name == "isv":
                json_data = {
                    f"{code}_{i + 1}": float(subdict[code])
                    for i, subdict in enumerate(json.loads(value))
                    for code in ["i", "p", "f"]
                }
            if measurement_name == "nrg":
                json_data = dict(
                    zip(
                        [
                            "U_L1",
                            "U_L2",
                            "U_L3",
                            "U_N",
                            "I_L1",
                            "I_L2",
                            "I_L3",
                            "P_L1",
                            "P_L2",
                            "P_L3",
                            "P_N",
                            "P_Total",
                            "pf_L1",
                            "pf_L2",
                            "pf_L3",
                            "pf_N",
                        ],
                        list(json.loads(value)),
                    )
                )
                # need to convert all values to floats
                json_data = {key: float(value) for key, value in json_data.items()}
                # add number of active phases
                json_data["n_phases"] = float(
                    len(
                        [
                            val
                            for key, val in json_data.items()
                            if key.startswith("P_L") and val > 500.0
                        ]
                    )
                )
            if measurement_name in simple_tokens:
                json_data = {measurement_name: float(msg.payload)}

            for store in self.stores:
                store.store_msg(node_name, measurement_name, json_data)

        self.client.on_connect = on_connect
        self.client.on_message = on_message

    def start(self):
        self.client.connect(self.host, self.port)
        # Blocking call that processes network traffic, dispatches callbacks and
        # handles reconnecting.
        # Other loop*() functions are available that give a threaded interface and a
        # manual interface.
        self.client.loop_forever()


def main():
    parser = argparse.ArgumentParser(
        description="MQTT to InfluxDB bridge for IOT data."
    )
    parser.add_argument("--mqtt-host", required=True, help="MQTT host")
    parser.add_argument("--mqtt-port", default=1883, help="MQTT port")
    parser.add_argument("--influx-host", required=True, help="InfluxDB host")
    parser.add_argument("--influx-port", default=8086, help="InfluxDB port")
    parser.add_argument("--influx-user", required=True, help="InfluxDB username")
    parser.add_argument(
        "--influx-pass-file", required=True, help="InfluxDB password file"
    )
    parser.add_argument("--influx-db", required=True, help="InfluxDB database")
    parser.add_argument(
        "--node-name", required=True, help="Sensor node name", action="append"
    )
    parser.add_argument(
        "--stringify-values-for-measurements",
        required=False,
        help="Force str() on measurements of the given name",
        action="append",
    )
    parser.add_argument(
        "--verbose",
        help="Enable verbose output to stdout",
        default=False,
        action="store_true",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    else:
        logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    store = InfluxStore(
        host=args.influx_host,
        port=args.influx_port,
        username=args.influx_user,
        password_file=args.influx_pass_file,
        database=args.influx_db,
    )
    source = MQTTSource(
        host=args.mqtt_host,
        port=args.mqtt_port,
        node_names=args.node_name,
        stringify_values_for_measurements=args.stringify_values_for_measurements,
    )
    source.register_store(store)

    while True:
        try:
            print("Forwarder wird gestartet")
            source.start()
        except Exception as e:
            print(f"Fehler abgefangen: {e}")


if __name__ == "__main__":
    main()
