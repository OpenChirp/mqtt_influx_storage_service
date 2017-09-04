#!/usr/bin/env python3

################################################################################
#
#  Copyright (C) 2017, Carnegie Mellon University
#  All rights reserved.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, version 2.0 of the License.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#  Contributing Authors (specific to this file):
#
#   Artur Balanuta                  arturb[at]andrew[dot]edu
#   Khushboo Bhatia                 khush[at]cmu[dot]edu
#
################################################################################

import datetime
import json
import logging
import queue
import signal
import ssl
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor

import paho.mqtt.client as mqtt

import common
import requests
import requests_cache
import influxdb
from requests.auth import HTTPBasicAuth

class MqttClient():

    def __init__(self, host, port, client_id, service_id, password,
        ssl_location):
        
        logging.info('MqttClient : Init')
        self.client = mqtt.Client(client_id)
        self.queue = queue.Queue()
        
        self.client.username_pw_set(service_id, password)
        self.client.tls_set(ssl_location, tls_version = ssl.PROTOCOL_TLSv1)
        self.client.connect(host, int(port), 60)

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        
        self.client.loop_start()


    def on_connect(self, client, userdata, flags, rc):
        
        threading.current_thread().setName("MqttClient")
        
        logging.info('Connected to mqtt broker with result code '+str(rc))
        for topic in TOPICS:
            logging.info('Subscibing to mqtt topic ' + topic)
            client.subscribe(topic)

    def on_message(self, client, userdata, msg):
        logging.debug("Got mesage from topic: " + str(msg.topic))
        self.queue.put( (datetime.datetime.utcnow(), msg) )

    def get_queue(self):
        return self.queue

    def publish(self, topic, payload ):
        logging.info('MqttClient: publishing message '+topic+':' + payload )
        #self.client.publish(msg["topic"], msg["message"], retain = True)
        self.client.publish(topic, payload)

    def stop(self):
        logging.info('Disconnecting MqttClient ...')
        self.client.loop_stop()
        logging.info('MqttClient Disconected')


# Define stop action
def signal_handler(signal, frame):
    logging.info('Received kill signal ..Stopping service daemon')

    global running
    global publish_stats_timer

    running = False

    if "mqtt_client" in globals():
        mqtt_client.stop()


signal.signal(signal.SIGINT, signal_handler)

# Wrapper function to catch Thread ExceptionS
def store_message_wrapper(msg, timestamp):
    try:
        store_message(msg, timestamp)
    except Exception as e:
        logging.exception("Error in storing message: "+str(e))

# Stores the mqtt message into influxdb
def store_message(msg, timestamp):
    logging.info("Storing message " + str(msg.topic))
    words = msg.topic.split('/')

    # ignore non transducer data
    if(len(words) < 5):
        logging.info("Skipping message: " + str(msg.topic)+" : "+str(msg.payload))
        return

    # check if the payload is a number or float
    try:
    	float(str(msg.payload, "utf-8"))
    except ValueError:
        logging.info("Ignoring message, payload not a number or float: ["+str(msg.payload)+"]")
        return

    device_id = words[2]
    device = get_device(device_id)

    if device is None:
        logging.info("Skipping message "+str(msg)+". Could not get a device with id :" + device_id)
        return

    transducer_name = words[4].lower()
    get_or_create_transducer(device, transducer_name)
    point = {
        'measurement': device_id+"_"+transducer_name,
        'time': timestamp, # time in UTC
        'fields': {
            'value': msg.payload
            }
    }

    try:
        success = influx_client.write_points([point], time_precision='n', protocol = 'json')
        if success:
            logging.debug("Point Saved in InfluxDB")
        else:
            logging.critical("Error pushing point to InfluxDB")
        global pointsWritten
        with points_lock:
            pointsWritten += 1
            logging.debug("Points written: "+str(pointsWritten))

    except influxdb.exceptions.InfluxDBClientError as ie:
        logging.error("Error in writing to influxdb")
        logging.error(ie)
    
# This method creates the transducer if it does not exist
def get_or_create_transducer(device, transducer_name):
    
    deviceId = device["id"]
    transducers = device["transducers"]

    for item in transducers:
        if(item["name"].lower() == transducer_name):
            logging.debug("Found transducer: "+ str(transducer_name) +
                " has content: "+str(item))
            return item

    logging.info("Transducer not found in device properties, reloading device transducers...")
    
    url = str(conf['rest_url'] + "/device/"+device["id"]+"/transducer")

    # Reload transducers before creating one
    try:
        with requests_cache.disabled():
            response = requests.get(url, auth = auth_cred)

        if(response.ok):
            transducers = response.json()
            for item in transducers:
                if(item["name"].lower() == transducer_name):
                    logging.debug("Found transducer: "+ str(transducer_name)+
                        " contains: " + str(item))
                    return item
        else:
            logging.error("Error in getting transducers " +
                str(response.status_code))
            return

    except ConnectionError as ce:
        logging.error("Connection error :" + url)
        logging.exception(ce)
        return

    except requests.exceptions.RequestException as re:
        logging.error("RequestsException :" + url)
        logging.exception(re)
        return

    logging.info("About to create non existent transducer "+transducer_name +
        " on device " + device["id"])    
    data = {}
    data['name'] = transducer_name;
    data['properties'] = {"created_by" : "OpenChirp Influxdb Storage service"} 
    try:
        response = requests.post(url, data = data, auth = auth_cred)
        if(response.ok):
            logging.info("Transducer created")
        else:
            logging.error("Error in creating transducer : HTTP code : "+str(response.status_code) +
                " Content : "+str(response.json()))

    except ConnectionError as ce:
            logging.error("Connection error : " + url)
            logging.exception(ce)
            return

    except requests.exceptions.RequestException as re:
            logging.error("RequestException : "+ url)
            logging.exception(re)
            return

def get_device(device_id):
    url = str(conf['rest_url'] + "/device/"+ device_id)
    logging.info("GET "+url)

    try:
        res = requests.get(url, auth = auth_cred)
        if(res.ok):
            device = res.json()

            if res.from_cache:
                logging.debug("Using Cached HTTP Query Content")
            else:
                logging.debug("Got New Content: "+str(device))

            return device
        
        else:
            logging.error("Error response ["+str(res.status_code)+"]")
            return

    except ConnectionError as ce:
        logging.error("Connection error : " + url)
        logging.exception(ce)
        return 
            
    except requests.exceptions.RequestException as re:
        logging.error("RequestException :" + url)
        logging.exception(re)
        return

    except Exception as e:
        logging.exception(e)


def publish_status():
    global pointsWritten
    while True:
        time.sleep(PUBLISH_STATS_INTERVAL)
        points_temp = pointsWritten
        status = dict()
        status["message"] = "Points written in the last 10 minutes : "+str(points_temp)
        mqtt_client.publish(status_topic, json.dumps(status))
        with points_lock:
            pointsWritten -= points_temp
        
# Global variables

NUM_THREAD_WORKERS = 1
PUBLISH_STATS_INTERVAL = 600    # 10 minutes
TOPICS = ['openchirp/devices/+/transducer/#']
INFLUX_DATABASE = 'openchirp'

running = True
conf = common.parse_arguments() # read configuration parameters
pointsWritten = 0   # keeps track of the points written in the last 10 minutes
status_topic = 'openchirp/services/'+ str(conf['service_id']) +'/status'
auth_cred = HTTPBasicAuth(conf['service_id'], conf['password'])
points_lock = threading.Lock()
publish_stats_daemon = threading.Thread(name='StatsTimer', target=publish_status, daemon=True)

ex = ThreadPoolExecutor(max_workers=NUM_THREAD_WORKERS)

# set logging configurations
common.configure_logging(conf)

# set the HTTP reqests cache timeout to 5 minutes and clear in memory cache
requests_cache.install_cache(expire_after=300)
requests_cache.clear()

# create Influx client
influx_client = influxdb.InfluxDBClient(conf['influxdb_host'], conf['influxdb_port'],
    conf['influxdb_user'], conf['influxdb_password'], INFLUX_DATABASE)

# start mqtt client
mqtt_client = MqttClient(host=conf['mqtt_broker'], port='1883', client_id=conf['client_id'],
    service_id=conf['service_id'], password=conf['password'], ssl_location=conf['ssl_location'])

logging.info("Starting ...")
publish_stats_daemon.start()
while running:
    try:
        timestamp, msg = mqtt_client.get_queue().get(timeout=.5)
        #logging.debug("Got " + str(msg) + " at " + str(timestamp))
        ex.submit(store_message_wrapper, msg, timestamp)
        logging.debug("New work submitted")
    except queue.Empty:
        continue
    except:
        logging.error("Unable to process message from the queue")
        continue
