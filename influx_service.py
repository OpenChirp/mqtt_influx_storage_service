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
#import requests_cache
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

        self.subTopics = set()
        
        self.client.loop_start()


    def on_connect(self, client, userdata, flags, rc):
        
        threading.current_thread().setName("MqttClient")
        
        logging.info('Connected to mqtt broker with result code '+str(rc))
        for topic in self.subTopics:
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

    def subscribe(self, topic):
        self.subTopics.add(topic)
        self.client.subscribe(topic)


# Define stop action
def signal_handler(signal, frame):
    logging.info('Received kill signal ..Stopping service daemon')

    global running

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
    logging.info("Got message " + str(msg.topic))

    if msg.topic == events_topic:
        process_event(msg.payload)
        return

    words = msg.topic.split('/')

    # ignore non transducer data
    if(len(words) < 5):
        logging.info("Skipping message: " + str(msg.topic)+" : "+str(msg.payload))
        return


    elif "transducer" not in str(words[3]):
        logging.error(words[3])
        logging.info("Skipping non transducer message: " + str(msg.topic)+" : "+str(msg.payload))
        return

    # get device info
    device_id = words[2]
    with devices_lock:
        if device_id not in devices.keys():
            logging.info("Device "+str(device_id)+" is not liked to the storage service, skipping...")
            return

    # check if the payload is a number or float
    #try:
    #	float(str(msg.payload, "utf-8"))
    #except ValueError:
    #    logging.info("Ignoring message, payload not a number or float: ["+str(msg.payload)+"]")
    #    return

    # Try to parse a Float or Boolean

    # Influx Notes:
    # * Influx must be able to discern a Float64, Int64, String, or Boolean value.
    # * Influx will assume float for a number if not explicitly specified.
    # * If the value is quoted, this signals to Influx to save it as a string.
    #
    # Implementation Notes:
    # * We will discern Numbers, Booleans, and String/Binary incoming types.
    # * We will only try to parse numbers as floats, since people and services
    #   may flip between integers and floats without care.
    # * We rely on the dict to JSON marshaller to produce a natural string or
    #   a base64 string from binary data.

    value = None

    # could be integer or float to succeed
    try:
        value = float(str(msg.payload, "utf-8"))
        logging.debug("Parsed payload as float "+str(value)+": ["+str(msg.payload)+"]")
    except ValueError:
        logging.debug("Failed to parse payload as a float: ["+str(msg.payload)+"]")

    if value == None:
        if msg.payload in ['true', 'True']:
            value = True
            logging.debug("Parsed payload as a boolean True: ["+str(msg.payload)+"]")
        elif msg.payload in ['false', 'False']:
            value = False
            logging.debug("Parsed payload as a boolean False: ["+str(msg.payload)+"]")
        else:
            value = msg.payload
            logging.debug("Failed to parse payload as a number or boolean: ["+str(msg.payload)+"]")

    transducer_name = words[4].lower()
    init_transducer(device_id, transducer_name)
    point = {
        'measurement': device_id+"_"+transducer_name,
        'time': timestamp, # time in UTC
        'fields': {
            'value': value
            }
    }

    organized_point = {
        'measurement': device_id,
        'time': timestamp, # time in UTC
        'fields': {
            transducer_name: value
            }
    }

    try:
        success = influx_client.write_points([point, organized_point], time_precision='n', protocol = 'json')
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
def init_transducer(device_id, transducer_name):
    
    with devices_lock:
        if device_id in devices.keys() and transducer_name in devices[device_id]:
            return

    # Check if transducer was just created
    updated_device = get_device(device_id)
    tl = set()
    for item in updated_device["transducers"]:
        tl.add(item['name'])
    with devices_lock:
        devices[device_id] = tl
        if transducer_name in devices[device_id]:
            return

    url = str(conf['rest_url'] + "/device/"+device_id+"/transducer")

    logging.debug("About to create non existent transducer "+transducer_name +
        " on device " + device_id)    
    data = {}
    data['name'] = transducer_name;
    data['properties'] = {"created_by" : "OpenChirp Influxdb Storage service"} 
    try:
        response = requests.post(url, data = data, auth = auth_cred)
        if(response.ok):
            logging.info("New Transducer created")
            with devices_lock:
                devices[device_id].add(transducer_name)
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

            #if res.from_cache:
            #    logging.debug("Using Cached HTTP Query Content")
            #else:
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
        with devices_lock:
            d = len(devices)
        status["message"] = "Points written 10 min avg : {} / #Devices: {}".format(
            str(points_temp), str(d))
        mqtt_client.publish(status_topic, json.dumps(status))
        with points_lock:
            pointsWritten -= points_temp

def process_event(payload):
    e = json.loads(str(payload, "utf-8"))
    a = str(e['action'])
    id = str(e['thing']['id'])
    logging.info("Service Event: {} device {}".format(str(a), str(id)))

    if a in ['new', 'update']:
        d = get_device(str(id))
        tl = set()
        for item in d["transducers"]:
            tl.add(item['name'])
        with devices_lock:
            devices[id] = tl
        logging.info("Loaded {} transducers for {}".format(str(len(tl)), str(id)))

    elif a == 'delete':
        with devices_lock:
            if id in devices.keys():
                del devices[id]
    
def load_devices():
    url = things_url
    transducers_loaded = 0
    try:
        res = requests.get(url, auth = auth_cred)
        if(res.ok):
            things = res.json()
            for t in things:
                d = get_device(str(t['id']))
                tl = set()
                for item in d["transducers"]:
                    tl.add(item['name'])
                    transducers_loaded += 1
                with devices_lock:
                    devices[t['id']] = tl
            with devices_lock:
                logging.info('Loaded {} devices and {} transducers'.format(
                    len(devices), transducers_loaded))                     
            return True
        else:
            logging.error("Error response ["+str(res.status_code)+"]")

    except ConnectionError as ce:
        logging.error("Connection error : " + url)
        logging.exception(ce)
            
    except requests.exceptions.RequestException as re:
        logging.error("RequestException :" + url)
        logging.exception(re)

    except Exception as e:
        logging.exception(e)



# Global variables

NUM_THREAD_WORKERS = 2
PUBLISH_STATS_INTERVAL = 600    # 10 minutes
INFLUX_DATABASE = 'openchirp'

running = True
conf = common.parse_arguments() # read configuration parameters
status_topic = 'openchirp/services/'+ str(conf['service_id']) +'/status'
events_topic = 'openchirp/services/'+ str(conf['service_id']) +'/thing/events'
transducers_topic = 'openchirp/devices/+/transducer/#'
things_url = str(conf['rest_url'] + '/service/' + conf['service_id'] + '/things')

auth_cred = HTTPBasicAuth(conf['service_id'], conf['password'])

publish_stats_daemon = threading.Thread(name='StatsTimer', target=publish_status, daemon=True)

# user in memory device defenition storage
devices = dict()
pointsWritten = 0   # keeps track of the points written in the last 10 minutes

points_lock = threading.Lock()
devices_lock = threading.Lock()



# Change Thread name in Python 3.6+
if sys.version_info.major >= 3 and sys.version_info.minor > 5:
    ex = ThreadPoolExecutor(max_workers=NUM_THREAD_WORKERS, thread_name_prefix="Consumer")
else:
    ex = ThreadPoolExecutor(max_workers=NUM_THREAD_WORKERS)

# set logging configurations
common.configure_logging(conf)

# create Influx client
influx_client = influxdb.InfluxDBClient(conf['influxdb_host'], conf['influxdb_port'],
    conf['influxdb_user'], conf['influxdb_password'], INFLUX_DATABASE)

# start mqtt client
mqtt_client = MqttClient(host=conf['mqtt_broker'], port='1883', client_id=conf['client_id'],
    service_id=conf['service_id'], password=conf['password'], ssl_location=conf['ssl_location'])

mqtt_client.subscribe(events_topic) # start processing service changes right away

# load Devices using the Rest API
logging.info('Loading Device Information ...')
if load_devices() != True:
    logging.fatal('Error Loading Devices')
    exit()

mqtt_client.subscribe(transducers_topic) # processing transducer requests

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


