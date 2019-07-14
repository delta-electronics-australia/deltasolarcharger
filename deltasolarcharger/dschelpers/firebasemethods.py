"""Firebase Methods takes care of any communication to Firebase as well as any logging that needs to be done"""

import csv
import json
import os
import random
import sqlite3
import time
import requests
from datetime import datetime, timedelta
from ftplib import FTP, error_perm
from multiprocessing import Manager
from queue import Empty
from collections import deque, OrderedDict
from threading import Timer, Thread
import logging

import pyrebase
import websocket

from utils import log


class FactoryResetNotifier(Thread):
    def __init__(self, url):
        self.url = url
        self.ws = None
        super().__init__()

    def run(self):
        self.ws = websocket.WebSocketApp(self.url, on_message=self.on_message, on_error=self.on_error,
                                         on_close=self.on_close)
        self.ws.on_open = self.on_open
        self.ws.run_forever()

    def on_message(self, message):
        received_message = json.loads(message)
        log(received_message)

    def on_error(self, error):
        log(error)

    def on_open(self):
        pass

    def on_close(self):
        pass

    def send(self, data):
        try:
            self.ws.send(data)
        except BrokenPipeError as e:
            log(e)
            return False

        except websocket.WebSocketConnectionClosedException as e:
            log(e)
            return False

        return True

    def stop(self):
        log('Software Update stop signal received')
        self.ws.close()


class SoftwareUpdateNotifier(Thread):
    def __init__(self, url):
        self.url = url
        self.ws = None
        super().__init__()

    def run(self):
        self.ws = websocket.WebSocketApp(self.url, on_message=self.on_message, on_error=self.on_error,
                                         on_close=self.on_close)
        self.ws.on_open = self.on_open
        self.ws.run_forever()

    def on_message(self, message):
        received_message = json.loads(message)
        log(received_message)

    def on_error(self, error):
        log(error)

    def on_open(self):
        pass

    def on_close(self):
        pass

    def send(self, data):
        try:
            self.ws.send(data)
        except BrokenPipeError as e:
            log(e)
            return False

        except websocket.WebSocketConnectionClosedException as e:
            log(e)
            return False

        return True

    def stop(self):
        log('Software Update stop signal received')
        self.ws.close()


class OCPPDataBridge(Thread):
    def __init__(self, url, information_bus, charger_status_information_bus, ws_receiver_stopped_event):
        self.url = url
        self.information_bus = information_bus
        self.charger_status_information_bus = charger_status_information_bus
        self.ws_receiver_stopped_event = ws_receiver_stopped_event

        self.ws = None
        super().__init__()

    def run(self):
        self.ws = websocket.WebSocketApp(self.url,
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)
        self.ws.on_open = self.on_open
        log('Initialized OCPP ws')
        self.ws.run_forever()

    def send(self, data):
        try:
            # log('Sending data...', data, datetime.now())
            self.ws.send(data)
        except BrokenPipeError as e:
            log(e)
            self.ws_receiver_stopped_event.set()
            pass

        except websocket.WebSocketConnectionClosedException as e:
            log(e)
            self.ws_receiver_stopped_event.set()
            pass

    def on_message(self, message):
        """ This function will receive messages from the local OCPP backend and act accordingly"""
        received_message = json.loads(message)

        if 'charging_status' in received_message and received_message['chargerID'] is not None:
            # log("Received a Status update for", received_message['chargerID'])

            self.charger_status_information_bus.put(('charging_status_change',
                                                     {'charging_status': received_message["charging_status"],
                                                      'charging_timestamp': received_message['charging_timestamp'],
                                                      'meterValue': received_message['meterValue'],
                                                      'transaction_id': received_message['transaction_id']},
                                                     received_message['chargerID']))

        elif 'transaction_alert' in received_message:
            log("Received a transaction alert in OCPP ws", received_message)
            self.charger_status_information_bus.put(('transaction_alert',
                                                     {'charging_status': received_message["transaction_alert"],
                                                      'meterValue': received_message['meterValue'],
                                                      'charging_timestamp': received_message['charging_timestamp'],
                                                      'transaction_id': received_message['transaction_id']},
                                                     received_message['chargerID']))

        elif 'new_charger_info' in received_message:
            log("Received a BOOT NOTIFICATION in OCPP ws", received_message)
            self.information_bus.put(
                ('new_charger_info', received_message["new_charger_info"], received_message['chargerID']))

        elif 'fw_status' in received_message:
            log('Received a Firmware Status Notification in OCPP ws', received_message)
            self.information_bus.put(('fw_status', received_message['fw_status'], received_message['chargerID']))

        elif 'alive' in received_message:
            self.information_bus.put(('alive', received_message["alive"], received_message['chargerID']))

        elif 'meter_values' in received_message:
            self.information_bus.put(('meter_values', received_message["meter_values"], received_message['chargerID']))

        elif "authorize_request" in received_message:
            self.charger_status_information_bus.put(
                ('authorize_request', received_message['authorize_request'], received_message['chargerID']))

    def on_open(self):
        log('OCPP Websocket Receiver open')
        self.ws_receiver_stopped_event.clear()

    def on_close(self):
        log('OCPP Websocket Receiver closed')

    def on_error(self, error):
        """ Errors here will trigger the stopped event which will prompt update_external_sources to
         try to reconnect to WS """

        log('Got an error in ws!!', error)

        # This error will trigger when we try to send a message when it is closed
        if type(error) is websocket.WebSocketConnectionClosedException:
            log('We got a closed error, lets wait 2 seconds and connect again....')
            self.ws_receiver_stopped_event.set()

        if type(error) is ConnectionRefusedError:
            log('Connection refused!')
            self.ws_receiver_stopped_event.set()

    def stop(self):
        log('OCPP Websocket stop signal received')
        self.ws.close()


class FirebaseMethods:
    def __init__(self, firebase_to_analyse_queue, stdin_payload):
        super().__init__()

        self.logger = logging.getLogger()

        # Define our stdin variables
        self._ONLINE = stdin_payload['online']
        self._LIMIT_DATA = stdin_payload['LIMIT_DATA']
        log('ONLINE:' + str(self._ONLINE), 'LIMIT DATA: ' + str(self._LIMIT_DATA))

        # Define if we are in manual charge control
        self._MANUAL_CHARGE_CONTROL = False

        # Define our Firebase parameters
        self.firebase = None
        self.auth = None
        self.uid = None
        self.idToken = None
        self.refreshToken = None
        self.db = None

        # Define our Firebase listeners
        self.charging_modes_listener = None
        self.buffer_aggressiveness_listener = None
        self.update_firmware_listener = None
        self.dsc_firmware_update_listener = None
        self.delete_charger_listener = None
        self.factory_reset_listener = None
        self.manual_charge_control_listener = None
        self.misc_listener = None

        # Define our update_external_sources flags
        self.history_counter = 0
        self.webanalytics_counter = 0
        self.log_counter = 0

        # Define a variable to save our MODBUS data
        self.latest_modbus_data = dict()

        # Define the current selected charging mode (should be the same as analyzemethods variable)
        self._CHARGING_MODE = 'PV_with_BT'

        # Depending on whether or not we want to limit our data usage, we set our max counters
        if self._LIMIT_DATA:
            self.history_counter_max = 180
            self.log_charge_session_counter_max = 10
            self.webanalytics_counter_max = 900
        else:
            self.history_counter_max = 2
            self.log_charge_session_counter_max = 2
            self.webanalytics_counter_max = 20

        self.log_counter_max = 2

        # Define parameters for logging charge sessions
        self._charger_status_list = dict()

        self.today = datetime.now().day

        # Define our available current variable
        self._AVAILABLE_CURRENT = 6

        # Define a queue going from Firebase methods into analyze methods (for charging mode changes)
        self.firebase_to_analyse_queue = firebase_to_analyse_queue

        # Define a process manager and define our two information queues
        self.information_manager = Manager()
        self.information_bus = self.information_manager.Queue()
        self.charger_status_information_bus = Manager().Queue()
        self.ws_receiver_stopped_event = self.information_manager.Event()

        # Define a queue that will be used to pass the latest MODBUS data to other threads within FirebaseMethods
        self.modbus_data_queue = deque(maxlen=10)

        # Define our FTP parameters
        self._FTP_HOST = "203.32.104.46"
        self._FTP_PORT = 21
        self._FTP_USER = 'delta'
        self._FTP_PW = 'P@ssw8rd'

        # Define our logging flag
        self._LOG = True

        # Check if we have the folders logs/ and charging_logs/
        if not os.path.exists('../data/logs/'):
            os.makedirs('../data/logs', exist_ok=True)
        if not os.path.exists('../data/charging_logs/'):
            os.makedirs('../data/charging_logs/', exist_ok=True)

        _LOG_FILE_NAME = datetime.now().strftime('%Y-%m-%d')
        self.log_data(location='../data/logs/' + _LOG_FILE_NAME, purpose='log_inverter_data', initial_run=True)

        # Define our local OCPP websocket connection
        self.ocpp_ws = OCPPDataBridge("ws://127.0.0.1:8000/ocpp_data_service/", self.information_bus,
                                      self.charger_status_information_bus, self.ws_receiver_stopped_event)
        self.ocpp_ws.name = 'OCPPWS'
        self.ocpp_ws.daemon = True
        self.ocpp_ws.start()

        # Define the function that will see if we are online or not
        self.internet_checker_thread = None

        # We only want to authenticate when we are online
        if self._ONLINE:
            for _ in range(10):
                try:
                    authentication_success = self.authenticate()
                    break

                except OSError as e:
                    log('Got an OSError! Lets try to authenticate again')
                    log(e)
                    self.handle_internet_check()

            # (If we are online) Handle all of the data syncing
            # self.perform_file_integrity_check(full_check=False)

            # Define our update software notifier
            self.software_update_ws = SoftwareUpdateNotifier("ws://127.0.0.1:5000/delta_solar_charger_software_update")
            self.software_update_ws.name = 'SOFTWAREUPDATEWS'
            self.software_update_ws.daemon = True
            self.software_update_ws.start()

            # Define our factory reset notifier
            self.factory_reset_ws = FactoryResetNotifier("ws://127.0.0.1:5000/delta_solar_charger_factory_reset")
            self.factory_reset_ws.name = 'FACTORYRESETWS'
            self.factory_reset_ws.daemon = True
            self.factory_reset_ws.start()

        self.logger.info('WE HAVE FINISHED INIT METHOD OF FIREBASE METHODS!!!')
        log('WE HAVE FINISHED INIT METHOD OF FIREBASE METHODS!!! ')

    def internet_checker(self):
        """ internet_checker will keep pinging Google until the internet comes back online """

        log('\nWe have entered the internet checker\n')

        internet_online_counter = 0
        internet_online = False
        while internet_online is False:
            try:
                log('Sending a request to Google from internet checker')
                response = requests.get("http://www.google.com", verify=False, timeout=10)
                log("response code: " + str(response.status_code))

                if response.status_code == 200:
                    log('internet online counter:', internet_online_counter)

                    # We must be able to ping google.com 20 times before we are allowed to exit
                    if internet_online_counter == 20:
                        internet_online = True

                    internet_online_counter += 1

            except requests.ConnectionError:
                log("Could not connect to the internet, trying again 3 seconds...")
                internet_online = False
                internet_online_counter = 0
                time.sleep(2)

        # Now that we recovered, we must synchronise charger statuses and perform file integrity check
        self.synchronise_charger_status()
        self.refresh_tokens()
        self.perform_file_integrity_check(full_check=False)

        log('\nWe are OUT of internet_checker thread\n')

    def handle_internet_check(self):
        """ This function checks if we have a internet checker active. If we don't then make one """

        if self.internet_checker_thread is None or not self.internet_checker_thread.isAlive():
            log('Internet checker thread is None, lets start one!')
            self.internet_checker_thread = Thread(target=self.internet_checker)
            self.internet_checker_thread.name = "INTERNET_CHECKER_THREAD"
            self.internet_checker_thread.daemon = True
            self.internet_checker_thread.start()

    @staticmethod
    def condition_data(modbus_data):
        # The structure of modbus_data is: # (inverter_data, bt_data, dpm_data)
        # Now extract them one by one to make it easier to work with:

        inverter_data_temp = modbus_data[0]
        inverter_data = {
            'AC1 Voltage': {'value': inverter_data_temp['ac1_voltage'] / 10,
                            # 'unit': 'V',
                            # 'name': 'AC1 Voltage'
                            },
            'AC1 Current': {'value': inverter_data_temp['ac1_current'] / 100,
                            # 'unit': 'A',
                            # 'name': 'AC1 Current'
                            },
            'AC1 Power': {'value': inverter_data_temp['ac1_power'],
                          # 'unit': 'W',
                          # 'name': 'AC1 Power'
                          },
            'AC1 Frequency': {'value': inverter_data_temp['ac1_freq'] / 100,
                              # 'unit': 'Hz',
                              # 'name': 'AC1 Frequency'
                              },

            'AC2 Voltage': {'value': inverter_data_temp['ac2_voltage'] / 10,
                            # 'unit': 'V',
                            # 'name': 'AC2 Voltage'
                            },
            'AC2 Current': {'value': inverter_data_temp['ac2_current'] / 100,
                            # 'unit': 'A',
                            # 'name': 'AC2 Current'
                            },
            'AC2 Power': {'value': inverter_data_temp['ac2_power'],
                          # 'unit': 'W',
                          # 'name': 'AC2 Power'
                          },
            'AC2 Frequency': {'value': inverter_data_temp['ac2_freq'] / 100,
                              # 'unit': 'Hz',
                              # 'name': 'AC2 Frequency'
                              },

            'DC1 Voltage': {'value': inverter_data_temp['dc1_voltage'] / 10,
                            # 'unit': 'V',
                            # 'name': 'DC1 Voltage'
                            },
            'DC1 Current': {'value': inverter_data_temp['dc1_current'] / 100,
                            # 'unit': 'A',
                            # 'name': 'DC1 Current'
                            },
            'DC1 Power': {'value': inverter_data_temp['dc1_power'],
                          # 'unit': 'W',
                          # 'name': 'DC1 Power'
                          },

            'DC2 Voltage': {'value': inverter_data_temp['dc2_voltage'] / 10,
                            # 'unit': 'V',
                            # 'name': 'DC2 Voltage'
                            },
            'DC2 Current': {'value': inverter_data_temp['dc2_current'] / 100,
                            # 'unit': 'A',
                            # 'name': 'DC2 Current'
                            },
            'DC2 Power': {'value': inverter_data_temp['dc2_power'],
                          # 'unit': 'W',
                          # 'name': 'DC2 Power'
                          },
            'Operation Mode': {'value': inverter_data_temp['inverter_op_mode'],
                               # 'name': 'Operation Mode'
                               },
            'Inverter Status': {'value': inverter_data_temp['inverter_status']},
            'DSP FW': {'value': inverter_data_temp['fw_dsp'],
                       },
            'RED FW': {'value': inverter_data_temp['fw_red'],
                       },
            'DISP FW': {'value': inverter_data_temp['fw_disp'],
                        }
        }

        bt_data_temp = modbus_data[1]
        bt_data = {
            'Battery SOC': {'value': bt_data_temp['bt_soc'] / 10,
                            'unit': '%',
                            # 'name': 'Battery SOC'
                            },

            'Battery Voltage': {'value': bt_data_temp['bt_voltage'] / 10,
                                # 'unit': 'V',
                                # 'name': 'Battery Voltage'
                                },
            'Battery Current': {'value': -1 * bt_data_temp['bt_current'] / 100,
                                # 'unit': 'A',
                                # 'name': 'Battery Current'
                                },
            'Battery Wattage': {'value': float(-1 * bt_data_temp['bt_wattage']),
                                # 'unit': 'W',
                                # 'name': 'Battery Wattage'
                                },

            'Utility AC Current': {'value': bt_data_temp['utility_current'] / 100,
                                   # 'unit': 'A',
                                   # 'name': 'Utility AC Current'
                                   },
            'Utility AC Power': {'value': bt_data_temp['utility_power'],
                                 # 'unit': 'W',
                                 # 'name': 'Utility AC Power'
                                 },

            'Battery Capacity': {'value': bt_data_temp['bt_capacity'] * 100,
                                 # 'unit': "Wh",
                                 # 'name': 'Battery Capacity'
                                 },

            'Battery Operation Mode': {'value': bt_data_temp['bt_op_mode'],
                                       # 'unit': None,
                                       # 'name': 'Battery Operation Mode'
                                       },
            'Battery Module 1 Max Temp': {'value': bt_data_temp['bt_module1_temp_max'] / 10,
                                          # 'unit': None,
                                          # 'name': 'Battery Module 1 Max Temp'
                                          },
            'Battery Module 1 Min Temp': {'value': bt_data_temp['bt_module1_temp_min'] / 10,
                                          # 'unit': None,
                                          # 'name': 'Battery Module 1 Min Temp'
                                          },
        }

        # dpm_data_temp = modbus_data[2]
        # dpm_data = {
        #     'dpm_test': {'value': dpm_data_temp['test'],
        #                  'unit': '%',
        #                  'name': 'dpm_placeholder'},
        # }

        final_data = dict()
        final_data.update({'inverter_data': inverter_data})
        final_data.update({'bt_data': bt_data})
        # final_data.update({'dpm_data': dpm_data})

        # Add a time stamp for the live database
        current_time = datetime.now()
        final_data.update({'timestamp': str(current_time)})

        # history_dict is a dictionary that will be uploaded to history in Firebase
        history_dict = {
            'time': current_time.strftime("%H%M%S"),
            'ac1p': inverter_data_temp['ac1_power'],
            'ac1v': inverter_data_temp['ac1_voltage'] / 10,
            'ac1c': inverter_data_temp['ac1_current'] / 100,

            'ac2p': inverter_data_temp['ac2_power'],
            'ac2v': inverter_data_temp['ac2_voltage'] / 10,
            'ac2c': inverter_data_temp['ac2_current'] / 100,

            'dc1p': inverter_data_temp['dc1_power'],
            'dc1v': inverter_data_temp['dc1_voltage'] / 10,
            'dc1c': inverter_data_temp['dc1_current'] / 100,

            'dc2p': inverter_data_temp['dc2_power'],
            'dc2v': inverter_data_temp['dc2_voltage'] / 10,
            'dc2c': inverter_data_temp['dc2_current'] / 100,

            'dctp': inverter_data_temp['dc2_power'] + inverter_data_temp['dc1_power'],

            # 'dsp_fw': inverter_data_temp['fw_dsp'],
            # 'red_fw': inverter_data_temp['fw_red'],
            # 'disp_fw': inverter_data_temp['fw_disp'],

            'btp': float(-1 * bt_data_temp['bt_wattage']),
            'btv': bt_data_temp['bt_voltage'] / 10,
            'btc': bt_data_temp['bt_current'] / 100,
            'btsoc': bt_data_temp['bt_soc'] / 10,

            'utility_p': bt_data_temp['utility_power'],
            'utility_c': bt_data_temp['utility_current'] / 100,

            'bt_module1_temp_min': bt_data_temp['bt_module1_temp_min'] / 10,
            'bt_module1_temp_max': bt_data_temp['bt_module1_temp_max'] / 10,

            'ac1_freq': inverter_data_temp['ac1_freq'] / 100
        }

        return final_data, history_dict

    def log_data(self, location, purpose, data=None, initial_run=False):
        """ This function deals with any logging to .csv files that needs to be done """

        if purpose == "log_inverter_data":
            # This function defaults to not initial_run. So if we ONLY want to log headers, we must override initial_run

            # Conditions for this block to run:
            # 1) logging MUST be enabled
            # 2) There must not be an existing csv file there
            if self._LOG and not os.path.isfile(location + '.csv'):
                with open(location + '.csv', 'a') as f:
                    writer = csv.writer(f)
                    writer.writerow(
                        ['time', 'ac1p', 'ac1v', 'ac1c', 'ac2p', 'ac2v', 'ac2c', 'dc1p', 'dc1v', 'dc1c', 'dc2p', 'dc2v',
                         'dc2c', 'btp', 'btv', 'btc', 'btsoc', 'utility_p', 'utility_c', 'bt_module1_max_temp',
                         'bt_module1_min_temp', 'ac1_freq'])

            # Log the data without header if initial_run is False and we have enabled logging
            if self._LOG and not initial_run:
                current_time = datetime.now()
                with open(location + '.csv', 'a') as f:
                    writer = csv.writer(f)
                    writer.writerow(
                        [str(current_time),
                         data['inverter_data']['AC1 Power']['value'],
                         data['inverter_data']['AC1 Voltage']['value'],
                         data['inverter_data']['AC1 Current']['value'],

                         data['inverter_data']['AC2 Power']['value'],
                         data['inverter_data']['AC2 Voltage']['value'],
                         data['inverter_data']['AC2 Current']['value'],

                         data['inverter_data']['DC1 Power']['value'],
                         data['inverter_data']['DC1 Voltage']['value'],
                         data['inverter_data']['DC1 Current']['value'],

                         data['inverter_data']['DC2 Power']['value'],
                         data['inverter_data']['DC2 Voltage']['value'],
                         data['inverter_data']['DC2 Current']['value'],

                         data['bt_data']['Battery Wattage']['value'],
                         data['bt_data']['Battery Voltage']['value'],
                         data['bt_data']['Battery Current']['value'],

                         data['bt_data']['Battery SOC']['value'],

                         data['bt_data']['Utility AC Power']['value'],
                         data['bt_data']['Utility AC Current']['value'],

                         data['bt_data']['Battery Module 1 Max Temp']['value'],
                         data['bt_data']['Battery Module 1 Min Temp']['value'],

                         data['inverter_data']['AC1 Frequency']['value']
                         ])

        elif purpose == "log_charge_session":
            # If logging is turned on
            if self._LOG:
                # Check if the folder for the charger exists
                if not os.path.isdir('../data/charging_logs/' + location.split('/')[3]):
                    os.makedirs('../data/charging_logs/' + location.split('/')[3])

                # First check if there is a file for the log file we are opening
                if not os.path.isfile(location + '.csv'):
                    # If there isn't then, we create a new file to write the headers
                    with open(location + '.csv', 'a') as f:
                        writer = csv.writer(f)
                        writer.writerow(
                            ['time', 'voltage', 'current_import', 'power_import', 'energy_import', 'solar_power',
                             'battery_power', 'battery_soc', 'battery_temp', 'grid_power'])

                # Log the data that we got from the MeterValues message
                with open(location + '.csv', 'a') as f:
                    writer = csv.writer(f)
                    writer.writerow(data)

    def authenticate(self):
        log('Attempting to authenticate with Firebase')
        config = {
            "apiKey": "AIzaSyCaxTOBofd7qrnbas5gGsZcuvy_zNSi_ik",
            "authDomain": "smart-charging-app.firebaseapp.com",
            "databaseURL": "https://smart-charging-app.firebaseio.com",
            "storageBucket": "",
        }

        self.firebase = pyrebase.initialize_app(config)

        # Get a reference to the auth service
        self.auth = self.firebase.auth()

        # Connect to our crentials DB
        sql_db = sqlite3.connect('../config/config.sqlite')
        firebase_cred = dict()
        # Write the DB to a dict
        for row in sql_db.execute("SELECT key, value FROM unnamed"):
            firebase_cred.update({row[0]: row[1]})

        # Login to Firebase with the credentials
        user = self.auth.sign_in_with_email_and_password(firebase_cred['firebase_email'],
                                                         firebase_cred['firebase_password'])

        # The new uid and idToken are now defined
        self.uid = user['localId']
        self.idToken = user['idToken']
        self.refreshToken = user['refreshToken']

        log(self.uid)

        # Check if authentication is all good here.
        authentication_success = True

        if authentication_success:
            # Initialise database

            self.db = self.firebase.database(timeout_length=5)

            # The first thing we need to do is to make sure all of the values we need to stream for are there
            self.initialize_firebase_db_values()

            # Now we can start our Firebase streamers
            self.start_firebase_listeners()

            # Post the current date to history_keys so we have a list of the days that have available data
            self.db.child("users").child(self.uid).child("history_keys").update(
                {datetime.now().strftime("%Y-%m-%d"): True}, self.idToken)

            # We need to refresh the token once every hour. So let's refresh it once every 40 minutes.
            self.refresh_timer = Timer(2400, self.refresh_tokens)
            self.refresh_timer.name = 'Refresh Timer'
            self.refresh_timer.daemon = True
            self.refresh_timer.start()

            log('Authentication Success - got a new idToken')

            return authentication_success

    def initialize_firebase_db_values(self):
        """ This method makes sure that all of the values that we are streaming for exist at start up """

        # Initialize our manual charge control node
        if self.db.child("users").child(self.uid).child('evc_inputs/manual_charge_control').get(
                self.idToken).val() is None:
            self.db.child("users").child(self.uid).child('evc_inputs/manual_charge_control').update(
                {'charge_rate': 0, 'chargerID': 'chargerID'},
                self.idToken)

        # Initialize buffer aggressiveness mode
        if self.db.child("users").child(self.uid).child('evc_inputs/buffer_aggro_mode').get(self.idToken).val() is None:
            self.db.child("users").child(self.uid).child('evc_inputs').update({'buffer_aggro_mode': "Balanced"},
                                                                              self.idToken)

        # Initialize the charging mode and authenticated required boolean
        if self.db.child("users").child(self.uid).child('evc_inputs/charging_modes').get(self.idToken).val() is None:
            self.db.child("users").child(self.uid).child('evc_inputs/charging_modes').update(
                {'single_charging_mode': "MAX_CHARGE_GRID", 'authentication_required': True},
                self.idToken)

        # Make all of our chargers offline, we will use Heartbeats to get them online
        if self.db.child("users").child(self.uid).child('ev_chargers').get(self.idToken).val() is not None:

            # charger_list is the list of chargers that are registered in the system
            charger_list = list(self.db.child("users").child(self.uid).child('ev_chargers').get(self.idToken).val())

            for charger in charger_list:
                self.db.child("users").child(self.uid).child("evc_inputs").child(charger).update({"alive": False},
                                                                                                 self.idToken)

        # We have to check if we need to handle inverter database - if yesterday's csv is not there
        with FTP(host=self._FTP_HOST) as ftp:
            ftp.login(user=self._FTP_USER, passwd=self._FTP_PW)
            try:
                ftp.cwd("/EVCS_portal/logs/" + self.uid + '/inverter_logs/')
                ftp_csv_list = ftp.nlst()

                # Take the current day, subtract a timedelta
                if (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d") + '.csv' not in ftp_csv_list:
                    log('Yesterdays file is not found, we need to fix our inverter log database')
                    self.handle_inverter_database()

            except error_perm as e:
                log('oops, doesnt exist, skipping: ', e)

        # Write our current version to Firebase
        with open('../docs/version.txt', 'r') as f:
            current_version = float(f.read())
            self.db.child("users").child(self.uid).update({'version': current_version}, self.idToken)

    # THIS METHOD IS ALWAYS RUNNING IN A DIFFERENT THREAD
    def refresh_tokens(self):
        """ This method will refresh tokens and recreate listeners """

        self.logger.info('We are refreshing tokens' + str(datetime.now()))
        log('We are refreshing tokens!', datetime.now())

        # First close existing listeners
        try:
            # log('charging mode listener before closing:', self.charging_modes_listener.sse.running)
            self.charging_modes_listener.close()
            # log('charging mode listener successfully closed')
            # log('charging mode listener after closing:', self.charging_modes_listener.sse.running)
        except AttributeError as e:
            log(e, 'but continue in refresh_tokens 2')

        try:
            # log('buffer listener before closing:', self.buffer_aggressiveness_listener)
            self.buffer_aggressiveness_listener.close()
            # log('buffer listener after closing:', self.buffer_aggressiveness_listener)
        except AttributeError as e:
            log(e)
        try:
            # log('firmware listener before closing:', self.update_firmware_listener)
            self.update_firmware_listener.close()
            # log('firmware listener after closing:', self.update_firmware_listener)
        except AttributeError as e:
            log(e)
        try:
            # log('dsc firmware upgrade listener before closing:', self.dsc_firmware_update_listener)
            self.dsc_firmware_update_listener.close()
            # log('dsc firmware upgrade listener after closing:', self.dsc_firmware_update_listener)
        except AttributeError as e:
            log(e)
        try:
            # log('delete_charger_listener before closing:', self.dsc_firmware_update_listener)
            self.delete_charger_listener.close()
            # log('delete_charger_listener after closing:', self.dsc_firmware_update_listener)
        except AttributeError as e:
            log(e)
        try:
            # log('factory_reset_listener before closing:', self.factory_reset_listener)
            self.factory_reset_listener.close()
            # log('factory_reset_listener after closing:', self.factory_reset_listener)
        except AttributeError as e:
            log(e)
        try:
            # log('manual control listener before closing:', self.misc_listener)
            self.manual_charge_control_listener.close()
            # log('manual control listener after closing:', self.misc_listener)
        except AttributeError as e:
            log(e)
        try:
            # log('misc listener before closing:', self.misc_listener)
            self.misc_listener.close()
            # log('misc listener after closing:', self.misc_listener)
        except AttributeError as e:
            log(e)

        # Refresh token
        log('before refresh')
        user = self.auth.refresh(self.refreshToken)
        log('after refresh')
        idToken = user['idToken']
        self.refreshToken = user['refreshToken']

        # Put new idToken on the information bus so that the idToken can be updated in the main thread
        self.information_bus.put(('idToken', idToken))

        log('Tokens successfully refreshed')

    def start_firebase_listeners(self):
        """ This method starts all of our Firebase listeners """

        # Start listener for our current charge mode
        self.charging_modes_listener = self.db.child("users").child(self.uid).child('evc_inputs/charging_modes').stream(
            stream_handler=self.firebase_stream_handler,
            token=self.idToken,
            stream_id="charging_modes_change")

        # Start a listener for our battery aggressiveness mode
        self.buffer_aggressiveness_listener = self.db.child("users").child(self.uid).child(
            'evc_inputs/buffer_aggro_mode').stream(stream_handler=self.firebase_stream_handler, token=self.idToken,
                                                   stream_id="buffer_aggro_change")

        self.update_firmware_listener = self.db.child("users").child(self.uid).child(
            'evc_inputs/update_firmware').stream(stream_handler=self.firebase_stream_handler, token=self.idToken,
                                                 stream_id="update_firmware_request")

        self.dsc_firmware_update_listener = self.db.child("users").child(self.uid).child(
            'evc_inputs/dsc_firmware_update').stream(stream_handler=self.firebase_stream_handler,
                                                     token=self.idToken,
                                                     stream_id="dsc_firmware_update")

        self.delete_charger_listener = self.db.child("users").child(self.uid).child(
            'evc_inputs/delete_charger').stream(stream_handler=self.firebase_stream_handler,
                                                token=self.idToken,
                                                stream_id="delete_charger")

        self.factory_reset_listener = self.db.child("users").child(self.uid).child(
            'evc_inputs/factory_reset').stream(stream_handler=self.firebase_stream_handler,
                                               token=self.idToken,
                                               stream_id="factory_reset")

        self.manual_charge_control_listener = self.db.child("users").child(self.uid).child(
            'evc_inputs/manual_charge_control').stream(stream_handler=self.firebase_stream_handler,
                                                       token=self.idToken,
                                                       stream_id="manual_charge_control")

        self.misc_listener = self.db.child("users").child(self.uid).child('evc_inputs/misc_command').stream(
            stream_handler=self.firebase_stream_handler, token=self.idToken, stream_id="misc_command")

    def firebase_stream_handler(self, message):
        """ Defines the callback due to changes in certain Firebase entries """

        # log('message received', message)

        if message['stream_id'] == "charging_modes_change" and message['data']:
            # If path is '/' then we know it is initial run or both has been modified
            if message['path'] == '/':
                if 'single_charging_mode' in message['data']:
                    self._CHARGING_MODE = message['data']['single_charging_mode']
                    self.firebase_to_analyse_queue.put(
                        {'purpose': 'change_single_charging_mode',
                         'charge_mode': message['data']['single_charging_mode']})

                if 'authentication_required' in message['data']:
                    try:
                        self.ocpp_ws.send(json.dumps({'purpose': 'change_authentication_requirement',
                                                      'authentication_required': message['data'][
                                                          'authentication_required']}))
                    except ConnectionRefusedError as e:
                        log(e, 'got a connection refused error')
                        pass

            elif message['path'] == "/single_charging_mode":
                self._CHARGING_MODE = message['data']
                self.firebase_to_analyse_queue.put(
                    {'purpose': 'change_single_charging_mode', 'charge_mode': message['data']})

            elif message['path'] == "/authentication_required":
                try:
                    self.ocpp_ws.send(json.dumps({'purpose': 'change_authentication_requirement',
                                                  'authentication_required': message['data']}))
                except ConnectionRefusedError as e:
                    log(e, 'got a connection refused error')
                    pass

        elif message['stream_id'] == "buffer_aggro_change" and message['data']:
            # Send the new buffer aggressiveness setting to analyze process
            self.firebase_to_analyse_queue.put({'purpose': 'buffer_aggro_change', 'buffer_aggro_mode': message['data']})

        elif message['stream_id'] == "update_firmware_request":
            self.information_bus.put(('update_firmware', message['data']))

        elif message['stream_id'] == "dsc_firmware_update" and message['data'] is not None:
            log('hello')

            self.information_bus.put(('dsc_firmware_update', message['data']))

        elif message['stream_id'] == "delete_charger":
            self.information_bus.put(('delete_charger', message['data']))

        elif message['stream_id'] == "factory_reset":
            self.information_bus.put(('factory_reset', message['data']))

        elif message['stream_id'] == "manual_charge_control" and message['data']:
            self.information_bus.put(('manual_charge_control', message['data']))

        elif message['stream_id'] == "misc_command" and message['data']:
            self.information_bus.put(('misc_command', message['data']))

    def respond_to_authorize(self, chargerID):
        """ This function is called when RFID mode is on and user swipes RFID card """

        log('We are now in respond to authorize', chargerID)

        counter = 0
        while True:
            # First we have to check if our chargerID is in the charge rates list
            if chargerID in self.charge_rates:
                # We now have our target charge rate
                target_charge_rate = self.charge_rates[chargerID]

                log(chargerID, 'is in our charge rates list with charge rate of:', self.charge_rates[chargerID])
                break

            counter += 1
            log(counter)
            # If our charger is not in the list because it has a "Finished" status, we wait 10 seconds
            if counter == 20:
                # Then we set the target charge rate to 6
                target_charge_rate = 6

                log('We could not find', chargerID, 'in the list of charge rates, so we are setting it to 6A')
                break

            time.sleep(0.5)

        # Make sure we are grid connected if we have more than 1 active charger
        num_active_chargers = 0
        for temp_chargerID, payload in self._charger_status_list.items():
            if payload['charging']:
                num_active_chargers += 1

        # Now get the current inverter operation mode
        latest_modbus_data = self.modbus_data_queue.pop()

        # If we are in standalone mode with more than 1 active charger, then we reject the incoming charge session
        if num_active_chargers > 1 and latest_modbus_data['inverter_data']['Inverter Status'] == "Stand Alone":
            log('We have more than one active charger but we are in standalone mode - denying the charge session')
            self.ocpp_ws.send(
                json.dumps({'chargerID': chargerID, 'purpose': 'authorize_request', 'authorized': False}))
            return

        # But if we are grid connected then we continue with monitoring the charge rate
        counter = 0
        while True:
            log('Available current is:', self._AVAILABLE_CURRENT, 'current needed is:', target_charge_rate)

            # Check if our available current is greater than our target charge rate
            if self._AVAILABLE_CURRENT > target_charge_rate:
                log('We have more available current than our target charge rate, lets start charging!')

                # If we have enough, then we can send a message to OCPP server to accept the authorize request
                self.ocpp_ws.send(
                    json.dumps({'chargerID': chargerID, 'purpose': 'authorize_request', 'authorized': True}))

                log('Just sent out the message to authorize the charging session!')

                break

            else:
                # If we have been waiting for 20 seconds or more, we break out of the loop
                if counter > 40:
                    log('20 seconds passed, denying access to charger')
                    self.ocpp_ws.send(
                        json.dumps({'chargerID': chargerID, 'purpose': 'authorize_request', 'authorized': False}))
                    break

                log('Not enough current. We are consuming:', self.latest_modbus_data['ac2c'],
                    'but we want to charge at', target_charge_rate)

            counter += 1
            log('counter is', counter)
            time.sleep(0.5)

        log('Responded to authorize, exiting thread now')

    def check_and_initialize_charger_list(self, charger_id):
        """ This function checks if the charger ID is in our list and if it's not then initialize the charger ID """

        if charger_id not in self._charger_status_list:
            log(charger_id, 'not in list, add it now')
            # If we don't then we need to add it
            self._charger_status_list.update(
                {charger_id: {'charging': False, 'charge_rate': None, 'charging_timestamp': None, 'meterStart': None,
                              'transaction_id': None}})

            # If we are not in the list then we return False
            return False

        else:
            # If the charger ID is already in the list then return True
            return True

    def synchronise_charger_status(self):
        """ This function sends a message to the OCPP backend to trigger a StatusNotificaiton for every charger """
        try:
            log("Sending a Trigger StatusNotification!")
            # log('Identifier:', unique_id)
            self.ocpp_ws.send(json.dumps(
                {'purpose': 'Trigger_StatusNotification'}))
        except ConnectionRefusedError as e:
            log(e, 'got a connection refused error')
            pass

    def check_status_information_bus(self):
        while True:
            if self.charger_status_information_bus.qsize() > 0:
                new = self.charger_status_information_bus.get()
                log('New info on charging information bus!', new)
            else:
                break

            if new[0] == "charging_status_change":
                """ If there is a StatusNotification message """
                temp_chargerID = new[2]
                new_data_charging_status = new[1]['charging_status']
                temp_charging_timestamp = new[1]['charging_timestamp']
                temp_meterValue = new[1]['meterValue']
                transaction_id = new[1]['transaction_id']

                log('We are in charging status change for', temp_chargerID)

                # First check if we have this chargerID in our charger status list
                if not self.check_and_initialize_charger_list(temp_chargerID):
                    # Send our analyze process the dicts of dicts so it can determine the best charge rates
                    self.firebase_to_analyse_queue.put(
                        {'purpose': 'charge_status', 'charger_list': self._charger_status_list})

                # Now that charger is in the status list, make sure the new status is different from the old status
                if self._charger_status_list[temp_chargerID]['charging'] != new_data_charging_status:
                    log('CHARGE STATUS CHANGED!', new)

                    # Then append the new charging status into the charger status list
                    self._charger_status_list[temp_chargerID]['charging'] = new_data_charging_status

                    # If timestamps are different, take the timestamp of the backend
                    if self._charger_status_list[temp_chargerID]['charging_timestamp'] != temp_charging_timestamp:
                        log('There is a charging timestamp mismatch:',
                            self._charger_status_list[temp_chargerID]['charging_timestamp'], temp_charging_timestamp,
                            'taking charging information from the backend!')
                        self._charger_status_list[temp_chargerID]['charging_timestamp'] = temp_charging_timestamp

                        # Also take the MeterValue from the backend
                        if temp_meterValue:
                            self._charger_status_list[temp_chargerID]['meterStart'] = temp_meterValue

                        # Also take the transaction_id from the backend
                        if transaction_id:
                            self._charger_status_list[temp_chargerID]['transaction_id'] = transaction_id

                    # Send our analyze process the dicts of dicts so it can determine the best charge rates
                    self.firebase_to_analyse_queue.put(
                        {'purpose': 'charge_status', 'charger_list': self._charger_status_list})

                # (Online) If we are charging and timestamp in the backend is a proper value, update Firebase status
                # The reason we are doing this is that we do not want to set Firebase 'charging' as True before we have
                # a proper timestamp uploaded to Firebase from transaction_alert
                # However, if we are recovering from a crash, transaction_alert would not be run so we need this block
                # to detect if there is a charging session and update Firebase
                if self._ONLINE:
                    if new_data_charging_status is True and temp_charging_timestamp:
                        log('The charging timestamp received is', temp_charging_timestamp, 'continuing...')

                        try:
                            # If we are online in general then we need to update our evc_inputs charging status
                            self.db.child("users").child(self.uid).child("evc_inputs/charging").update(
                                {temp_chargerID: new_data_charging_status}, self.idToken)
                        except OSError as e:
                            log('charging status - charging time out', e)
                            self.handle_internet_check()

                    elif new_data_charging_status is False or new_data_charging_status == "plugged":
                        try:
                            # If we are online in general then we need to update our evc_inputs charging status
                            self.db.child("users").child(self.uid).child("evc_inputs/charging").update(
                                {temp_chargerID: new_data_charging_status}, self.idToken)
                        except OSError as e:
                            log('charging status - not charging time out', e)
                            self.handle_internet_check()

            elif new[0] == "transaction_alert":
                """ If there is a StartTransaction or StopTransaction message """
                log('Got a transaction alert on information bus for', new[2])

                temp_chargerID = new[2]
                is_start_transaction_message = new[1]['charging_status']
                meterValue = new[1]['meterValue']
                received_timestamp = new[1]['charging_timestamp']
                transaction_id = new[1]['transaction_id']

                # Update our charging status in the charger status list
                self._charger_status_list[temp_chargerID]['charging'] = is_start_transaction_message

                # Send our analyze process the dicts of dicts so it can determine the best charge rates
                self.firebase_to_analyse_queue.put(
                    {'purpose': 'charge_status', 'charger_list': self._charger_status_list})

                # If we got a StartTransaction message
                if is_start_transaction_message is True:

                    charging_timestamp = received_timestamp

                    # If charging is True then we need to add a timestamp to use for logging
                    self._charger_status_list[temp_chargerID]['charging_timestamp'] = charging_timestamp

                    # We also need to note the MeterStart value
                    self._charger_status_list[temp_chargerID]['meterStart'] = meterValue

                    # Also remember the transactionId
                    self._charger_status_list[temp_chargerID]['transaction_id'] = transaction_id

                    # (Online) Update charging history keys
                    if self._ONLINE:
                        try:
                            self.db.child("users").child(self.uid).child("charging_history_keys").child(
                                temp_chargerID).child(charging_timestamp.split(' ')[0]).update(
                                {charging_timestamp.split(' ')[1]: True}, self.idToken)
                        except OSError:
                            log('transaction_alert - start charging history keys time out')
                            self.handle_internet_check()
                            # Todo: put this in the once-online to do queue

                # If we got a StopTransaction message
                elif is_start_transaction_message is False:

                    # (Online) Update our charging history analytics and update our charging database
                    if self._ONLINE:
                        try:
                            total_energy_charged = meterValue - self._charger_status_list[temp_chargerID]['meterStart']

                            # Calculate the duration of the charging session
                            total_charge_duration = datetime.strptime(received_timestamp,
                                                                      '%Y-%m-%d %H%M') - datetime.strptime(
                                self._charger_status_list[temp_chargerID]['charging_timestamp'], '%Y-%m-%d %H%M')
                            total_charge_duration = total_charge_duration.total_seconds()

                            log('We have a stop transaction, total charge duration is', total_charge_duration)
                            # Upload our analytics to analytics->charging_history_analytics->chargerID->date->time
                            try:
                                self.db.child("users").child(self.uid).child(
                                    "analytics/charging_history_analytics").child(
                                    temp_chargerID).child(
                                    self._charger_status_list[temp_chargerID]['charging_timestamp'].split(' ')[
                                        0]).child(
                                    self._charger_status_list[temp_chargerID]['charging_timestamp'].split(' ')[
                                        1]).update(
                                    {'energy': total_energy_charged, 'duration_seconds': total_charge_duration},
                                    self.idToken)

                                # If 'charging' is False then we need to update our charging database
                                upload_thread = Thread(target=self.ftp_upload_charge_session(temp_chargerID,
                                                                                             self._charger_status_list[
                                                                                                 temp_chargerID][
                                                                                                 'charging_timestamp']))
                                upload_thread.daemon = True
                                upload_thread.start()

                                # Delete the charging history record
                                self.db.child("users").child(self.uid).child("charging_history").child(
                                    temp_chargerID).child(
                                    self._charger_status_list[temp_chargerID]['charging_timestamp']).remove(
                                    self.idToken)

                            except OSError as e:
                                log(e)
                                log('transation alert - stop, charging history analytics time out')
                                log('transation alert - stop upload charge session time out')
                                self.handle_internet_check()
                                # Todo: Make sure we add this to the once-online todo queue

                        except TypeError as e:
                            # If we get a TypeError it is because our OCPP server crashed. For now, remove all traces
                            # of the charging session
                            log(e)

                    self._charger_status_list[temp_chargerID]['charging_timestamp'] = None
                    self._charger_status_list[temp_chargerID]['meterStart'] = None
                    self._charger_status_list[temp_chargerID]['transaction_id'] = None
                    self._charger_status_list[temp_chargerID]['charge_rate'] = None

                # (Online) Update evc_inputs charging status for that chargerID now that everything is set up
                if self._ONLINE:
                    try:
                        # Update our evc_inputs charging status
                        self.db.child("users").child(self.uid).child("evc_inputs/charging").update(
                            {temp_chargerID: is_start_transaction_message}, self.idToken)
                    except OSError as e:
                        log('translation alert end time out', e)
                        self.handle_internet_check()

                log("Finished our a Transaction message, new charger list:", self._charger_status_list)

            elif new[0] == "authorize_request":
                temp_chargerID = new[2]
                authorize_thread = Thread(target=self.respond_to_authorize, args=(temp_chargerID,))
                authorize_thread.daemon = True
                authorize_thread.start()

    def check_information_bus(self):
        """ Called from update_external_sources once a second and handles any new requests sent on the information bus """

        # Try to grab something from the information bus
        try:
            new = self.information_bus.get_nowait()
            # log('New info on information bus!', new)

            # If the info bus payload is about idToken then we refresh our listeners with the new idToken
            if new[0] == "idToken":
                log('I FOUND A NEW IDTOKEN!')

                self.idToken = new[1]
                # Now that we have a new refreshToken, we need to stop all the current listeners and recreate them
                self.start_firebase_listeners()

                if self.refresh_timer.isAlive():
                    log('Timer is still alive, cancel it first')
                    self.refresh_timer.cancel()

                self.refresh_timer = Timer(2400, self.refresh_tokens)
                self.refresh_timer.daemon = True
                self.refresh_timer.start()

            elif new[0] == "new_charger_info":
                log('We got new charger info', new)
                temp_charger_info = new[1]
                temp_chargerID = new[2]

                # We need to send them back a message to adjust the MeterValue interval to 10 seconds
                try:
                    if self._LIMIT_DATA:
                        self.ocpp_ws.send(json.dumps({'chargerID': temp_chargerID, 'purpose': 'misc_command',
                                                      'action': "ChangeConfiguration",
                                                      'misc_data': {"key": "MeterValueSampleInterval", "value": "10"}}))
                    else:
                        self.ocpp_ws.send(json.dumps({'chargerID': temp_chargerID, 'purpose': 'misc_command',
                                                      'action': "ChangeConfiguration",
                                                      'misc_data': {"key": "MeterValueSampleInterval", "value": "10"}}))
                except ConnectionRefusedError as e:
                    log(e, 'got a connection refused error')
                    pass

                # (If we are online) We update the information about our charger in Firebase
                if self._ONLINE:
                    try:
                        log('Uploading', temp_charger_info, 'to Firebase for', temp_chargerID)
                        self.db.child("users").child(self.uid).child("evc_inputs").child(temp_chargerID).update(
                            {"charger_info": temp_charger_info}, self.idToken)
                    except OSError as e:
                        log('new charger info timeout', e)
                        self.handle_internet_check()

            # Alive is received AFTER EVERY OCPP MESSAGE TO A CHARGE POINT
            elif new[0] == "alive":
                log('New info on information bus!', new)

                temp_chargerID = new[2]
                temp_alive = new[1]

                # Update our alive value for that charger in Firebase
                if self._ONLINE:
                    try:
                        # Post our alive status to evc_inputs in Firebase
                        self.db.child("users").child(self.uid).child("evc_inputs").child(temp_chargerID).update(
                            {"alive": new[1]}, self.idToken)

                        # Post our charger to ev_chargers in Firebase
                        self.db.child("users").child(self.uid).child('ev_chargers').update({temp_chargerID: True},
                                                                                           self.idToken)
                    except OSError as e:
                        log('evc_inputs alive true timeout', e)
                        self.handle_internet_check()

                # If alive is True...
                if temp_alive:
                    # Check if the charger exists in our status list and if it doesn't...
                    if temp_chargerID not in self._charger_status_list:
                        # Send a request to synchronise all charge points with OCPP backend
                        self.synchronise_charger_status()
                        # try:
                        #     log("Sending a Trigger StatusNotification!")
                        #     # log('Identifier:', unique_id)
                        #     self.ocpp_ws.send(json.dumps(
                        #         {'purpose': 'Trigger_StatusNotification'}))
                        # except ConnectionRefusedError as e:
                        #     log(e, 'got a connection refused error')
                        #     pass

                # If alive is False (charger gone offline), we must remove it from the list
                else:
                    log('Alive is false, removing', temp_chargerID)
                    if temp_chargerID in self._charger_status_list:
                        del self._charger_status_list[temp_chargerID]

                    # We also have to update the evc_inputs charging status
                    if self._ONLINE:
                        try:
                            # Todo: make sure this is what we want - especially if we are in the middle of charging
                            self.db.child('users').child(self.uid).child('evc_inputs').child(temp_chargerID).update(
                                {'alive': False}, self.idToken)
                        except OSError as e:
                            log('evc_inputs alive false timeout', e)
                            self.handle_internet_check()

                log('*****************************************************************')
                log(datetime.now().strftime('%H:%M:%S'), 'Update on our charger status list:')
                for chargerID, payload in self._charger_status_list.items():
                    log(chargerID, ' ', payload)
                log('*****************************************************************')

            elif new[0] == "meter_values":
                # Define the dictionary that contains all of the electrical info
                temp_metervalues_payload = new[1]['sampledValue']

                # Define the timestamp that came with the MeterValues message
                temp_charging_timestamp = new[1]['timestamp']

                # Define the charger ID that the MeterValues message was sent from
                temp_chargerID = new[2]

                try:
                    # Only run the code if there is still an existing charging session
                    if self._charger_status_list[temp_chargerID]['charging_timestamp']:

                        charging_timestamp_obj = datetime.strptime(temp_charging_timestamp, '%Y-%m-%dT%H:%M:%SZ')
                        timestamp_delta = datetime.now() - charging_timestamp_obj
                        log('Our timestamp delta will be for this metervalue message is: ', timestamp_delta.seconds)

                        # If timestamp is less than 2 minutes off server time, use server time as current timestamp
                        if timestamp_delta.seconds < 120:
                            log('Our metervalues message is live. Using server time as the timestamp')
                            final_metervalue_timestamp = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
                        else:
                            log('Our metervalues message is not live. Using the message timestamp')
                            final_metervalue_timestamp = str(
                                datetime.strptime(temp_charging_timestamp, '%Y-%m-%dT%H:%M:%SZ'))

                        log('Got a metervalue message and it belongs to', temp_chargerID, 'of charging timestamp:',
                            self._charger_status_list[temp_chargerID]['charging_timestamp'])

                        # Initialize a list that is 11 entries long
                        temp_metervalue_entry = ["" for _ in range(11)]

                        # The timestamp we use will be the timestamp from the MeterValue message
                        temp_metervalue_entry[0] = final_metervalue_timestamp

                        for metervalue_dict in temp_metervalues_payload:
                            if metervalue_dict['measurand'] == 'Voltage':
                                temp_metervalue_entry[1] = metervalue_dict['value']

                            elif metervalue_dict['measurand'] == "Current.Import":
                                temp_metervalue_entry[2] = metervalue_dict['value']

                                # Todo: renable this when we need it
                                # Transfer our metervalue current to analyze methods
                                # self.firebase_to_analyse_queue.put(
                                #     {'purpose': 'metervalue_current', 'chargerID': temp_chargerID,
                                #      'metervalue_current': metervalue_dict['value']})

                            elif metervalue_dict['measurand'] == "Power.Active.Import":
                                temp_metervalue_entry[3] = metervalue_dict['value']

                            elif metervalue_dict['measurand'] == "Energy.Active.Import.Register":
                                temp_metervalue_entry[4] = float(metervalue_dict['value']) - \
                                                           self._charger_status_list[temp_chargerID]['meterStart']

                        # We only log and upload if we actually have modbus data
                        if self.latest_modbus_data:
                            temp_metervalue_entry[5] = (self.latest_modbus_data['dc1p'] + self.latest_modbus_data[
                                'dc2p']) / 1000
                            temp_metervalue_entry[6] = self.latest_modbus_data['btp'] / 1000
                            temp_metervalue_entry[7] = self.latest_modbus_data['btsoc']
                            temp_metervalue_entry[8] = self.latest_modbus_data['bt_module1_temp_max']
                            temp_metervalue_entry[9] = self.latest_modbus_data['utility_p'] / 1000

                            # This is there for comparison
                            temp_metervalue_entry[10] = self.latest_modbus_data['ac2p']

                            # Push data to charging_history, create a new log file if there is an active charging session.
                            self.log_data(
                                location='../data/charging_logs/' + temp_chargerID + '/' +
                                         self._charger_status_list[temp_chargerID][
                                             'charging_timestamp'], purpose='log_charge_session',
                                data=temp_metervalue_entry)

                            if self._ONLINE:
                                try:
                                    # (If we are online) Update our charge history
                                    self.db.child("users").child(self.uid).child("charging_history").child(
                                        temp_chargerID).child(
                                        self._charger_status_list[temp_chargerID]['charging_timestamp']).push(
                                        {"Time": temp_metervalue_entry[0], "Voltage": temp_metervalue_entry[1],
                                         "Current_Import": temp_metervalue_entry[2],
                                         "Power_Import": temp_metervalue_entry[3],
                                         "Energy_Import_Aggregate": temp_metervalue_entry[4],
                                         "Solar_Power": temp_metervalue_entry[5],
                                         'Battery_Power': temp_metervalue_entry[6],
                                         'Battery_SOC': temp_metervalue_entry[7],
                                         'Battery_Temperature': temp_metervalue_entry[8],
                                         'Grid_Power': temp_metervalue_entry[9]}, self.idToken)
                                except OSError as e:
                                    log('Meter value timed out', e)
                                    self.handle_internet_check()

                except KeyError as e:
                    log('charger ID does not exist in charger list yet', e)

            elif new[0] == "update_firmware":
                data = new[1]

                # If we got a firmware update request with a "True" value of set
                if data is not None and data['set']:
                    try:
                        # Use the included chargerID to look up the charger model
                        charger_model = self.db.child("users").child(self.uid).child('evc_inputs').child(
                            data['chargerID']).child('charger_info/chargePointModel').get(self.idToken).val()
                        log('We have identified that the charger model is:', charger_model)

                        # Send the request to the OCPP server
                        self.ocpp_ws.send(json.dumps(
                            {'chargerID': data['chargerID'], 'charger_model': charger_model,
                             'firmwareType': data['firmwareType'], 'purpose': 'update_firmware',
                             'fw_url': data['fw_url']}
                        ))

                        # Reset the update_firmware values in Firebase
                        self.db.child("users").child(self.uid).child('evc_inputs/update_firmware').remove(self.idToken)
                        self.db.child("users").child(self.uid).child('evc_inputs/temp_remote_fw_update_info').remove(
                            self.idToken)

                    except OSError as e:
                        log('Update Firmware time out', e)
                        self.handle_internet_check()

            elif new[0] == "fw_status":
                temp_chargerID = new[2]
                fw_status = new[1]

                try:
                    self.db.child("users").child(self.uid).child('evc_inputs/temp_remote_fw_update_info').update({
                        'chargerID': temp_chargerID,
                        'firmware_update_status': fw_status
                    }, self.idToken)
                except OSError as e:
                    log('Firmware update status notification time out', e)
                    self.handle_internet_check()

            elif new[0] == "dsc_firmware_update":
                log('got a dsc firmware update in info bus')
                # First delete the entry
                self.db.child('users').child(self.uid).child("evc_inputs").child("dsc_firmware_update").remove(
                    self.idToken)

                # If the payload on dsc_firmware_update is True, then we send a command to the launcher to run an update
                if new[1]:
                    successful_transmission = False
                    while not successful_transmission:
                        # Send the message to the launcher
                        successful_transmission = self.software_update_ws.send(
                            json.dumps({'dsc_firmware_update': new[1]}))

                        if not successful_transmission:
                            self.software_update_ws.stop()
                            self.software_update_ws = SoftwareUpdateNotifier(
                                "ws://127.0.0.1:5000/delta_solar_charger_software_update")
                            self.software_update_ws.name = 'SOFTWAREUPDATEWS'
                            self.software_update_ws.daemon = True
                            self.software_update_ws.start()

            elif new[0] == "delete_charger":
                charger_id = new[1]

                if charger_id is not None:
                    log('Deleting charger with charger ID:', charger_id)

                    # Remove the charger ID from our charging logs
                    if os.path.exists('/home/pi/deltasolarcharger/data/charging_logs/' + charger_id):
                        os.remove('/home/pi/deltasolarcharger/data/charging_logs/' + charger_id)

                    # Remove the charger ID from Firebase
                    self.db.child('users').child(self.uid).child("evc_inputs").child(charger_id).remove(self.idToken)
                    self.db.child('users').child(self.uid).child("evc_inputs").child('charging').child(
                        charger_id).remove(self.idToken)
                    self.db.child('users').child(self.uid).child("ev_chargers").child(charger_id).remove(self.idToken)
                    self.db.child('users').child(self.uid).child("charging_history").child(charger_id).remove(
                        self.idToken)
                    self.db.child('users').child(self.uid).child("charging_history_keys").child(charger_id).remove(
                        self.idToken)
                    self.db.child('users').child(self.uid).child("analytics").child('charging_history_analytics').child(
                        charger_id).remove(self.idToken)

                    # Now remove the charging logs folder from the FTP server
                    with FTP(host=self._FTP_HOST) as ftp:
                        ftp.login(user=self._FTP_USER, passwd=self._FTP_PW)

                        # Change our working directory to our charging logs directory
                        ftp.cwd("/EVCS_portal/logs/" + self.uid + '/charging_logs/')

                        # Now get a list of all of the directories in this folder
                        ftp_list = ftp.nlst()

                        # Check if our charger ID has a directory in this folder
                        if charger_id in ftp_list:
                            # If there is, then delete that folder
                            ftp.rmd("/EVCS_portal/logs/" + self.uid + '/charging_logs/' + charger_id)

                    # Finally delete the delete charger command
                    self.db.child('users').child(self.uid).child("evc_inputs").child("delete_charger").remove(
                        self.idToken)

            elif new[0] == "factory_reset":
                # If the payload on dsc_firmware_update is True, then we send a command to the launcher to run an update
                if new[1]:

                    successful_transmission = False
                    while not successful_transmission:
                        # Send the message to the launcher
                        successful_transmission = self.factory_reset_ws.send(
                            json.dumps({'dsc_firmware_update': new[1]}))

                        if not successful_transmission:
                            self.factory_reset_ws.stop()
                            self.factory_reset_ws = FactoryResetNotifier(
                                "ws://127.0.0.1:5000/delta_solar_charger_factory_reset")
                            self.factory_reset_ws.name = 'FACTORYRESETWS'
                            self.factory_reset_ws.daemon = True
                            self.factory_reset_ws.start()

                    # First delete the entry
                    self.db.child('users').child(self.uid).child("evc_inputs").child("factory_reset").remove(
                        self.idToken)

                    # Now delete the whole user node
                    # self.db.child('users').child(self.uid).remove(self.idToken)

            elif new[0] == "manual_charge_control":
                manual_charge_rate = new[1]['charge_rate']
                manual_charge_charger_id = new[1]['chargerID']

                if manual_charge_rate == 0:
                    log('Our manual charge rate is 0, turning manual charge control off')
                    self._MANUAL_CHARGE_CONTROL = False

                else:
                    self._MANUAL_CHARGE_CONTROL = True

                    # We need to check if the charger ID is in our status list
                    if manual_charge_charger_id in self._charger_status_list:
                        try:
                            log("We have a new manual charge rate!", manual_charge_rate)

                            unique_id = random.randint(0, 10000)
                            self.ocpp_ws.send(json.dumps(
                                {'uniqueID': unique_id, 'chargerID': new[1]['chargerID'],
                                 'purpose': 'charge_rate',
                                 'charge_rate': manual_charge_rate}))

                        except ConnectionRefusedError as e:
                            log(e, 'got a connection refused error')
                            pass

            elif new[0] == "misc_command":
                data = new[1]

                # If the action of the misc command is an integrity check then perform the integrity check
                if data['action'] == 'File Integrity Check':
                    file_integrity_thread = Thread(target=self.perform_file_integrity_check)
                    file_integrity_thread.daemon = True
                    file_integrity_thread.start()

                else:
                    try:
                        self.ocpp_ws.send(json.dumps({'chargerID': data['chargerID'], 'purpose': 'misc_command',
                                                      'action': data['action'],
                                                      'misc_data': data['misc_data']}))
                    except ConnectionRefusedError as e:
                        log(e, 'got a connection refused error')
                        pass

                self.db.child("users").child(self.uid).child('evc_inputs/misc_command').remove(self.idToken)

        except Empty:
            pass

    @staticmethod
    def check_and_make_ftp_dir(ftp, directory):
        """ This function takes in a directory and checks if it exists on the FTP server, if it doesn't then m """
        _directory_check_passed = False
        while _directory_check_passed is False:
            try:
                ftp.cwd(directory)
                _directory_check_passed = True
            except error_perm as e:
                log('oops, doesnt exist: ', e)
                ftp.mkd(directory)

    def ftp_upload_charge_session(self, charger_id, charging_timestamp):
        log('Uploading', charging_timestamp, 'from', charger_id)

        try:
            with FTP(host=self._FTP_HOST) as ftp:
                ftp.login(user=self._FTP_USER, passwd=self._FTP_PW)

                self.check_and_make_ftp_dir(ftp, "/EVCS_portal/logs/" + self.uid)
                self.check_and_make_ftp_dir(ftp, "/EVCS_portal/logs/" + self.uid + '/charging_logs/')
                self.check_and_make_ftp_dir(ftp, "/EVCS_portal/logs/" + self.uid + '/charging_logs/' + charger_id)

                filename = charging_timestamp + '.csv'
                with open('../data/charging_logs/' + charger_id + '/' + filename, 'rb') as file:
                    ftp.storbinary('STOR ' + filename, file)

        except FileNotFoundError:
            log('Tried to upload but we got a FileNotFound Error!')

            # If file is not found, then we have no MeterValue so no .csv file, so we delete this charge session
            charge_date = charging_timestamp.split(' ')[0]
            charge_time = charging_timestamp.split(' ')[1]

            # Delete our Firebase history key
            self.db.child("users").child(self.uid).child('charging_history_keys').child(charger_id).child(
                charge_date).child(charge_time).remove(self.idToken)
            self.db.child("users").child(self.uid).child('analytics').child('charging_history_analytics').child(
                charger_id).child(charge_date).child(charge_time).remove(self.idToken)

    def perform_file_integrity_check(self, full_check=True):
        self.handle_charging_database(full_check)
        self.handle_inverter_database(full_check)

    def handle_charging_database(self, full_check=True):
        """ This ensures that all of the local csv charging history logs are in the ftp server """
        log('Checking our charging log databse now...')

        local_charging_folder_list = os.listdir('../data/charging_logs/')

        with FTP(host=self._FTP_HOST) as ftp:
            ftp.login(user=self._FTP_USER, passwd=self._FTP_PW)

            ############################################################################################################
            # First check if the files stored locally are also stored on the FTP server
            ############################################################################################################
            log('\nChecking if the file stored locally is also stored on the FTP server')

            self.check_and_make_ftp_dir(ftp, "/EVCS_portal/logs/" + self.uid)

            for charger_id in local_charging_folder_list:
                log('Looking at charging sessions for:', charger_id)
                local_csv_list = os.listdir('../data/charging_logs/' + charger_id)

                # If we are doing a full check, we take our complete local csv list.
                if full_check:
                    final_local_csv_list = local_csv_list

                # If we aren't doing a full check, we take 10 of our latest csv files
                else:
                    final_local_csv_list = sorted(local_csv_list, reverse=True)[:5]

                ftp_directory = "/EVCS_portal/logs/" + self.uid + '/charging_logs/' + charger_id

                # This block is to make sure that the directory exists. If not, then we have to make it
                self.check_and_make_ftp_dir(ftp, "/EVCS_portal/logs/" + self.uid + '/charging_logs/')
                self.check_and_make_ftp_dir(ftp, ftp_directory)

                # This generates a list with all of the csv files in the user's charging_log folder
                ftp_csv_list = ftp.nlst()

                # Loop through the charging sessions stored locally for this charger ID
                for filename in final_local_csv_list:
                    log(filename)

                    # Check if our local csv file is in the ftp csv charging log list
                    if filename in ftp_csv_list:
                        # If it is then we get the size of the log on the FTP server
                        filesize_ftp = ftp.size(filename)

                        # Then get the size of the log that is stored locally
                        filesize_local = os.path.getsize('../data/charging_logs/' + charger_id + '/' + filename)
                        log('ftp filesize =', filesize_ftp, 'compared to local:', filesize_local)

                        if filesize_ftp != filesize_local:
                            log('local and ftp are not the same, updating ftp')
                            with open('../data/charging_logs/' + charger_id + '/' + filename, 'rb') as file:
                                ftp.storbinary('STOR ' + filename, file)
                        else:
                            # File sizes are the same. Don't need to do anything
                            log('File sizes are the same. Moving to the next date...')

                    # If the file does not exist on the ftp server AND the file does not belong to a
                    # current charging session, we need to upload it
                    else:
                        if (charger_id not in self._charger_status_list) or (not self._charger_status_list[charger_id][
                            'charging'] and self._charger_status_list[charger_id]['charging_timestamp'] !=
                                                                             filename.split('.')[0]):
                            log(filename, 'does not exist on ftp server, upload it now')
                            with open('../data/charging_logs/' + charger_id + '/' + filename, 'rb') as file:
                                ftp.storbinary('STOR ' + filename, file)

                ########################################################################################################
                # Now we need to make sure that the FTP server does not have any files that local does not have
                ########################################################################################################
                log('\nMaking sure that FTP server does not have any files that the local device does not have')

                ftp_csv_list = ftp.nlst()

                if full_check:
                    final_ftp_csv_list = ftp_csv_list

                else:
                    final_ftp_csv_list = sorted(ftp_csv_list, reverse=True)[:5]

                # Loop through the files in the ftp server
                for filename in final_ftp_csv_list:
                    if filename not in local_csv_list:
                        ftp.delete(filename)
                        log('Deleted', filename, 'from ftp as it was not on the local device')

                ########################################################################################################
                # Make sure Firebase does not have any keys for charging sessions that don't exist locally
                ########################################################################################################
                log('\nMaking sure that charging history keys does not have any keys that dont exist locally')

                # First get the charging history keys payload for the current charger ID
                try:
                    firebase_charging_history_keys_payload = self.db.child("users").child(self.uid).child(
                        "charging_history_keys").child(charger_id).get(self.idToken).val()
                except AttributeError as e:
                    firebase_charging_history_keys_payload = {}
                    log('No payload for this charger ID', e)

                if full_check:
                    final_firebase_charging_history_keys_payload = firebase_charging_history_keys_payload
                else:
                    # final_firebase_charging_history_keys_payload = self.db.child("users").child(self.uid).child(
                    #     "charging_history_keys").child(charger_id).order_by_value().limit_to_last(5).get(
                    #     self.idToken).val()

                    final_firebase_charging_history_keys_payload = OrderedDict(list(
                        OrderedDict(sorted(firebase_charging_history_keys_payload.items(), key=lambda item: item[0],
                                           reverse=True)).items())[0:5])

                    # log(final_firebase_charging_history_keys_payload)
                    # if firebase_charging_history_keys_payload:
                    #     final_firebase_charging_history_keys_payload = OrderedDict(
                    #         list(firebase_charging_history_keys_payload.items())[-5:])
                    # else:
                    #     final_firebase_charging_history_keys_payload = firebase_charging_history_keys_payload

                # If a payload exists then we can go through the dates and extract the individual charging times
                if final_firebase_charging_history_keys_payload:
                    for date in final_firebase_charging_history_keys_payload:
                        for charging_time in list(final_firebase_charging_history_keys_payload[date].keys()):

                            # Define the csv name that we will be looking for in the local folder
                            firebase_csv_name = date + ' ' + charging_time + '.csv'

                            log('Checking:', firebase_csv_name, 'from Firebase')

                            # If csv file name from charging history keys doesn't exist locally, then delete the key
                            if firebase_csv_name not in local_csv_list:
                                log(charger_id, date, charging_time,
                                    'does not exist locally, delete the Firebase key')

                                # Remove the charging history key
                                self.db.child("users").child(self.uid).child("charging_history_keys").child(
                                    charger_id).child(date).child(charging_time).remove(self.idToken)

                                # Remove the charging history analytic entry (if there is one)
                                self.db.child("users").child(self.uid).child('analytics').child(
                                    'charging_history_analytics').child(charger_id).child(date).child(
                                    charging_time).remove(self.idToken)

                ########################################################################################################
                # Now make sure that Firebase charging_history_keys contains all the charge sessions stored locally
                ########################################################################################################
                log('\nNow checking if charging history keys contains all of the charging sessions stored locally')

                # Loop through all of the charging sessions stored locally for this charger ID
                for filename in final_local_csv_list:
                    log('Checking if', filename, 'exists in charging history keys')

                    # Split out filename into charging date and charging time
                    charging_date = filename.split('.')[0].split(' ')[0]
                    charging_time = filename.split('.')[0].split(' ')[1]

                    # First check if the charging date exists in the charging history keys payload
                    if charging_date in firebase_charging_history_keys_payload:

                        # If it does exist, then check if the charging time is NOT in the charging date
                        if charging_time not in firebase_charging_history_keys_payload[charging_date]:
                            # If it is not, then we know to add it into Firebase
                            log(filename, 'does not exist. Lets add it')
                            self.db.child("users").child(self.uid).child("charging_history_keys").child(
                                charger_id).child(charging_date).update({charging_time: True}, self.idToken)

                    # If our charging date does not exist, then we should add the charging session by default
                    else:
                        log(charging_date, 'does not exist in charging history keys. Must add charging session')
                        self.db.child("users").child(self.uid).child("charging_history_keys").child(
                            charger_id).child(charging_date).update({charging_time: True}, self.idToken)

                ########################################################################################################
                # Now make sure that Firebase charging_history_analytics contains analytics for every session in keys
                ########################################################################################################

    @staticmethod
    def analyze_csv_integrity(csv_filename):
        """ This function analyzes the csv file and makes sure that there are no NULL bytes """

        def fix_nulls(s):
            """ This function takes a generator that replaces NULL bytes with blank """
            for line in s:
                yield line.replace('\0', '')

        try:
            fixed_csv_list = None

            with open('../data/logs/' + csv_filename) as csvfile:
                if '\0' in csvfile.read():
                    log('We found a null byte, lets fix it')

                    # Reset our csv iterator
                    csvfile.seek(0)

                    # Remove the NULL bytes from the csv file
                    fixed_csv = csv.reader(fix_nulls(csvfile))
                    fixed_csv_list = list()

                    # Now save the rows to a list
                    for row in fixed_csv:
                        fixed_csv_list.append(row)

            # If a list exists then we know that there was something wrong so we need to rewrite the csv file
            if fixed_csv_list:
                # Open the corrupt csv
                with open('../data/logs/' + csv_filename, mode='w', newline='') as f:
                    writer = csv.writer(f)

                    # Write the uncorrupt rows to it and replace the file
                    for row in fixed_csv_list:
                        if len(row) != 0:
                            writer.writerow(row)

                log('Fixed the csv file!!')

        except FileNotFoundError as e:
            log('File doesnt exist! Skipping integrity check')

    def handle_inverter_database(self, full_check=True):
        """ This ensures that all of the local csv files are in the FTP server """
        log('\nChecking our inverter_logs database now...')

        # Post the current date to history_keys
        self.db.child("users").child(self.uid).child("history_keys").update(
            {datetime.now().strftime("%Y-%m-%d"): True}, self.idToken)

        # Get a list of the .csv files in the local directory
        local_csv_list = os.listdir('../data/logs/')

        ################################################################################################################
        # First look through 30 of the previous log files and check that all files are valid (have actual data)
        ################################################################################################################
        # Sort the local csv list from newest to oldest
        sorted_local_csv_list = sorted(local_csv_list, reverse=True)

        # Loop through the last 3 days
        for filename in sorted_local_csv_list[0:4]:

            # Make sure we are not looking at today's csv file
            if filename != datetime.now().strftime("%Y-%m-%d"):

                # Look for NULL bytes and fix them
                self.analyze_csv_integrity(filename)

                # Open the file and count the number of rows in each file
                with open('../data/logs/' + filename) as f:
                    csv_reader = csv.reader(f)
                    row_count = sum(1 for row in csv_reader)

                    # Files with only one row don't have any data, so we need to delete inverter history file
                    if row_count == 1:
                        log(filename, 'only has one row. We need to delete the csv file')
                        os.remove('../data/logs/' + filename)

        # Refresh our list of the .csv files in the local directory
        local_csv_list = os.listdir('../data/logs/')

        ################################################################################################################
        # Make sure that the FTP server has all of the local inverter history csv files
        ################################################################################################################
        # Download a list of .csv files that exist for the user
        with FTP(host=self._FTP_HOST) as ftp:
            ftp.login(user=self._FTP_USER, passwd=self._FTP_PW)
            ftp_directory = "/EVCS_portal/logs/" + self.uid + '/inverter_logs/'

            # First check if the uid for the logs folder exists
            self.check_and_make_ftp_dir(ftp, "/EVCS_portal/logs/" + self.uid)
            self.check_and_make_ftp_dir(ftp, ftp_directory)

            # This generates a list with all of the csv files in the user's log folder
            ftp_csv_list = ftp.nlst()

            # If we are doing a full check, we take our complete local csv list.
            if full_check:
                final_local_csv_list = local_csv_list

            # If we aren't doing a full check, we take 10 of our latest csv files
            else:
                final_local_csv_list = sorted(local_csv_list, reverse=True)[:5]

            for filename in final_local_csv_list:
                # If the looped date is NOT today
                if filename.split('.')[0] != datetime.now().strftime("%Y-%m-%d"):
                    log(filename, 'is a valid date. Checking integrity of csv on our server...')

                    # First check if the file exists in the ftp server
                    if filename in ftp_csv_list:
                        # Get size of ftp csv file for that valid date, compare to size of local csv file.
                        # If they mismatch, upload the local one of that date to the ftp server
                        filesize_ftp = ftp.size(filename)
                        filesize_local = os.path.getsize('../data/logs/' + filename)
                        log('ftp filesize =', filesize_ftp, 'compared to local:', filesize_local)
                        if filesize_ftp != filesize_local:
                            log('local and ftp are not the same, updating ftp')
                            with open('../data/logs/' + filename, 'rb') as file:
                                ftp.storbinary('STOR ' + filename, file)
                        else:
                            # File sizes are the same. Don't need to do anything
                            log('File sizes are the same. Moving to the next date...')

                    # If the file does not exist on the ftp server (something has gone wrong), we need to upload it
                    else:
                        log(filename, 'does not exist on ftp server, upload it now')
                        with open('../data/logs/' + filename, 'rb') as file:
                            ftp.storbinary('STOR ' + filename, file)

            ############################################################################################################
            # Now make sure that the server does not have any files that local does not have
            ############################################################################################################
            ftp_csv_list = ftp.nlst()

            if full_check:
                final_ftp_csv_list = ftp_csv_list

            else:
                final_ftp_csv_list = sorted(ftp_csv_list, reverse=True)[:5]

            # So loop through all of the files in the ftp server
            for filename in final_ftp_csv_list:

                # If the file in the ftp server is not in the complete local csv list, then we can delete it from FTP
                if filename not in local_csv_list:
                    ftp.delete(filename)
                    log('Deleted', filename, 'from ftp as it was not on the local device')

        ################################################################################################################
        # Now that the FTP server has been synced with the local files, make sure Firebase has the correct keys
        ################################################################################################################
        try:
            firebase_csv_list = list(
                self.db.child("users").child(self.uid).child("history_keys").get(self.idToken).val().keys())

        except AttributeError as e:
            firebase_csv_list = []
            log('None detected!', e)

        # If we are doing a full check then we get all history_keys
        if full_check:
            final_firebase_csv_list = firebase_csv_list

        # If we aren't doing a full check then we only get the last ten history_keys
        else:
            final_firebase_csv_list = sorted(firebase_csv_list, reverse=True)[:5]

        ################################################################################################################
        # Remove all entries in Firebase that are not in the local directory and do not belong to today
        ################################################################################################################
        for firebase_csv_name in final_firebase_csv_list:
            log('Checking:', firebase_csv_name, 'from Firebase')
            if (firebase_csv_name + '.csv' not in local_csv_list) and firebase_csv_name != datetime.now().strftime(
                    "%Y-%m-%d"):
                log('I cant find ' + firebase_csv_name + "in", local_csv_list, 'remove it from Firebase')
                self.db.child("users").child(self.uid).child("history_keys").child(firebase_csv_name).remove(
                    self.idToken)

        ################################################################################################################
        # Go through all of the csv files in our local folder and check if there is an entry in Firebase
        ################################################################################################################
        for filename in final_local_csv_list:
            shortened_filename = filename.split('.')[0]
            if shortened_filename not in firebase_csv_list:
                log(shortened_filename, 'is not in', firebase_csv_list, 'lets upload it')
                self.db.child("users").child(self.uid).child("history_keys").update({shortened_filename: True},
                                                                                    self.idToken)
        ################################################################################################################
        # Finally we need to remove history from 2 days ago to save space
        ################################################################################################################
        firebase_csv_name = (datetime.now() - timedelta(2)).strftime("%Y-%m-%d")
        self.db.child("users").child(self.uid).child("history").child(firebase_csv_name).remove(self.idToken)

        # # Finally we need to delete the unnecessary entries in history to save space
        # for firebase_csv_name in firebase_csv_list:
        #     log('Checking:', firebase_csv_name, 'from history')
        #     if firebase_csv_name != datetime.now().strftime("%Y-%m-%d"):
        #         self.db.child("users").child(self.uid).child("history").child(firebase_csv_name).remove(self.idToken
        #         )
        #     else:
        #         log(firebase_csv_name, 'it is today, dont need to delete')

    def update_external_sources(self, update_package):
        """ This function updates all of the different Firebase lists with up to date data """

        # (If we're online) If the day is different, then we need to run special code to fix our database up
        day = datetime.now().day
        if self._ONLINE and day != self.today:
            csv_thread = Thread(target=self.perform_file_integrity_check, args=(False,))
            csv_thread.daemon = True
            csv_thread.start()

            self.today = day

        # Check our information buses
        self.check_status_information_bus()
        self.check_information_bus()

        label = update_package[0]
        payload = update_package[1]
        current_time = datetime.now()

        if label == "modbus_data":
            # Get the modbus data, condition it so it matches our live database and history structure
            # firebase_ready_data has more data than history_ready_data!!
            (firebase_ready_data, history_ready_data) = self.condition_data(payload)

            # latest_modbus_data is used for logging and firebase purposes
            self.latest_modbus_data = history_ready_data

            # Put our latest modbus data on to the queue for other threads in FirebaseMethods
            self.modbus_data_queue.append(firebase_ready_data)

            # We log everytime log_counter reaches log_counter_max
            if self.log_counter == self.log_counter_max:
                _LOG_FILE_NAME = current_time.strftime('%Y-%m-%d')
                self.log_data(location='../data/logs/' + _LOG_FILE_NAME, purpose='log_inverter_data',
                              data=firebase_ready_data)
                self.log_counter = 0

            self.log_counter += 1

            if self._ONLINE:
                # (If we are online) Now push it to the live database - more for debug purposes (not limiting data)
                if not self._LIMIT_DATA:
                    try:
                        self.db.child("users").child(self.uid).child("live_database").update(firebase_ready_data,
                                                                                             self.idToken)
                    except OSError as e:
                        log('update_firebase live database timed out', e)
                        self.handle_internet_check()

                # (If we are online) Push data to history so we can bring it up in the future
                if self.history_counter == self.history_counter_max:
                    try:
                        self.db.child("users").child(self.uid).child("history").child(
                            current_time.strftime("%Y-%m-%d")).push(history_ready_data, self.idToken)
                    except OSError as e:
                        log('update_firebase history timed out', e)
                        self.handle_internet_check()

                    self.history_counter = 0

                self.history_counter += 1

        # WebSocket is local so we do not have to be online
        elif label == "update_charge_rate":
            # log('Got a update charge rate payload', payload)

            # charge_rates will be the list of new charge rates that should be delivered to the charger through ws
            self.charge_rates = payload['charge_rates']

            # We have an available current figure which we use for our charging firewall
            self._AVAILABLE_CURRENT = payload['available_current']

            # If our websocket connection isn't broken then we run the code to send
            if not self.ws_receiver_stopped_event.is_set():

                # Send our new charge rate out through websocket
                try:
                    # Only send our charge rate to websocket if we are not in manual charge control
                    if not self._MANUAL_CHARGE_CONTROL:
                        for chargerID in self.charge_rates:
                            log('Looking at', chargerID, 'with a charge rate of', self.charge_rates[chargerID])

                            unique_id = random.randint(0, 1000)
                            try:
                                self.ocpp_ws.send(json.dumps(
                                    {'uniqueID': unique_id, 'chargerID': chargerID,
                                     'purpose': 'charge_rate',
                                     'charge_rate': self.charge_rates[chargerID]}))

                            except websocket.WebSocketConnectionClosedException as e:
                                log(e)

                            # Double check to makes sure the chargerID has not disconnected
                            if chargerID in self._charger_status_list:
                                # Loop through our charger status list and set the charge rate
                                self._charger_status_list[chargerID]['charge_rate'] = self.charge_rates[chargerID]

                # If there is no connection, then we just pass.
                except ConnectionRefusedError as e:
                    log(e, 'got a connection refused error')
                    pass

            else:
                # If ws receiver stopped event is set then we need to try to restart
                self.ocpp_ws.stop()

                log('stopped!', self.ocpp_ws)
                self.ocpp_ws = OCPPDataBridge("ws://127.0.0.1:8000/ocpp_data_service/", self.information_bus,
                                              self.charger_status_information_bus,
                                              self.ws_receiver_stopped_event)
                self.ocpp_ws.name = 'OCPPWS'
                self.ocpp_ws.daemon = True
                self.ocpp_ws.start()
                log('Started a new OCPP WS Receiver')

        elif label == "update_charge_mode":
            new_charge_mode = payload

            log('Updating the charge mode from analyze_to_firebase queue')
            # (If we are online) then we update our charge mode in Firebase
            if self._ONLINE:
                try:
                    self.db.child("users").child(self.uid).child('evc_inputs/charging_modes').update(
                        {'single_charging_mode': new_charge_mode}, self.idToken)
                    log('Updated charge mode in Firebase - check if web interface is showing this change')
                except OSError as e:
                    log('update_firebase update charge mode timed out', e)
                    self.handle_internet_check()

        # Updating analytics requires us to be online
        elif self._ONLINE and label == "analytics_data":
            payload.update({'time': str(current_time)})

            # We will update analytics data every "webanalytics_counter_max" seconds
            if self.webanalytics_counter == self.webanalytics_counter_max:
                log('updating analytics')

                try:
                    self.db.child("users").child(self.uid).child("analytics/live_analytics").update(payload,
                                                                                                    self.idToken)
                except OSError as e:
                    log('update_firebase live analytics timed out', e)
                    self.handle_internet_check()

                self.webanalytics_counter = 0

            self.webanalytics_counter += 1
