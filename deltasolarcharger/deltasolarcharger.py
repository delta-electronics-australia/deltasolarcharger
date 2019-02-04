from multiprocessing import Process, Manager

import threading

import time
from datetime import datetime
from sys import stdin
from json import loads

import random

from requests.exceptions import HTTPError, SSLError, ConnectionError
from requests.packages.urllib3.exceptions import NewConnectionError, MaxRetryError

# Add dschelpers into our path
import sys
sys.path.insert(0, './dschelpers')

from firebasemethods import FirebaseMethods
from modbusmethods import ModbusMethods
from analysemethods import AnalyseMethods
from webanalyticsmethods import WebAnalyticsMethods


# This process will handle everything to do with Modbus communications
class ModbusCommunications(ModbusMethods):
    def __init__(self, **kwargs):
        super().__init__(kwargs['analyse_to_modbus_queue'])

        # *****************
        self.kill_counter = 0
        self.kill_count = random.randint(45, 60)
        # ********************

        # This will initialize the Modbus parameters
        # self.modbus_methods = ModbusMethods()

        # Now unpack the queues we need
        self.modbus_to_firebase_queue = kwargs['modbus_to_firebase_queue']
        self.modbus_to_analyse_queue = kwargs['modbus_to_analyse_queue']
        self.modbus_to_webanalytics_queue = kwargs['modbus_to_webanalytics_queue']

        # Define our stop events
        self._stop_event = kwargs['stop_event']
        self._webanalytics_event = kwargs['webanalytics_event']

    def stop(self):
        print('tried to stop modbus')
        self._stop_event.set()
        self._webanalytics_event.set()
        print('Modbus stop signal set')

    def stopped(self):
        return self._stop_event.is_set()

    def start_transmission(self):
        error_counter = 0
        while True:
            start = time.time()
            # print('start of a new cycle!', datetime.now())

            # Check if a flag has been raised to stop the process
            if self.stopped():
                print('Modbus broken')
                break

            try:
                # This gets a dictionary of tuples of libraries that is ready to be uploaded to Firebase
                # The structure of modbus_data is: # {modbus_data: (inverter_data, bt_data, dpm_data)}
                modbus_data = self.get_modbus_data()

                # if self.kill_counter == self.kill_count:
                #     raise IOError

                # Then we upload this to Firebase by adding the library to the queue.
                self.modbus_to_firebase_queue.put(modbus_data)

                # Push the data to the Analyse process
                self.modbus_to_analyse_queue.put(modbus_data)

                # Push the data to the WebAnalytics process
                self.modbus_to_webanalytics_queue.put(modbus_data)
                self._webanalytics_event.set()

                end = time.time()
                time.sleep(1 - (end - start))

            # When we get an exception, we reinitialize the MODBUS library and keep going.
            except IOError as error:
                error_counter += 1
                if error_counter == 6:
                    self.stop()
                    break

                print("IO Error!")
                print(error)
                self.initiate_parameters(1, 5)

            except ValueError as error:
                error_counter += 1
                if error_counter == 6:
                    self.stop()
                    break

                print("Value Error!")
                print(error)
                self.initiate_parameters(1, 5)

    def run(self):
        self.start_transmission()


# This process will handle all Firebase communications. All data is piped into this thread and dealt with
class FirebaseCommunications(FirebaseMethods, Process):
    def __init__(self, **kwargs):
        super().__init__(kwargs['firebase_to_analyse_queue'], kwargs['stdin_payload'])

        # Define queues going into FirebaseCommunications
        self.modbus_to_firebase_queue = kwargs['modbus_to_firebase_queue']
        self.analyse_to_firebase_queue = kwargs['analyse_to_firebase_queue']
        self.webanalytics_to_firebase_queue = kwargs['webanalytics_to_firebase_queue']
        self.firebase_to_analyse_queue = kwargs['firebase_to_analyse_queue']

        # Define multiprocessing events
        self._webanalytics_event = kwargs['webanalytics_event']
        self._stop_event = kwargs['stop_event']

        # self.firebase = FirebaseMethods(kwargs['stop_event'], kwargs["firebase_to_analyse_queue"])

    def stop(self):
        print('tried to stop firebase')
        self.ocpp_ws.stop()

        print('Firebase broken')
        self._stop_event.set()
        self._webanalytics_event.set()

    def stopped(self):
        return self._stop_event.is_set()

    def run(self):
        while True:
            # Check if a stop event has been raised and then break out
            if self.stopped():
                print('Firebase broken')
                # Close our OCPP Websocket Client
                self.ocpp_ws.stop()
                break

            try:
                if not self.analyse_to_firebase_queue.empty():
                    data_from_analyse = self.analyse_to_firebase_queue.get()
                    # If the data from analyze is a charging mode, then we have to update charge mode, not charge rate
                    if data_from_analyse in ['MAX_CHARGE_GRID', 'MAX_CHARGE_STANDALONE', 'PV_no_BT', 'PV_with_BT']:
                        self.update_firebase(['update_charge_mode', data_from_analyse])

                    # If the type is not a tuple then it is either a charge rate or start/stop
                    else:
                        self.update_firebase(['update_charge_rate', data_from_analyse])

                if not self.modbus_to_firebase_queue.empty():
                    # First we get the data from our queue (remember: data is a tuple of dictionaries)
                    modbus_data = self.modbus_to_firebase_queue.get()
                    # Then we send it to be uploaded to Firebase
                    self.update_firebase(['modbus_data', modbus_data])

                if not self.webanalytics_to_firebase_queue.empty():
                    analytics_data = self.webanalytics_to_firebase_queue.get()
                    self.update_firebase(['analytics_data', analytics_data])

            except HTTPError as e:
                print('got a http error, laters', e, datetime.now())
                self.stop()
                break
            except SSLError as e:
                print('got an ssl error, laters', e, datetime.now())
                self.stop()
                break
            except ConnectionResetError as e:
                print('got a connection reset error, laters', e, datetime.now())
                self.stop()
                break
            except ConnectionError as e:
                print('got a connection error, laters', e, datetime.now())
                self.stop()
            except NewConnectionError as e:
                print('got a new connection error, laters', e, datetime.now())
                self.stop()
            except MaxRetryError as e:
                print('got a max retry error, laters', e, datetime.now())
                self.stop()
            except OSError as e:
                print('got a OS Error, laters', e, datetime.now())
                self.stop()


# This process will wait and react to anything from Firebase or executes actions
class Analyse(Process):
    def __init__(self, **kwargs):
        super().__init__()

        self.modbus_to_analyse_queue = kwargs['modbus_to_analyse_queue']
        self.analyse_to_firebase_queue = kwargs['analyse_to_firebase_queue']

        # # self.analyse contains all of our analysis methods
        self.analyse = AnalyseMethods(kwargs['firebase_to_analyse_queue'], kwargs['analyse_to_modbus_queue'])

        self._webanalytics_event = kwargs['webanalytics_event']
        self._stop_event = kwargs['stop_event']

    def stop(self):
        print('tried to stop analyse')
        self._stop_event.set()
        self._webanalytics_event.set()
        print('analyse stop signal sent')

    def stopped(self):
        return self._stop_event.is_set()

    def run(self):
        while True:
            if self.stopped():
                print('Analyse broken')
                break

            # When we see that the queue has been populated with something then we take action
            if not self.modbus_to_analyse_queue.empty():
                # First we get the data from our queue (remember: data is a tuple of dictionaries)
                modbus_data = self.modbus_to_analyse_queue.get()

                charge_rate = self.analyse.make_decision(modbus_data)
                self.analyse_to_firebase_queue.put(charge_rate)

        time.sleep(0.10)


class WebAnalytics(WebAnalyticsMethods, Process):
    def __init__(self, **kwargs):
        super().__init__()

        # Define our stdin variables
        stdin_payload = kwargs['stdin_payload']
        self._ONLINE = stdin_payload['online']
        self._LIMIT_DATA = stdin_payload['LIMIT_DATA']

        # Define the queues going in and out of webanalytics
        self.modbus_to_webanalytics_queue = kwargs['modbus_to_webanalytics_queue']
        self.webanalytics_to_firebase_queue = kwargs['webanalytics_to_firebase_queue']

        # Define our stop events
        self._stop_event = kwargs['stop_event']
        self._webanalytics_event = kwargs['webanalytics_event']

        # (If we are online) This function makes sure our analytics are up to date in this program
        if self._ONLINE:
            self.sync_analytics_data()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()

    def run(self):
        while True:
            # print('Webanalytics at the start of loop', self.stopped())
            if self.stopped():
                print('Webanalytics broken')
                break
            # Wait for new data to come
            self._webanalytics_event.wait()
            # Check if a stop event has been raised and then break out
            # print('Webanalytics after webanalytics event', self.stopped())
            if self.stopped():
                print('Webanalytics broken')
                break

            while not self.modbus_to_webanalytics_queue.empty():
                new_data = self.modbus_to_webanalytics_queue.get()

                if self._ONLINE:
                    # Call this function to update all our current analytics data (completely offline)
                    self.update_analytics(new_data)

                    # Now push the new data to the Firebase worker to be uploaded to Firebase
                    self.webanalytics_to_firebase_queue.put(self.current_analytics_data)

                if self.stopped():
                    print('Webanalytics broken')
                    break

            # Clear the event and wait for the next event to be signalled
            self._webanalytics_event.clear()


def main():
    with open('../docs/version.txt', 'r') as f:
        current_version = float(f.read())
        print('Running software version:', current_version)

    # Read from stdin and get the data that has been sent from start.py
    stdin_payload = loads(stdin.read())

    # Create a manager to manage all our multiprocess queues and events
    process_manager = Manager()

    # Create our Queues to transmit information between processes
    _modbus_to_firebase_queue = process_manager.Queue()
    _modbus_to_analyse_queue = process_manager.Queue()
    _modbus_to_webanalytics_queue = process_manager.Queue()

    _analyse_to_modbus_queue = process_manager.Queue()
    _analyse_to_firebase_queue = process_manager.Queue()

    _firebase_to_modbus_queue = process_manager.Queue()
    _firebase_to_analyse_queue = process_manager.Queue()

    _webanalytics_to_firebase_queue = process_manager.Queue()

    # Package it into one dictionary
    queue_kwargs = {"modbus_to_firebase_queue": _modbus_to_firebase_queue,
                    "modbus_to_analyse_queue": _modbus_to_analyse_queue,
                    'modbus_to_webanalytics_queue': _modbus_to_webanalytics_queue,

                    "analyse_to_modbus_queue": _analyse_to_modbus_queue,
                    "analyse_to_firebase_queue": _analyse_to_firebase_queue,

                    "firebase_to_modbus_queue": _firebase_to_modbus_queue,
                    "firebase_to_analyse_queue": _firebase_to_analyse_queue,

                    'webanalytics_to_firebase_queue': _webanalytics_to_firebase_queue}

    queue_kwargs.update({'stdin_payload': stdin_payload})

    # Create a multiprocessing stop event. This event will be raised whenever any process has an exception
    _stop_event = process_manager.Event()
    _webanalytics_event = process_manager.Event()

    queue_kwargs.update({'stop_event': _stop_event,
                         'webanalytics_event': _webanalytics_event,
                         })

    # Define and start our processes
    webanalytics_process = WebAnalytics(**queue_kwargs)
    modbuscommunications_process = ModbusCommunications(**queue_kwargs)
    firebasecommunications_process = FirebaseCommunications(**queue_kwargs)
    analyse_process = Analyse(**queue_kwargs)

    print('Initialization of processes is done. Starting all processes...')

    webanalytics_process.start()
    firebasecommunications_process.start()
    analyse_process.start()
    modbuscommunications_process.run()

    # Wait for the processes to end if a stop event is raised
    #################
    webanalytics_process.join()
    print('Web Analytics joined', threading.enumerate(), datetime.now())
    #################

    try:
        print('Closing charging modes listener from main')
        firebasecommunications_process.charging_modes_listener.close()
        firebasecommunications_process.charging_modes_listener = None
    except AttributeError as e:
        print(e)
    try:
        print('Closing buffer agro listener from main')
        firebasecommunications_process.buffer_aggressiveness_listener.close()
        firebasecommunications_process.buffer_aggressiveness_listener = None
    except AttributeError as e:
        print(e)
    try:
        print('Closing update firmware listener from main')
        firebasecommunications_process.update_firmware_listener.close()
        firebasecommunications_process.update_firmware_listener = None
    except AttributeError as e:
        print(e)
    try:
        print('Closing dsc firmware update listener from main')
        firebasecommunications_process.dsc_firmware_update_listener.close()
        firebasecommunications_process.dsc_firmware_update_listener = None
    except AttributeError as e:
        print(e)
    try:
        print('Closing delete charger listener from main')
        firebasecommunications_process.delete_charger_listener.close()
        firebasecommunications_process.delete_charger_listener = None
    except AttributeError as e:
        print(e)
    try:
        print('Closing factory reset listener from main')
        firebasecommunications_process.factory_reset_listener.close()
        firebasecommunications_process.factory_reset_listener = None
    except AttributeError as e:
        print(e)
    try:
        print('Closing misc listener listener from main')
        firebasecommunications_process.misc_listener.close()
        firebasecommunications_process.misc_listener = None
    except AttributeError as e:
        print(e)

    print(firebasecommunications_process.refresh_timer, datetime.now())
    firebasecommunications_process.refresh_timer.cancel()
    print('refresh timer cancelled', firebasecommunications_process.refresh_timer, datetime.now())

    print(firebasecommunications_process.exitcode)
    # If the process is None then it has not been terminated
    if not firebasecommunications_process.exitcode:
        firebasecommunications_process.terminate()

    #################
    firebasecommunications_process.join()
    print('Firebase joined', threading.enumerate(), datetime.now())
    #################

    print(firebasecommunications_process.refresh_timer, datetime.now())
    firebasecommunications_process.refresh_timer.cancel()
    print('refresh timer cancelled', firebasecommunications_process.refresh_timer, datetime.now())

    print(analyse_process.exitcode)
    # If the process is None then it has not been terminated
    if not analyse_process.exitcode:
        analyse_process.terminate()

    #################
    analyse_process.join()
    print('Analyse joined', threading.enumerate(), datetime.now())
    #################

    print(firebasecommunications_process.refresh_timer, datetime.now())
    firebasecommunications_process.refresh_timer.cancel()
    print('refresh timer cancelled', firebasecommunications_process.refresh_timer, datetime.now())

    # Check what other threads are still running
    print(threading.enumerate())

    print('We are out of the program')

    exit(0)


if __name__ == '__main__':
    main()
