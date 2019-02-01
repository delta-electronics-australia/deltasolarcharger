""" Delta Solar Charger Backend v0.80
    Written by Benjamin Ong for Delta Electronics in collaboration with the CSIRO """

import subprocess
import psutil
import time
import threading
import os
import sys
import time
import requests
from ftplib import FTP
from io import StringIO

import sqlite3

import asyncio
import tornado.web
import tornado.websocket
import tornado.ioloop
from tornado.escape import json_decode
from json import dumps, loads

# Define our FTP parameters
_FTP_HOST = "203.32.104.46"
_FTP_PORT = 21
_FTP_USER = 'delta'
_FTP_PW = 'P@ssw8rd'

# The ONLINE flag determines whether or not our system should run with or without the internet
_ONLINE = True
# The limit data flag determines whether or not we should limit data usage - for heavily metered connections
_LIMIT_DATA = False


class ConfigServer(tornado.web.Application):

    def __init__(self):
        handlers = [(r"/delta_solar_charger_initial_setup", InitialSetupHandler),
                    (r"/delta_solar_charger_software_update", SoftwareUpdateHandler),
                    (r"/delta_solar_charger_factory_reset", FactoryResetHandler)]
        settings = {'debug': True}
        super().__init__(handlers, **settings)

    def run(self):
        self.listen(5000)
        tornado.ioloop.IOLoop.instance().start()


class FactoryResetHandler(tornado.web.RequestHandler):
    def post(self):
        print("Post in factory reset handler!")
        decoded_message = json_decode(self.request.body)

    def perform_factory_reset(self):
        # Todo: delete the data folder, remove whole user node in Firebase - need to test if this can be recovered
        pass


class SoftwareUpdateHandler(tornado.websocket.WebSocketHandler):

    def open(self):
        print('SoftwareUpdateHandler open!')

    def on_message(self, message):
        print('Received a message for software update!', message)

        decoded_message = loads(message)
        if decoded_message['dsc_firmware_update']:
            print('doing an update now!')
            check_for_updates()
            pass


class InitialSetupHandler(tornado.web.RequestHandler):

    def get(self):
        print('get!')
        self.write("hello")

    def post(self):
        initial_setup_payload = json_decode(self.request.body)

        print('We got an initial setup message!', initial_setup_payload)

        return_message = self.handle_initial_setup(initial_setup_payload)

        # Now that we have handled the initial setup, we send the result back to the app
        response = dumps({'success': return_message})
        self.write(response)

        # If our return message is True, then that means it was a success
        if return_message is True:
            print('We have officially completed initial setup. Lets kill proceses now...')
            # Now kill all solar charger processes that are running
            kill_sc_backend()

        # If a configuration file already exists, then we need to tell the app that we cannot initialize the DSC
        elif return_message == "config exists":
            print('Config file already exists, sending a fail')
            pass

    def handle_initial_setup(self, initial_setup_payload):
        """ This functions all of the initial setup """

        print('Initial setup payload is:', initial_setup_payload)

        # See if a config file already exists
        if os.path.exists('/home/pi/deltasolarcharger/config/config.sqlite'):
            # Then we reject the initial setup
            return "config exists"
        else:

            # Write our payload into an sqlite DB file
            self.write_to_sqlite(initial_setup_payload)
            return True

    @staticmethod
    def write_to_sqlite(initial_setup_payload):
        """ This function writes our initial setup payload into a sqlite file """

        # Check if we have the folder for the config file
        if not os.path.exists('/home/pi/deltasolarcharger/config/'):
            os.mkdir('/home/pi/deltasolarcharger/config')

        # Initialize our sqlite DB
        conn = sqlite3.connect('../config/config.sqlite')
        c = conn.cursor()

        # Clear the table completely for initial setup
        c.execute('''DROP TABLE IF EXISTS unnamed''')

        # Create table
        c.execute('''CREATE TABLE IF NOT EXISTS unnamed (key text, value text)''')

        # Insert the data row by row
        for db_key, db_value in initial_setup_payload.items():
            c.execute("INSERT INTO unnamed (key, value) values (?, ?)", (db_key, db_value))

        # Close the DB
        conn.commit()
        conn.close()


class ModifySettingsHandler(tornado.web.RequestHandler):

    def post(self):
        print('post!')
        print(json_decode(self.request.body))
        response = dumps({'success': True})
        self.write(response)


def configure_3g_settings():
    """ This function sets our IP tables when a 3G connection is detected """

    celluar_network_interfaces = ['ppp0', 'wwan0']

    for i in range(60):
        network_interfaces = psutil.net_if_addrs().keys()

        celluar_interface = [interface for interface in celluar_network_interfaces if interface in network_interfaces]

        print(celluar_interface)

        if len(celluar_interface) == 1:

            # First remove all existing rules in IP tables
            for interface in ['ppp0', 'wwan0']:
                os.system('sudo iptables -t nat -D POSTROUTING -o ' + interface + ' -j MASQUERADE')
                os.system('sudo iptables -D FORWARD -i ' + interface +
                          ' -o wlan0 -m state --state RELATED,ESTABLISHED -j ACCEPT')
                os.system('sudo iptables -D FORWARD -i wlan0 -o ' + interface + ' -j ACCEPT')

            # Now add the rules that we want
            if celluar_interface[0] == "ppp0" or celluar_interface[0] == "wwan0":
                os.system('sudo iptables -t nat -A POSTROUTING -o ' + celluar_interface[0] + ' -j MASQUERADE')
                os.system('sudo iptables -A FORWARD -i ' + celluar_interface[0] +
                          ' -o wlan0 -m state --state RELATED,ESTABLISHED -j ACCEPT')
                os.system('sudo iptables -A FORWARD -i wlan0 -o ' + celluar_interface[0] + ' -j ACCEPT')
                break
            else:
                print("Odd interface...")

        time.sleep(1)

    global _LIMIT_DATA
    _LIMIT_DATA = True


def check_internet():
    """ This function checks whether or not there is an internet connection for one minute"""
    # First check if we are on 3G or not 3G
    sql_db = sqlite3.connect('../config/config.sqlite')
    firebase_cred = dict()
    # Write the DB to a dict
    for row in sql_db.execute("SELECT key, value FROM unnamed"):
        firebase_cred.update({row[0]: row[1]})

    # If the user has selected connectionMethod as 'none' then we simply set internet status to False
    if firebase_cred['connectionMethod'] == 'none':
        return False

    # Check if the system is set for a 3G connection
    elif 'connectionMethod' in firebase_cred and firebase_cred["connectionMethod"] == "3G":
        # Then we need to configure some 3G settings - writing to IP tables so chargers can have internet
        configure_3g_settings()

    # Now we ping Google to check if the internet is up. If after 2 minutes it is still not up, then _ONLINE = False
    internet_status = False
    for i in range(60):
        try:
            response = requests.get("http://www.google.com")
            print("response code: " + str(response.status_code))
            internet_status = True
            break

        except requests.ConnectionError:
            print("Could not connect, trying again 3 seconds...")
            internet_status = False
            time.sleep(2)

    return internet_status


def restart():
    """ This script restarts the OCPP backend and the solar charger back end """
    kill_ocpp_backend()
    kill_sc_backend()
    os.system(
        'lxterminal --working-directory=/home/pi/deltasolarcharger/deltasolarcharger/ocppserver -e sudo python3 ocppserver.py &')
    os.execv(sys.executable, ['python3'] + sys.argv)


def kill_ocpp_backend():
    """ This function will kill the OCPP backend """

    # List all of the current processes
    for proc in psutil.process_iter(attrs=['pid', 'name']):
        process_command = proc.cmdline()

        # Look for the process with our OCPP backend in it and kill it
        if 'ocppserver.py' in process_command:
            proc.kill()


def kill_sc_backend():
    """ This function will kill the whole Delta Solar Charger back end """

    try:
        # First check if a solar charger process exists
        if solar_charger_process is not None:

            # Then kill all of the processes
            process = psutil.Process(solar_charger_process.pid)
            for proc in process.children(recursive=True):
                proc.kill()
            process.kill()
    except psutil.NoSuchProcess:
        pass


def check_latest_version():
    """ This function checks for the latest version number of the software """

    with FTP(host=_FTP_HOST) as ftp:
        ftp.login(user=_FTP_USER, passwd=_FTP_PW)
        directory = "/deltasolarcharger/docs"

        # We are looking for a specific file - version.txt
        filematch = 'version.txt'
        ftp.cwd(directory)

        for file_name in ftp.nlst(filematch):
            r = StringIO()
            ftp.retrlines('RETR ' + file_name, r.write)
            return float(r.getvalue())


def download_from_ftp():
    """ This function goes into the FTP server and downloads all of the Python Scripts """

    with FTP(host=_FTP_HOST) as ftp:
        ftp.login(user=_FTP_USER, passwd=_FTP_PW)

        # First download deltasolarcharger.py
        ftp.cwd("/deltasolarcharger/deltasolarcharger/")
        with open('/home/pi/deltasolarcharger/deltasolarcharger/deltasolarcharger.py', 'wb') as file:
            ftp.retrbinary('RETR ' + 'deltasolarcharger.py', file.write)

        # Then download all of the files in dschelpers
        ftp.cwd("/deltasolarcharger/deltasolarcharger/dschelpers")
        for file_name in ftp.nlst('*.py'):
            with open('/home/pi/deltasolarcharger/deltasolarcharger/dschelpers/' + file_name, 'wb') as file:
                print('updated ' + file_name)
                ftp.retrbinary('RETR ' + file_name, file.write)

        # Then download the OCPP backend scripts
        ftp.cwd("/deltasolarcharger/deltasolarcharger/ocppserver/")
        with open("/home/pi/deltasolarcharger/deltasolarcharger/ocppserver/ocppserver.py", 'wb') as file:
            ftp.retrbinary('RETR ' + 'ocppserver.py', file.write)
        with open("/home/pi/deltasolarcharger/deltasolarcharger/ocppserver/response_database.py", 'wb') as file:
            ftp.retrbinary('RETR ' + 'response_database.py', file.write)

        # Finally, update our software version
        ftp.cwd("/deltasolarcharger/docs/")
        with open('/home/pi/deltasolarcharger/docs/version.txt', 'wb') as file:
            ftp.retrbinary('RETR ' + 'version.txt', file.write)

    return True


def check_for_updates():
    """ This function checks for updates and performs an update if necessary """

    print("Performing a software update")
    latest_version = check_latest_version()

    with open('../docs/version.txt', 'r') as f:
        current_version = float(f.read())
        print('The latest version of the firmware is:', latest_version, 'current version is:', current_version)

    if latest_version > current_version:
        print('Newer version detected. Updating now...')

        # If there is a newer version detected, we have to download it from our FTP server
        download_success = download_from_ftp()

        # After we finish the update, we must run start.sh and immediately kill the OCPP backend, SC backend and start
        restart()

    else:
        print('Firmware versions the same, no need to update')


def check_credentials():
    """ This function checks if Firebase credentials exist """

    credential_stage_passed = False
    while credential_stage_passed is False:

        # If there is no firebase credentials file then we need to run the initial setup
        if not os.path.exists('../config/config.sqlite'):
            print('No credentials found, please run initial setup from the app')
            time.sleep(7)

        else:
            print('Firebase credentials found! Exiting the loop.')
            credential_stage_passed = True


def check_program_integrity():
    """ Checks if all of the Python files we need are there """

    dschelpers_file_list = ['firebasemethods.py', 'analysemethods.py', 'webanalyticsmethods.py',
                            'modbusmethods.py', '__init__.py']

    ocpp_file_list = ['ocppserver.py', 'response_database.py', '__init__.py']

    downloaded_files = False

    # First check for deltasolarcharger.py
    if os.path.exists('/home/pi/deltasolarcharger/deltasolarcharger/deltasolarcharger.py'):
        print('deltasolarcharger.py exists, moving on to dschelpers')

    # If it doesn't exist then I should download the file from the FTP server
    else:
        if _ONLINE:
            print('deltasolarcharger not found, downloading from FTP...')
            download_success = download_from_ftp()
            downloaded_files = True

    # Loop through all of the files that should exist for dschelpers
    for file in dschelpers_file_list:

        # If the file exists, then move on to the next file
        if os.path.exists('/home/pi/deltasolarcharger/deltasolarcharger/dschelpers/' + file):
            print(file, 'exists. Checking the next file...')

        # If the file doesn't exist, we need to download from FTP, break out of the loop and test again
        else:
            if _ONLINE:
                print(file, 'not found, downloading from FTP...')
                download_success = download_from_ftp()
                downloaded_files = True
            else:
                print('Need an internet connection, restarting')
                # This function simply restarts startv2.py
                os.execv(sys.executable, ['python3'] + sys.argv)

    # Loop through all of the files for the OCPP server that should exist
    for file in ocpp_file_list:

        # If the file exists, then move on to the next file
        if os.path.exists('/home/pi/deltasolarcharger/deltasolarcharger/ocppserver/' + file):
            print(file, 'exists. Checking the next file...')

        # If the file doesn't exist, we need to download from FTP
        else:
            print(file, 'not found, downloading from FTP...')
            download_success = download_from_ftp()
            downloaded_files = True

    # If we have downloaded files, then we should kill everything and restart from scratch
    if downloaded_files:
        print('Restarting the program!')
        restart()


def start_config_server():
    """ This function simply starts our config server"""

    asyncio.set_event_loop(asyncio.new_event_loop())
    config_server.run()


if __name__ == '__main__':

    # Initialize our process
    solar_charger_process = None

    ####################################################################################################################
    # **************************** The config server will be started and will continue running *********************** #
    ####################################################################################################################
    config_server = ConfigServer()
    config_server_thread = threading.Thread(target=start_config_server, args=())
    config_server_thread.daemon = True
    config_server_thread.start()
    ####################################################################################################################
    ####################################################################################################################

    # Check if we have existing credentials file, if we don't then we infinite loop until initial setup is run
    check_credentials()

    # This line checks if there's internet available.
    _ONLINE = check_internet()

    # First check if the files we need are here
    check_program_integrity()
    print('Initial stage passed')

    ''' This script starts main.py and ensures that if there are any crashes that the file restarts itself '''
    while True:
        check_for_updates()

        print("\nStarting Delta Solar Charger Backend")
        solar_charger_process = subprocess.Popen("sudo python3 deltasolarcharger.py", shell=True, stdin=subprocess.PIPE)

        # Once we start our main program, we then send whether or not we're online, in CSIRO mode or if we limit data
        solar_charger_process.communicate(
            input=bytes(dumps({'online': _ONLINE, 'LIMIT_DATA': _LIMIT_DATA}), 'UTF-8'))

        print('Waiting for Python to exit')
        solar_charger_process.wait()
