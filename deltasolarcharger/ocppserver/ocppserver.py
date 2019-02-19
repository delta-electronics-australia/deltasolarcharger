""" Delta Solar Charger OCPP 1.6J Backend
    Written by Benjamin Ong for Delta Electronics in collaboration with the CSIRO """
import tornado.web
import tornado.websocket
import tornado.ioloop
from tornado import gen

import json
from random import randint
from datetime import datetime, timedelta

from collections import deque

import response_database

_DEBUG = False


class ws_clients_wrapper:
    def __init__(self):
        print("Initializing the ws clients wrapper!", self)
        # ws_clients is the set of clients that exists at the moment
        self.ws_clients = set()

        # data_handler will be...
        self.data_handler = [None]


class httpHandler(tornado.web.RequestHandler):

    def on_finish(self):
        print('finished')

    def get(self):
        print('sup')
        print('hoola~!')
        self.render("index.html")

    def on_connection_close(self):
        print('closed...')


class WebSocketHandler(tornado.websocket.WebSocketHandler):

    def lookup_action_database(self, action, optional=False):
        if action == "Authorize":
            # Before authorizing, we need to go through the firewall. This checks the mode to allow or disallow charging
            # authorize_status = self.charging_firewall()

            # If the optional input is True, then we accept the charging session
            if optional:
                authorize_status = 'Accepted'
            else:
                authorize_status = 'Blocked'

            return response_database.Authorize(authorize_status=authorize_status)

        elif action == "BootNotification":
            print('Boot notification!!!')
            print(self.decoded_message[3])
            self.new_charger_info = self.decoded_message[3]

            # If a valid data_handler exists (meaning our E5 is sending messages to this charge point)
            if self.data_handler[0]:
                try:
                    self.data_handler[0].write_message(json.dumps({"new_charger_info": self.new_charger_info,
                                                                   "chargerID": self._CHARGER_ID}))
                except tornado.websocket.WebSocketClosedError as e:
                    print(e)
                    pass
            return response_database.BootNotification(interval=90, status='Accepted')

        elif action == "ChangeAvailability":
            return response_database.ChangeAvailability(optional)

        elif action == "ChangeConfiguration":
            return response_database.ChangeConfiguration(optional)

        elif action == "ClearChargingProfile":
            return response_database.ClearChargingProfile(optional)

        elif action == "DiagnosticsStatusNotification":
            return response_database.DiagnosticsStatusNotification()

        elif action == "FirmwareStatusNotification":
            self.data_handler[0].write_message(
                json.dumps({"fw_status": self.decoded_message[3]['status'], "chargerID": self._CHARGER_ID}))

            return response_database.FirmwareStatusNotification()

        elif action == "GetCompositeSchedule":
            return response_database.GetCompositeSchedule(optional)

        elif action == "GetConfiguration":
            return response_database.GetConfiguration(optional)

        elif action == "GetDiagnostics":
            return response_database.GetDiagnostics()

        elif action == "GetLocalListVersion":
            return response_database.GetLocalListVersion()

        elif action == "Heartbeat":
            return response_database.Heartbeat()

        elif action == "MeterValues":
            sampledValues = self.decoded_message[3]['meterValue'][0]

            # print('TransactionID from payload is', self.decoded_message[3]['transactionId'], 'compared to',
            #       self.transaction_id)

            # Todo: this one might have to change if we want to allow offline recovery
            # Only run MeterValues if we are charging
            if self._isCharging:
                # Loop through the message to find the current value
                for metervalue_dict in sampledValues['sampledValue']:
                    if metervalue_dict['measurand'] == "Current.Import":
                        current = metervalue_dict['value']

                        # If the current value is less than 0.2 (zero basically) then we increment a counter
                        if float(current) < 0.20:
                            self.zero_counter += 1
                            print('zero counter incremented!', self.zero_counter)
                        else:
                            self.zero_counter = 0

                # If it has been over 5 minutes of zeros
                if self.zero_counter > 30:
                    # We want to remotely stop the charging session
                    self.charge_rate_queue.append('stop')

            if self.data_handler[0] and self._isCharging:
                try:
                    # Todo: send the transaction ID to the backend
                    self.data_handler[0].write_message(
                        json.dumps({"meter_values": sampledValues, "chargerID": self._CHARGER_ID}))
                except tornado.websocket.WebSocketClosedError as e:
                    print(e)
                    pass
                return response_database.MeterValues(self.decoded_message)

        elif action == "RemoteStartTransaction":
            return response_database.RemoteStartTransaction()

        elif action == "RemoteStopTransaction":
            if optional:
                return response_database.RemoteStopTransaction(optional)
            else:
                return response_database.RemoteStopTransaction(self.transaction_id)

        elif action == "SendLocalList":
            return response_database.SendLocalList()

        elif action == "SetChargingProfile":
            if optional is True:
                print('\nSending a TxDefaultProfile message from', self.charger_id, '\n')
            else:
                print('\nSending a TxProfile message from', self.charger_id, 'with charge rate', self.charge_rate, '\n')

            return response_database.SetChargingProfile(self.charge_rate, initialize=optional,
                                                        transaction_id=self.transaction_id)

        elif action == "StartTransaction":
            # We set our isCharging status to True
            self._isCharging = True

            # We also reset our zero counter
            self.zero_counter = 0

            # Record the timestamp from the StartTransaction message
            timestamp = self.decoded_message[3]["timestamp"]

            # Record the meter value from the StartTransaction message
            self.meter_value = float(self.decoded_message[3]["meterStart"]) / 1000

            # Convert the raw timestamp into a datetime object
            charging_timestamp_obj = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ')
            # Find the difference between the time now and the time on the timestamp
            timestamp_delta = datetime.now() - charging_timestamp_obj

            # If the difference in time is greater than 1 minute and 20 seconds then this is not a live charging session
            if timestamp_delta.seconds > 120:
                self.charging_timestamp = timestamp.split('T')[0] + ' ' + timestamp.split('T')[1][0:5].replace(':', '')
                live = False

            # If the difference is smaller, then we consider this a live charging session so we use our own timestamp
            else:
                self.charging_timestamp = datetime.strftime(datetime.now(), '%Y-%m-%d %H%M')
                live = True

            # Define our transactionID for the charging session
            self.transaction_id = randint(100, 76437)

            if self.data_handler[0]:
                self.data_handler[0].write_message(
                    json.dumps({"transaction_alert": self._isCharging, "chargerID": self._CHARGER_ID,
                                'meterValue': self.meter_value,
                                'charging_timestamp': self.charging_timestamp, 'transaction_id': self.transaction_id
                                }))

            # Now that our charging session has started, we want our first charge value to be forced through
            self._INITIAL_CHARGE_CHANGE = True

            self.ready_to_update_current = True

            self._TRANSACTION_ID_DICT.update({self.transaction_id: {
                'meterValue': float(self.decoded_message[3]["meterStart"]) / 1000,
                'charging_timestamp': self.charging_timestamp, 'live': live}})

            return response_database.StartTransaction(self.transaction_id)

        elif action == "StatusNotification":
            try:
                if self.decoded_message[3]['status'] == "Charging":
                    print('statusnotification is charging!!!')
                    self._isCharging = True
                    if self.data_handler[0]:
                        print('sending that we are charging now')
                        self.data_handler[0].write_message(json.dumps(
                            {"charging_status": self._isCharging, 'charging_timestamp': self.charging_timestamp,
                             "meterValue": self.meter_value, "chargerID": self._CHARGER_ID,
                             "transaction_id": self.transaction_id}))

                # Todo: this one needs work. The charger sends suspendedEV after preparing...
                # # If our status is SuspendedEV and we are charging, then we have finished charging
                # elif self.decoded_message[3]['status'] == "SuspendedEV" and self._isCharging:
                #     self._isCharging = False
                #     if self.data_handler[0]:
                #         print('Our charging session has finished - lets stop it')
                #         self.charge_rate_queue.append('stop')

                elif self.decoded_message[3]['status'] == "Finishing":
                    self._isCharging = False
                    if self.data_handler[0]:
                        print('sending that we arent charging now')
                        self.data_handler[0].write_message(json.dumps(
                            {"charging_status": self._isCharging, 'charging_timestamp': self.charging_timestamp,
                             "meterValue": self.meter_value, "chargerID": self._CHARGER_ID,
                             "transaction_id": self.transaction_id}))

                elif self.decoded_message[3]['status'] == "Available":
                    # If we are charging and our StatusNotification is NOT from connector 0 then we know it's legitimate
                    # If we are not charging then it's automatically legitimate
                    if not self._isCharging or (self._isCharging and self.decoded_message[3]['connectorId'] != 0):
                        self._isCharging = False
                        if self.data_handler[0]:
                            print('sending that we arent charging now')
                            self.data_handler[0].write_message(json.dumps(
                                {"charging_status": self._isCharging, 'charging_timestamp': self.charging_timestamp,
                                 "meterValue": self.meter_value, "chargerID": self._CHARGER_ID,
                                 "transaction_id": self.transaction_id}))

                        # Now send a message to change the default profile to 6A
                        payload = self.lookup_action_database('SetChargingProfile', optional=True)
                        self.write_message(json.dumps([2, str(543543), 'SetChargingProfile', payload]))

                elif self.decoded_message[3]['status'] == "Preparing":
                    self._isCharging = 'plugged'
                    if self.data_handler[0]:
                        print('sending that we are plugged in')
                        self.data_handler[0].write_message(json.dumps(
                            {"charging_status": self._isCharging, 'charging_timestamp': self.charging_timestamp,
                             "meterValue": self.meter_value, "chargerID": self._CHARGER_ID,
                             "transaction_id": self.transaction_id}))

                    # If we do not need authentication, then we should attempt to start a charging session straight away
                    if self.authentication_required is False:
                        print('No authentication is required so start charging immediately')
                        self.control_remote_charging('start')

                elif self.decoded_message[3]['status'] == "Faulted":
                    # Todo: Maybe we have to send a fault
                    self._isCharging = False
                    if self.data_handler[0]:
                        print('sending that we have a fault')
                        self.data_handler[0].write_message(json.dumps(
                            {"charging_status": self._isCharging, 'charging_timestamp': self.charging_timestamp,
                             "meterValue": self.meter_value, "chargerID": self._CHARGER_ID,
                             "transaction_id": self.transaction_id}))

            except tornado.websocket.WebSocketClosedError as e:
                print(e)
                pass

            return response_database.StatusNotification()

        elif action == "StopTransaction":
            self._isCharging = False
            self.zero_counter = 0

            timestamp = self.decoded_message[3]["timestamp"]

            # Convert the raw timestamp into a datetime object
            charging_timestamp_obj = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ')
            # Find the difference between the time now and the time on the timestamp
            timestamp_delta = datetime.now() - charging_timestamp_obj

            # If the received StopTransaction message is not live then we take the timestamp from the message
            if timestamp_delta.seconds > 120:
                print('\nOur StopTransaction message is not live! Send the timestamp from the message \n')
                timestamp = self.decoded_message[3]["timestamp"]
                timestamp = timestamp.split('T')[0] + ' ' + timestamp.split('T')[1][0:5].replace(':', '')

            # If the received StopTransaction message IS live then we take the current time as the timestamp
            else:
                print('\nOur StopTransaction message is live! Sending the current server timestamp\n')
                timestamp = datetime.strftime(datetime.now(), '%Y-%m-%d %H%M')

            # Make the class timestamp for this charger None (since we're not charging anymore)
            self.charging_timestamp = None
            self.meter_value = None

            # Reset our transaction_id
            self.transaction_id = None

            try:
                if self.data_handler[0]:
                    self.data_handler[0].write_message(
                        json.dumps({"transaction_alert": self._isCharging, "chargerID": self._CHARGER_ID,
                                    'meterValue': float(self.decoded_message[3]["meterStop"]) / 1000,
                                    'charging_timestamp': timestamp, 'transaction_id': self.transaction_id
                                    }))
            except tornado.websocket.WebSocketClosedError as e:
                print(e)
                pass

            self.charge_rate_queue.clear()
            self._INITIAL_CHARGE_CHANGE = False

            return response_database.StopTransaction()

        elif action == "TriggerMessage":
            return response_database.TriggerMessage(optional)

        elif action == "UpdateFirmware":
            charger_model = optional['charger_model']

            # If our model is the ACMP...
            if charger_model[0:4] == "EVPE":
                if optional['firmwareType'] == "Kernal":
                    return response_database.UpdateFirmware(
                        location='ftp://203.32.104.46/Delta_FW_FTP/ACMP/latest_version/DcoKImage')

                elif optional['firmwareType'] == "FileSystem_Admin":
                    # return response_database.UpdateFirmware(
                    #     location='ftp://203.32.104.46/Delta_FW_FTP/ACMP/beta/v2.09.01/DcoFImage')
                    return response_database.UpdateFirmware(
                        location=optional['fw_url'])

                else:
                    return response_database.UpdateFirmware(
                        location='ftp://203.32.104.46/Delta_FW_FTP/ACMP/latest_version/DcoFImage')

            # If our model is the DCWB...
            elif charger_model[0:4] == 'EVDE':
                if optional['firmwareType'] == "Aux_Power_Admin":
                    print('Updating Aux Power')
                    return response_database.UpdateFirmware(
                        location='ftp://203.32.104.46/Delta_FW_FTP/DCWB/admin/EU/Aux_Power/v4.04/5505604234')

                elif optional['firmwareType'] == "BA_Dual_Admin":
                    print('Updating BA Dual Admin')
                    return response_database.UpdateFirmware(
                        location='ftp://203.32.104.46/Delta_FW_FTP/DCWB/admin/EU/Control_Unit_Dual_Output/vA1.36/5505604248F')

                elif optional['firmwareType'] == "BA_Single_Admin":
                    print('Updating BA Single Admin')
                    return response_database.UpdateFirmware(
                        location='ftp://203.32.104.46/Delta_FW_FTP/DCWB/admin/EU/Control_Unit_Single_Output/vA1.36/5505604248F')

                elif optional['firmwareType'] == "RCB_Admin":
                    print('Updating RCB')
                    return response_database.UpdateFirmware(
                        location='ftp://203.32.104.46/Delta_FW_FTP/DCWB/admin/EU/Relay_Control_Board/v4/5505604240')

                elif optional['firmwareType'] == "Aux_Power":
                    print('Updating Aux Power')
                    return response_database.UpdateFirmware(
                        location='ftp://203.32.104.46/Delta_FW_FTP/DCWB/latest_version/Aux_Power/5505604234')

                elif optional['firmwareType'] == "BA_Dual":
                    print('Updating BA Dual')
                    return response_database.UpdateFirmware(
                        location='ftp://203.32.104.46/Delta_FW_FTP/DCWB/latest_version/Control_Unit_Dual_Output/5505604248F')

                elif optional['firmwareType'] == "BA_Single":
                    print('Updating BA Single')
                    return response_database.UpdateFirmware(
                        location='ftp://203.32.104.46/Delta_FW_FTP/DCWB/latest_version/Control_Unit_Single_Output/5505604248F')

                elif optional['firmwareType'] == "CA":
                    print('Updating CA')
                    return response_database.UpdateFirmware(
                        location='ftp://203.32.104.46/Delta_FW_FTP/DCWB/latest_version/CCS/5505604249F')

                elif optional['firmwareType'] == "RCB":
                    print('Updating RCB')
                    return response_database.UpdateFirmware(
                        location='ftp://203.32.104.46/Delta_FW_FTP/DCWB/latest_version/Relay_Control_Board/5505604240')

        else:
            return False

    @gen.coroutine
    def charging_firewall(self, unique_id):
        """ This function will send a Authorize request to backend to ensure there is enough power"""

        self.authorize_response_decision = None

        print('We are in the charging firewall with uniqueID:', unique_id)

        if self.data_handler[0]:
            try:
                # First we need to send an authorize request to the EVCS backend
                self.data_handler[0].write_message(json.dumps({"authorize_request": True,
                                                               "chargerID": self._CHARGER_ID}))

                # Now loop over 20 seconds, waiting for a decision on the authorize response
                counter = 0
                while self.authorize_response_decision is None:
                    yield gen.sleep(0.5)
                    counter += 1

                    # If we reach 20 seconds and there is no response, then we reject the charging session
                    if counter == 40:
                        print('Our authorization request has timed out (20 seconds has passed)')
                        self.authorize_response_decision = False
                        break

                print('We are out of the authorize response received loop!', self.authorize_response_decision)

                # When we get a response then we send the response to the charger
                payload = self.lookup_action_database("Authorize", self.authorize_response_decision)
                response = json.dumps([3, unique_id, payload])
                print('my response is: ', response)
                self.write_message(response)

            except tornado.websocket.WebSocketClosedError as e:
                print(e)

        else:
            print('There is no data handler, so we have to decline')
            payload = self.lookup_action_database("Authorize", False)
            response = json.dumps([3, unique_id, payload])
            print('my response is: ', response)
            self.write_message(response)

    def control_remote_charging(self, command):
        if command == "stop":
            payload = self.lookup_action_database('RemoteStopTransaction')
            self.write_message(json.dumps([2, "88888", 'RemoteStopTransaction', payload]))
            print('RemoteStopTransaction message sent:', payload)

        elif command == "start":
            payload = self.lookup_action_database('RemoteStartTransaction')
            self.write_message(json.dumps([2, "88889", 'RemoteStartTransaction', payload]))
            print('RemoteStartTransaction message sent:', payload)

    @gen.coroutine
    def charge_rate_control(self):
        while True:
            # If a vehicle is currently charging and the queue is NOT EMPTY and we are ready to update
            if self._isCharging and self.charge_rate_queue and self.ready_to_update_current:
                # Generate a new message id
                new_message_id = randint(10000, 99999)
                self.latest_charge_id = str(new_message_id)
                print('Request found! Our latest charge id is: ', new_message_id)

                # Now we pop out the latest charge_rate (take value out from the right)
                self.charge_rate = self.charge_rate_queue.pop()

                # Check if the charge_rate is a string "stop"
                if self.charge_rate == "stop":
                    self.control_remote_charging(self.charge_rate)
                else:
                    # If it is not a string, it is a normal charge rate so send to send that straight to the charger
                    payload = self.lookup_action_database('SetChargingProfile', optional=False)
                    self.write_message(json.dumps([2, str(new_message_id), 'SetChargingProfile', payload]))
                    print('charge rate is now set to: ', self.charge_rate, datetime.now())
                    print('the charge rate message is: ', new_message_id, payload)

                    # We are now NOT READY to update. Waiting for the return message to change this again
                    self.ready_to_update_current = False

            # The only other condition is if we are not charging and there is something in the queue
            elif (not self._isCharging) and self.charge_rate_queue:
                print('We have reached the alternate in', self._CHARGER_ID)

                self.charge_rate = self.charge_rate_queue.pop()

                # Check if the charge_rate is a string "start"
                if self.charge_rate == "start":
                    self.control_remote_charging(self.charge_rate)

            yield gen.sleep(0.3)

    def authorize_response_returned(self, authorized):
        print('Im in authorize response received!', self)
        self.authorize_response_decision = authorized

    def change_authentication_requirement(self, authentication_required):
        """ This function takes an input from the user that they do/don't want authentication """

        if authentication_required == "True" or authentication_required == "true":
            authentication_required = True
        elif authentication_required == "False" or authentication_required == "false":
            authentication_required = False

        print('Authentication required changed to', authentication_required, 'in', self._CHARGER_ID)
        self.authentication_required = authentication_required

    def evc_inputs_change(self, payload):
        """ This function is called when our DataHandler gets new data from our EVCS """

        # If our payload's purpose is changing the charging rate
        if payload['purpose'] == "charge_rate":
            charge_rate = payload['charge_rate']

            # If we have a string input (start or stop), then we add it to the left of the queue
            if isinstance(charge_rate, str):
                if (charge_rate == 'stop' and self._isCharging is True) or (
                        charge_rate == 'start' and self._isCharging is False):
                    print('We have detected a string with the correct isCharging conditions')
                    self.charge_rate_queue.append(charge_rate)

                    print('The charge rate queue is now: ', self.charge_rate_queue)

            # If we are currently charging then there are a few options...
            elif self._isCharging is True:

                # If we JUST started a charging session
                if self._INITIAL_CHARGE_CHANGE:
                    print('Initial charge change - Forcing:', charge_rate)

                    # Then we force append the charge rate
                    self.charge_rate_queue.appendleft(charge_rate)
                    print('The charge rate queue is now: ', self.charge_rate_queue)

                    self._INITIAL_CHARGE_CHANGE = False

                # If it's just a normal condition...
                else:
                    # Check if there are no 'start' and 'stop' messages in the queue and there is more than
                    # one item in the queue...
                    if self.charge_rate_queue.count('start') == 0 and self.charge_rate_queue.count(
                            'stop') == 0 and len(self.charge_rate_queue) > 1:
                        print('We have too many items in the queue, time to clear!')

                        # Clear the queue completely. This will make the charger respond quicker
                        self.charge_rate_queue.clear()

                    # If there is more than one item in the queue. make sure the item currently at the back of the queue
                    # is not the same as our proposed new charge rate
                    # OR... if there are NO items in the queue, then ensure that the proposed charge rate is not the
                    # same as the most recently updated charge rate
                    if (len(self.charge_rate_queue) > 0 and charge_rate != self.charge_rate_queue[0]) or (
                            len(self.charge_rate_queue) == 0 and charge_rate != self.charge_rate):
                        self.charge_rate_queue.appendleft(charge_rate)
                        print('The charge rate queue is now: ', self.charge_rate_queue)

                    else:
                        _DEBUG and print('This is a duplicate, do not add')

            # If we aren't charging, we still need to update the charge rate value for our charging firewall
            else:
                self.charge_rate = charge_rate

            # We will return our charging status. The EVCS needs this as a response to update its status
            return self._isCharging

        # If our purpose is to change the charging mode, then we modify the instance variable
        elif payload['purpose'] == "charging_mode":
            self.charging_mode = payload["charging_mode"]
            return None

        elif payload['purpose'] == 'update_firmware':
            payload = self.lookup_action_database('UpdateFirmware', optional=payload)
            self.write_message(json.dumps([2, str(284387), 'UpdateFirmware', payload]))
            print('Sent a message to update firmware!')

        elif payload['purpose'] == "Trigger_StatusNotification":
            return self._isCharging, self.charging_timestamp, self.meter_value, self.transaction_id

        elif payload['purpose'] == 'misc_command':
            print('Got a misc command', payload)
            action = payload['action']
            misc_data = payload['misc_data']

            # We have to make an exception for RemoteStopTransaction, we add to the queue instead
            if action == "RemoteStopTransaction" and self._isCharging:
                self.charge_rate_queue.append('stop')

            payload = self.lookup_action_database(action, optional=misc_data)
            misc_payload = json.dumps([2, str(648294), action, payload])
            try:
                self.write_message(misc_payload)
                print("Sent misc command", misc_payload)

            except tornado.websocket.WebSocketClosedError as e:
                print(e)
                pass

    def send_follow_up(self, messagetypeid, follow_up_payload):

        # follow_up_payload will be 'action' for id 2 and 'message id' for id 3
        # If we have just replied to a REQUEST from the charge point
        if messagetypeid == 2:
            action = follow_up_payload

            # if action == "Heartbeat" and not self._isCharging:
            #     payload = self.lookup_action_database('SetChargingProfile', optional=True)
            #     self.write_message(json.dumps([2, str(543543), 'SetChargingProfile', payload]))

            # Send a message to the EVCS that we are alive
            if self.data_handler[0]:
                try:
                    self.data_handler[0].write_message(({"alive": True, "chargerID": self._CHARGER_ID}))
                except tornado.websocket.WebSocketClosedError as e:
                    print(e)
                    pass

        # If we are receiving a follow up from the charge point for a request that WE sent
        elif messagetypeid == 3:
            received_msg_id = follow_up_payload
            # This if block compares the id of the .CONF we got from the charge point with the id of our latest request
            if received_msg_id == self.latest_charge_id:
                print('We have a match in the latest conf payload id and the id we are looking for')

                # Now we can tell update_charge_rate that we are ready to pop another charge rate from the queue
                self.ready_to_update_current = True

    def respond_to_charger(self, message):
        # Load in a message as soon as it is received from the charge point and output a proper response to it

        # First decode the message into individual parts
        self.decoded_message = json.loads(message)
        messagetypeid = self.decoded_message[0]
        unique_id = self.decoded_message[1]

        # If there is a REQUEST from the charge point
        if messagetypeid == 2:
            action = self.decoded_message[2]

            # If we have an authorize message, we divert from the normal data route
            if action == "Authorize":
                tornado.ioloop.IOLoop.current().spawn_callback(self.charging_firewall, unique_id)

            else:
                # lookup_action_database will take in request, do whatever the server needs to do and return a response
                payload = self.lookup_action_database(action)

                # Convert the list to a JSON
                response = json.dumps([3, unique_id, payload])
                print('my response is: ', response)

                # Make sure we have a response for the message first.
                if payload is not False:
                    self.write_message(response)
                else:
                    print('Couldnt find the message in the database - please add!')

                # Now send any follow up messages that we need to send. Send in the action
                self.send_follow_up(messagetypeid, action)

        # If there is a RESPONSE from from the charge point
        elif messagetypeid == 3:

            # Now send any follow up messages that we need to send. Send in the message id
            self.send_follow_up(messagetypeid, unique_id)

    def on_message(self, message):
        print(datetime.now(), 'the incoming message from: ' + self._CHARGER_ID + message)

        # If a timeout exists, then we renew it
        if self.timeout:
            tornado.ioloop.IOLoop.current().remove_timeout(self.timeout)
        self.timeout = tornado.ioloop.IOLoop.current().add_timeout(timedelta(minutes=self.timeout_minutes),
                                                                   self.close_charger_ws_connection)

        # This function sends a response back to the charger
        # There is logic in here to filter to ensure that responses are only sent when they have to be!
        self.respond_to_charger(message)

    def initialize(self, ws_clients_obj):
        self.ws_clients = ws_clients_obj.ws_clients
        self.data_handler = ws_clients_obj.data_handler

        self.timeout_minutes = 10

    def open(self):
        print('open')
        print('chargerID is:', self.request.path.split('/')[-1])
        print('remote ip is:', self.request.remote_ip)
        print('data_handler is', self.data_handler)

        # Initialize variables
        self.charge_rate = 6
        self.charging_mode = "PV_with_BT"
        self.latest_charge_id = str()
        self.decoded_message = None
        self.charge_rate_queue = deque()

        self.charger_id = self.request.path.split('/')[-1]
        self.new_charger_info = None
        self.charging_timestamp = None
        self.meter_value = None
        self.transaction_id = None

        # Initial charge change is to tell the OCPP server to allow the first charge rate after StartTransaction to
        # be able to append to the charge queue
        self._INITIAL_CHARGE_CHANGE = False

        # Define our flags
        self._isCharging = False
        self._changeConfiguration = True
        self.ready_to_update_current = True

        # Define our authentication required flag
        self.authentication_required = True

        # TRANSACTION_ID_DICT will contain information about each charging transaction Id
        self._TRANSACTION_ID_DICT = dict()

        # Record my own charger ID down
        self._CHARGER_ID = self.request.path.split('/')[-1]

        # Todo: decide whether or not we need this. Probably easier just to access the object directly
        self.db = UserDataStore(self.request.path.split('/')[-1])

        print('ws clients before checking for duplicate:', self.ws_clients)
        # We search for existing WebSocket clients with the same chargerID and we close them
        for client in self.ws_clients.copy():
            if client[1] == self._CHARGER_ID:
                # If we see a duplicate, then we need to check the charging status of the previous object.
                # if client[0]._isCharging:
                self._isCharging = client[0]._isCharging
                client[0].close()
                print('Forced closed...')

        # Add this websocket client to our list
        self.ws_clients.add((self, self.request.path.split('/')[-1], self.db))
        print('ws clients after latest add:', self.ws_clients)

        tornado.ioloop.IOLoop.current().spawn_callback(self.charge_rate_control)

        # Start a timeout to close the connection after a period of inactivity
        self.timeout = tornado.ioloop.IOLoop.current().add_timeout(timedelta(minutes=self.timeout_minutes),
                                                                   self.close_charger_ws_connection)
        self.zero_counter = 0

    def close_charger_ws_connection(self):
        # Todo: monitor this, right now we only want to close if we aren't charging - probably has to change
        # If we aren't charging, then we send a message to remove the charger from the back end
        if self._isCharging is not True:
            # Send a message to the EVCS that this charge point will no longer be alive
            if self.data_handler[0]:
                try:
                    _DEBUG and print('Sending a message to remove', self._CHARGER_ID)
                    self.data_handler[0].write_message(({"alive": False, "chargerID": self._CHARGER_ID}))
                except tornado.websocket.WebSocketClosedError as e:
                    print(e)
                    pass

            self.close()

    def on_close(self):
        print('closing...', self._CHARGER_ID)
        self.ws_clients.remove((self, self.request.path.split('/')[-1], self.db))

        if self.data_handler[0]:
            try:
                _DEBUG and print('Sending a message to remove', self._CHARGER_ID)
                self.data_handler[0].write_message(({"alive": False, "chargerID": self._CHARGER_ID}))
            except tornado.websocket.WebSocketClosedError as e:
                print(e)
                pass

        print('closed')
        print(self.ws_clients)

        self.timeout = None


class UserDataStore:
    def __init__(self, charger_id):
        self.charger_id = charger_id
        self.charging = False

        self.random_number = randint(0, 101)


class OCPPDataHandler(tornado.websocket.WebSocketHandler):
    def synchronise_data(self, message):
        for client in self.ws_clients:
            synchronise_payload = client[0].evc_inputs_change(payload=message)
            _is_charging = synchronise_payload[0]
            charging_timestamp = synchronise_payload[1]
            meter_value = synchronise_payload[2]
            transaction_id = synchronise_payload[3]
            if _is_charging is not None:
                print('Synchronising', client[1], 'from OCPPDataHandler')
                self.write_message(json.dumps(
                    {"charging_status": _is_charging, 'charging_timestamp': charging_timestamp,
                     'meterValue': meter_value, "transaction_id": transaction_id, "chargerID": client[1]}))

    def initialize(self, ws_clients_obj):
        print('In OCPPDataHandler initialize!\n')
        # self.ws_clients_obj = ws_clients_obj

        # Define self.ws_clients to be a reference to the set of ws clients in ws_clients_obj that is shared
        self.ws_clients = ws_clients_obj.ws_clients
        ws_clients_obj.data_handler.clear()
        ws_clients_obj.data_handler.append(self)

        print('ws_clients_obj in initialize. Data handler is:', ws_clients_obj.data_handler)
        for client in self.ws_clients:
            print(client)

    def open(self):
        print('OCPP Data Handler open', datetime.now())
        self.synchronise_data({'purpose': 'Trigger_StatusNotification'})

    def on_message(self, message):
        _DEBUG and print('received in OCPPDataHandler', message, datetime.now())

        # Decode the JSON message
        message = json.loads(message)

        if message['purpose'] == "Trigger_StatusNotification":
            self.synchronise_data(message)

        elif message['purpose'] == "authorize_request":
            print('Got an authorize request in OCPP for charger ID', message['chargerID'])

            for client in self.ws_clients:
                if client[1] == message['chargerID']:
                    client[0].authorize_response_returned(message['authorized'])

        elif message['purpose'] == "change_authentication_requirement":
            for client in self.ws_clients:
                client[0].change_authentication_requirement(message['authentication_required'])

        else:
            _found_charger = False
            # Loop through all of the clients, see if there is a match. If there is a match then send the message to
            # that client and update the _found_charger flag.
            """ self.ws_clients = (self, self.request.path.split('/')[-1], self.db) """
            for client in self.ws_clients:
                # Check the charger ID matches the instance's ID
                if client[1] == message['chargerID']:
                    # If it matches, we find the object for the charger and COMPLETE payload to them
                    _is_charging = client[0].evc_inputs_change(payload=message)

                    if _is_charging is not None:
                        self.write_message(json.dumps({"charging_status": _is_charging, "chargerID": client[1]}))

                    _DEBUG and print('Charger found - responded to the message')

                    _found_charger = True

            # If no client is found then we just send False for _is_charging
            if not _found_charger and message['purpose'] == 'charge_rate':
                self.write_message(json.dumps({"charging_status": False, "chargerID": None}))
                _DEBUG and print('No charger found - responded to the message')

        _DEBUG and print('Message has been dealt with', datetime.now())

    def on_close(self):
        _DEBUG and print('OCPP Data Handler closed', datetime.now())


#
class DataHandler(tornado.websocket.WebSocketHandler):
    def open(self):
        print('data open!')
        self.write_message('Hello from rasp pi')

    def on_message(self, message):
        print('received', message)
        self.write_message('hello hello')

    def on_close(self):
        print('closed')


class Application(tornado.web.Application):
    def __init__(self):
        ws_client_obj = ws_clients_wrapper()

        handlers = [
            (r"/ocpp/.*", WebSocketHandler, {'ws_clients_obj': ws_client_obj}),
            (r"/ocpp_data_service/", OCPPDataHandler, {'ws_clients_obj': ws_client_obj}),

            (r"/data/", DataHandler),
            (r"/", httpHandler),
        ]

        tornado.web.Application.__init__(self, handlers)


ws_app = Application()
print('Starting OCPP Backend...')
ws_app.listen(8000)
io_loop = tornado.ioloop.IOLoop.instance()
io_loop.start()
