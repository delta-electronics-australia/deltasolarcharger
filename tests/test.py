import pyrebase
import time
from datetime import datetime
from threading import Timer

from collections import OrderedDict


# db = None
# uid = None
# idToken = None


def stream_handler(message):
    print(message["event"])  # put
    print(message["path"])  # /-K7yGTTEp7O549EzTYtI
    print(message["data"])  # {'title': 'Pyrebase', "body": "etc..."}
    print(message["stream_id"])


def authenticate():
    def firebase_stream_handler(message):
        print(message)
        if message['data'] is not None and message['data']['set']:
            # Need to lookup the charger model
            test = db.child("users").child(uid).child('evc_inputs').child(message['data']['chargerID']).child(
                'charger_info/chargePointModel').get(idToken).val()
            print(test)

    config = {
        "apiKey": "AIzaSyCaxTOBofd7qrnbas5gGsZcuvy_zNSi_ik",
        "authDomain": "smart-charging-app.firebaseapp.com",
        "databaseURL": "https://smart-charging-app.firebaseio.com",
        "storageBucket": "",
    }

    print('Attempting to authenticate with Firebase')
    firebase = pyrebase.initialize_app(config)
    auth = firebase.auth()
    user = auth.sign_in_with_email_and_password("jgv115@gmail.com", "test123")
    uid = user['localId']
    idToken = user['idToken']

    print(uid)

    db = firebase.database(timeout_length=2)

    # Send a package to update DCWB firmware
    # db.child("users").child(uid).child('evc_inputs').update({
    #     "update_firmware": {
    #         'chargerID': "MEL-DCWB",
    #         'firmwareType': 'BA_Dual',
    #         'set': True
    #     }
    # })

    # # Send a package to update ACMP firmware
    # db.child("users").child(uid).child('evc_inputs').update({
    #     "update_firmware": {
    #         'chargerID': "MEL-ACMP-WIFI2",
    #         'firmwareType': 'FileSystem_Admin',
    #         'set': True,
    #         'fw_url': 'ftp://203.32.104.46/Delta_FW_FTP/ACMP/beta/v2.09.02/DcoFImage'
    #     }
    # }, idToken)

    # # Send a package to send a misc command
    # db.child("users").child(uid).child('evc_inputs').update({
    #     "misc_command": {
    #         'chargerID': "MEL-ACMP-WIFI",
    #         'action': 'GetConfiguration',
    #         # 'action': 'ChangeConfiguration',
    #         'misc_data': {
    #             'key': ['AuthorizationRequired', 'NonAuthorizedTag', 'OfflinePolicy', 'AuthorizeRemoteTxRequests']},
    #         # 'misc_data': {"key": "MeterValueSampleInterval", "value": "10"}
    #     }
    # }, idToken)

    # try:
    #     # Send a package to send a misc command - LEGIT
    #     db.child("users").child(uid).child('evc_inputs').update({
    #         "misc_command": {
    #             'chargerID': "CSIRO-ACMP4",
    #             'action': 'GetCompositeSchedule',
    #             'misc_data': {'connectorId': 1, 'duration': 180, 'chargingRateUnit': 'A'}
    #         }
    #     }, idToken)
    # except OSError as e:
    #     print(e)
    #     print('got an OSerror')
    #     pass

    try:
        # Send a package to send a misc command - LEGIT
        db.child("users").child(uid).child('evc_inputs').update({
            "misc_command": {
                'chargerID': "MEL-ACMP",
                'action': 'RemoteStartTransaction',
                'misc_data': True
            }
        }, idToken)
    except OSError as e:
        print(e)
        print('got an OSerror')
        pass

    # try:
    #     # Send a package to send a misc command - LEGIT
    #     db.child("users").child(uid).child('evc_inputs').update({
    #         "manual_charge_control": {
    #             'chargerID': "MEL-ACMP",
    #             'charge_rate': 0
    #         }
    #     }, idToken)
    # except OSError:
    #     print('got an OSerror')
    #     pass

    # # Send a package to send a misc command
    # db.child("users").child(uid).update({
    #     "misc_command": {
    #         'chargerID': "MEL-ACMP-WIFI",
    #         'action': 'SendLocalList',
    #         'misc_data': "BootNotification"
    #     }
    # })

    # firebase_csv_list = list(
    #     db.child("users").child(uid).child("history_keys").get(idToken).val().keys())
    #
    # print(firebase_csv_list)
    # # Finally we need to delete the unnecessary entries in history to save space
    # for firebase_csv_name in firebase_csv_list:
    #     print('Checking:', firebase_csv_name, 'from history')
    #     if firebase_csv_name != datetime.now().strftime("%Y-%m-%d"):
    #         db.child("users").child(uid).child("history").child(firebase_csv_name).remove()
    #     else:
    #         print(firebase_csv_name, 'it is today, dont need to delete')


if __name__ == '__main__':
    authenticate()
