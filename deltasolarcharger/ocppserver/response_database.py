from datetime import datetime
from random import randint


def Authorize(authorize_status='Accepted'):
    return {'idTagInfo': {
        'status': authorize_status
    }}


def BootNotification(interval, status):
    return {"status": status, "currentTime": datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), "interval": interval}


def ChangeAvailability(availability_type):
    print('availability type is', availability_type)
    return {"connectorID": 0, "type": availability_type}


def ChangeConfiguration(payload):
    return payload
    # return {"key": "MeterValueSampleInterval", "value": "10"}
    # return {"key": "MeterValuesSampledData", "value": "Voltage,Current.Import,Power.Active.Import"}
    # return {"key": "MeterValuesAlignedData",
    #         "value": "Voltage,Current.Import,Power.Active.Import,Energy.Active.Import.Interval"
    #         }


def ClearChargingProfile(payload):
    return payload


def DiagnosticsStatusNotification():
    return {}


def FirmwareStatusNotification():
    return {}


def GetCompositeSchedule(payload):
    return payload


def GetConfiguration(payload):
    return payload
    # return {'key': ['MeterValuesSampledData', 'MeterValueSampleInterval']}


def GetDiagnostics():
    return {'location': 'ftp://getdiagnostics:12345@203.32.104.46/Diagnostic_Test/'}
    # return {'location': 'ftp://203.32.104.46/Diagnostic_Test/'}


def GetLocalListVersion():
    return {}


def Heartbeat():
    return {"currentTime": datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}


def MeterValues(decoded_message):
    sampledValues = decoded_message[3]['meterValue'][0]['sampledValue']

    # for parameters in sampledValues:
    #     print(parameters)

    return {}


def RemoteStartTransaction():
    return {
        'connectorId': 1,
        'idTag': '0498ECCA704880',
        # 'chargingProfile': {
        #     'chargingProfileId': 1,
        #     'stackLevel': 2,
        #     'chargingProfilePurpose': 'TxProfile',
        #     'chargingProfileKind': 'Absolute',
        #     'chargingSchedule': {
        #         'chargingRateUnit': 'A',
        #         'ChargingSchedulePeriod': {
        #             'startPeriod': 0,
        #             'limit': 16
        #
        #         }
        #     }
        # }
    }


def RemoteStopTransaction(transaction_id):
    return {'transactionId': transaction_id}


def SendLocalList():
    localAuthorizationList = list()

    for i in range(0, 100):
        localAuthorizationList.append(
            {
                "idTag": "0498ECCA704" + str(i),
                "idTagInfo": {
                    "status": "Accepted",
                },
            }, )

    return {
        "listVersion": 1,
        "localAuthorizationList": localAuthorizationList,
        "updateType": "Full"}

    # return {
    #     "listVersion": 1,
    #     "localAuthorizationList": [
    #         {
    #             "idTag": "0498ECCA704880",
    #             "idTagInfo": {
    #                 "status": "Accepted",
    #             },
    #         },
    #         {
    #             "idTag": "0498ECCA704881",
    #             "idTagInfo": {
    #                 "status": "Accepted",
    #             },
    #         },
    #         {
    #             "idTag": "0498ECCA704882",
    #             "idTagInfo": {
    #                 "status": "Accepted",
    #             },
    #         },
    #         {
    #             "idTag": "0498ECCA704883",
    #             "idTagInfo": {
    #                 "status": "Accepted",
    #             },
    #         },
    #         {
    #             "idTag": "0498ECCA704884",
    #             "idTagInfo": {
    #                 "status": "Accepted",
    #             },
    #         },
    #         {
    #             "idTag": "0498ECCA704885",
    #             "idTagInfo": {
    #                 "status": "Accepted",
    #             },
    #         },
    #         {
    #             "idTag": "0498ECCA704886",
    #             "idTagInfo": {
    #                 "status": "Accepted",
    #             },
    #         },
    #         {
    #             "idTag": "0498ECCA704887",
    #             "idTagInfo": {
    #                 "status": "Accepted",
    #             },
    #         },
    #         {
    #             "idTag": "0498ECCA704888",
    #             "idTagInfo": {
    #                 "status": "Accepted",
    #             },
    #         },
    #
    #         {
    #             "idTag": "0498ECCA704889",
    #             "idTagInfo": {
    #                 "status": "Accepted",
    #             },
    #         }
    #     ],
    #     "updateType": "Full"}


def SetChargingProfile(charge_rate, initialize, transaction_id):
    # Initialize specifies whether or not we are setting the default profile outside of transaction or in a transaction
    # If initialize is True, then we are setting the default profile OUTSIDE of transaction
    if initialize:
        return {
            'connectorId': 1,
            'csChargingProfiles': {
                'chargingProfileId': 2,
                'stackLevel': 0,
                'chargingProfilePurpose': 'TxDefaultProfile',
                'chargingProfileKind': 'Recurring',
                'recurrencyKind': 'Daily',
                'chargingSchedule': {
                    'duration': 86400,
                    # 'startSchedule': '2019-02-11T10:54Z',
                    'chargingRateUnit': 'A',
                    'chargingSchedulePeriod': {
                        'startPeriod': 0,
                        'limit': 6
                        # 'numberPhases': 1
                    }
                }
            }
        }
    else:
        return {
            'connectorId': 1,
            'csChargingProfiles': {
                'chargingProfileId': 1,
                'transactionId': transaction_id,
                'stackLevel': 0,
                'chargingProfilePurpose': 'TxProfile',
                'chargingProfileKind': 'Absolute',
                'recurrencyKind': 'Daily',
                'chargingSchedule': {
                    'chargingRateUnit': 'A',
                    'chargingSchedulePeriod': {
                        'startPeriod': 0,
                        'limit': charge_rate
                    }

                }
            }
        }


def StartTransaction(transaction_id):
    return {'idTagInfo': {'status': 'Accepted'},
            'transactionId': transaction_id}


def StatusNotification():
    return {}


def StopTransaction():
    return {'idTagInfo': {'status': 'Accepted'}}


def TriggerMessage(requested_message):
    return {'requestedMessage': requested_message}


def UpdateFirmware(location):
    return {
        'location': location,
        'retries': 1,
        'retrieveDate': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
    }
