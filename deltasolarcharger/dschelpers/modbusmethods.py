import minimalmodbus
import serial
import numpy as np

import time
from datetime import datetime

from queue import Queue


class ModbusMethods:
    def __init__(self, analyse_to_modbus_queue):
        self.E5 = None
        self.DPM = None

        self.analyse_to_modbus_queue = analyse_to_modbus_queue

        self.initiate_parameters(1, 5)


    def initiate_parameters(self, e5_id, dpm_id):
        self.E5 = minimalmodbus.Instrument('/dev/serial0', e5_id)  # port name, slave address (in decimal)
        self.E5.debug = False
        self.E5.baudrate = 19200
        self.E5.serial.bytesize = 8
        self.E5.serial.parity = serial.PARITY_NONE
        self.E5.serial.stopbits = 1
        self.E5.serial.timeout = 0.5  # seconds
        self.E5.mode = minimalmodbus.MODE_RTU
        self.E5.handle_local_echo = False
        self.E5.close_port_after_each_call = False
        # self.E5.precalculate_read_size = False

        # self.DPM = minimalmodbus.Instrument('/dev/ttyUSB0', dpm_id)
        # self.DPM.debug = False
        # self.DPM.baudrate = 19200
        # self.DPM.serial.bytesize = 8
        # self.DPM.serial.parity = serial.PARITY_NONE
        # self.DPM.serial.stopbits = 1
        # self.DPM.serial.timeout = 0.5  # seconds
        # self.DPM.mode = minimalmodbus.MODE_RTU
        # self.DPM.handle_local_echo = False

        print('Modbus initialized!')

    @staticmethod
    def twos_comp(vals, bits):
        """compute the 2's compliment of array of int values vals"""
        vals[vals & (1 << (bits - 1)) != 0] -= (1 << bits)
        return vals

    @staticmethod
    def bt_mode_database(modbus_input):
        # Check the status
        if modbus_input == 0:
            status_output = "STAND_BY_MODE"
        elif modbus_input == 1:
            status_output = "SELF_CONSUMPTION_MODE_INTERNAL"
        elif modbus_input == 2:
            status_output = "PEAK_CUT_MODE"
        elif modbus_input == 3:
            status_output = "CHARGE_FIRST_MODE"
        elif modbus_input == 4:
            status_output = "DISCHARGE_FIRST_MODE"
        elif modbus_input == 5:
            status_output = "WITHOUT_BT_MODE"
        elif modbus_input == 6:
            status_output = "PV_CHARGE_BT_FIRST_MODE"
        elif modbus_input == 11:
            status_output = "STAND_BY_PAUSE_MODE"
        elif modbus_input == 12:
            status_output = "MAINTENANCE_MODE"
        else:
            status_output = "STAND_ALONE_MODE"

        return status_output

    @staticmethod
    def lookup_inverter_status(status_int):
        if status_int == 0:
            return "Standby"
        elif status_int == 1:
            return "Countdown"
        elif status_int == 2:
            return "On Grid"
        elif status_int == 3:
            return "No DC"
        elif status_int == 4:
            return "Alarm"
        elif status_int == 5:
            return "Reserved"
        elif status_int == 6:
            return "Stand Alone"
        else:
            return "On"

    @staticmethod
    def lookup_operation_mode(opmode_int):
        # Check the status
        if opmode_int[0] == 0:
            return "STAND_BY_MODE"
        elif opmode_int[0] == 1:
            return "SELF_CONSUMPTION_MODE_INTERNAL"
        elif opmode_int[0] == 2:
            return "PEAK_CUT_MODE"
        elif opmode_int[0] == 3:
            return "CHARGE_FIRST_MODE"
        elif opmode_int[0] == 4:
            return "DISCHARGE_FIRST_MODE"
        elif opmode_int[0] == 5:
            return "WITHOUT_BT_MODE"
        elif opmode_int[0] == 6:
            return "PV_CHARGE_BT_FIRST_MODE"
        elif opmode_int[0] == 11:
            return "STAND_BY_PAUSE_MODE"
        elif opmode_int[0] == 12:
            return "MAINTENANCE_MODE"
        else:
            return "STAND_ALONE_MODE"

    def get_modbus_data(self, _debug=False):
        # Check for any inputs from analyse methods
        if not self.analyse_to_modbus_queue.empty():
            new_payload = self.analyse_to_modbus_queue.get()
            purpose = new_payload['purpose']

            if purpose == "inverter_op_mode":
                if new_payload['inverter_op_mode'] == "CHARGE_FIRST_MODE":
                    self.E5.write_register(25626, 4, 0, 6, False)
                    print('Changed mode to Charge First Mode!')
                elif new_payload['inverter_op_mode'] == "SELF_CONSUMPTION_MODE_INTERNAL":
                    self.E5.write_register(25626, 1, 0, 6, False)
                    print('Changed mode to Self Consumption Mode!')
                elif new_payload['inverter_op_mode'] == "WITHOUTBTMODE":
                    self.E5.write_register(25626, 6, 0, 6, False)
                    print('Changed mode to Without BT Mode!')

        # Grab all inverter_cont_data
        inverter_data = dict()
        # Write 0 to holding register 800 to read AC1 info
        self.E5.write_register(799, 0, 0, 6, False)

        inverter_data['time'] = str(datetime.now())

        # Todo: AC1 Power needs to be twos compliment - CHECK THIS
        # Read the relevant registers, store in "temp"
        temp = self.E5.read_registers(1056, 4, 4)
        _debug and print(temp)
        inverter_data["ac1_voltage"] = float(temp[0])
        inverter_data['ac1_current'] = float(temp[1])
        inverter_data['ac1_power'] = float(temp[2])
        inverter_data['ac1_freq'] = float(temp[3])

        # Same as above
        self.E5.write_register(799, 32, 0, 6, False)
        temp = self.E5.read_registers(1056, 4, 4)
        _debug and print(temp)
        inverter_data["ac2_voltage"] = float(temp[0])
        inverter_data['ac2_current'] = float(temp[1])
        inverter_data['ac2_power'] = float(temp[2])
        inverter_data['ac2_freq'] = float(temp[3])

        self.E5.write_register(799, 48, 0, 6, False)
        temp = self.E5.read_registers(1056, 4, 4)
        _debug and print(temp)
        inverter_data['dc1_voltage'] = float(temp[0])
        inverter_data['dc1_current'] = float(temp[1])
        inverter_data['dc1_power'] = float(temp[2])

        self.E5.write_register(799, 49, 0, 6, False)
        temp = self.E5.read_registers(1056, 4, 4)
        _debug and print(temp)
        inverter_data['dc2_voltage'] = float(temp[0])
        inverter_data['dc2_current'] = float(temp[1])
        inverter_data['dc2_power'] = float(temp[2])

        temp = self.E5.read_registers(1079, 4, 4)
        _debug and print(temp)
        inverter_data['ambient_temp'] = str(temp[0])
        inverter_data['boost_1_temp'] = str(temp[1])
        inverter_data['boost_2_temp'] = str(temp[2])
        inverter_data['inverter_temp'] = str(temp[3])

        temp = self.E5.read_registers(1551, 1, 4)
        _debug and print(temp)
        operation_mode = self.lookup_operation_mode(temp)

        inverter_data['inverter_op_mode'] = operation_mode

        temp = self.E5.read_registers(1047, 1, 4)
        _debug and print(temp)
        inverter_data['inverter_status'] = self.lookup_inverter_status(temp[0])

        # temp = self.E5.read_registers(1047, 1, 4)
        # inverter_data['inverter_status'] = temp
        #
        temp = self.E5.read_registers(1039, 1, 4)
        _debug and print(temp)
        dsp = hex(int(temp[0]))
        inverter_data['fw_dsp'] = 'v0' + str(int(dsp[2], 16)) + '.' + str(int(dsp[3:5], 16))

        temp = self.E5.read_registers(1041, 1, 4)
        _debug and print(temp)
        red = hex(int(temp[0]))
        inverter_data['fw_red'] = 'v0' + str(int(red[2], 16)) + '.' + str(int(red[3:5], 16))

        temp = self.E5.read_registers(1043, 1, 4)
        _debug and print(temp)
        comm = hex(int(temp[0]))
        inverter_data['fw_disp'] = 'v0' + str(int(comm[2], 16)) + '.' + str(int(comm[3:5], 16))

        # Grab all bt_cont_data
        bt_data = dict()

        # This block grabs all of the registers to do with battery
        bt_data_temp = dict()
        start = 1536
        num = 32
        modbus_int_out = self.E5.read_registers(start - 1, num, 4)
        _debug and print(modbus_int_out)

        for x in range(0, num):
            bt_data_temp[start + x] = (modbus_int_out[x])
        ##

        array = np.array([bt_data_temp[1566], bt_data_temp[1567]])
        a = self.twos_comp(array, 16)

        array = np.array([bt_data_temp[1547], bt_data_temp[1548]])
        b = self.twos_comp(array, 16)

        bt_data['bt_soc'] = float(bt_data_temp[1538])
        bt_data['bt_voltage'] = float(bt_data_temp[1565])
        bt_data['bt_current'] = float(a[0])  # == 1566 twos comp
        bt_data['bt_wattage'] = a[1]  # == 1567 twos comp
        bt_data['utility_current'] = float(b[0])  # float(bt_data_temp[1547])
        bt_data['utility_power'] = float(b[1])  # bt_data_temp[1548]
        bt_data['bt_capacity'] = float(bt_data_temp[1549])
        bt_data['bt_op_mode'] = self.bt_mode_database(bt_data_temp[1552])

        temp = self.E5.read_registers(1607, 2, 4)
        _debug and print(temp)
        bt_data['bt_module1_temp_min'] = float(temp[1])
        bt_data['bt_module1_temp_max'] = float(temp[0])
        # Grab all dpm_cont_data
        dpm_data = dict()

        dpm_data['test'] = 'This is a test'

        # The structure of modbus_data is:
        # {modbus_data: (inverter_data, bt_data, dpm_data)}

        modbus_data = (inverter_data, bt_data, dpm_data)

        return modbus_data


if __name__ == '__main__':
    modbus_methods = ModbusMethods(Queue())
    while True:
        try:
            payload = modbus_methods.get_modbus_data(_debug=True)
            print('BT SOC is: ', payload[1]['bt_soc'])
            print(payload[0]['dc1_power'])

            time.sleep(1)
        except OSError as e:
            print(e)
            modbus_methods = ModbusMethods(Queue())
        except ValueError as e:
            print(e)
            modbus_methods = ModbusMethods(Queue())

