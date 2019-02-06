import random
import csv
import os

from statistics import stdev
import numpy as np

from math import floor
from statistics import mean
from collections import deque
from datetime import datetime


class AnalyseMethods:
    def __init__(self, firebase_to_analyse_queue, analyze_to_modbus_queue):

        # Define the stock parameters that we need for our analysis:

        # Define the default charging mode
        self._CHARGING_MODE = 'PV_with_BT'
        self.MULTIPLE_CHARGING_MODE = "EVEN_SPLIT_with_GRID"
        # Define the battery aggressiveness setting (only vslid for PV_with_BT)
        self._BUFFER_AGGRESSIVENESS = "Balanced"

        self.charger_list = dict()

        # Define the base charge rate in Amps
        self._BASE_CHARGE_RATE = 6
        # Define the variable for the current charge rate
        self._CURRENT_CHARGE_RATE = self._BASE_CHARGE_RATE
        # Define the current charge rate for the battery at 240V
        self._BATTERY_CHARGE_RATE = 13.5
        # Window size in seconds
        self._WINDOWSIZE = 8

        # Define the charging upper limit threshold
        self.UPPER_THRESHOLD = 0.07
        # Define the charging lower limit threshold
        self.LOWER_THRESHOLD = 0.04
        # Define the increase in charging rate
        self._CHARGE_RATE_INCREASE = 1.05
        # Define the decline in charging rate
        self._CHARGE_RATE_DECREASE = 0.80
        # Define the base buffer (the lowest possible buffer)
        self._BASE_BUFFER = 0.20
        # Define the max buffer (the highest possible buffer)
        self._MAX_BUFFER = 0.4

        # Define the maximum current for standalone mode (limited by the E5)
        self._MAX_STANDALONE_CURRENT = 27
        # Define the maximum current for grid connected modes (limited by the E5, trips out at 28A)
        self._MAX_GRID_CONNECTED_CURRENT = 27

        # Define the temperature that we should throttle our battery - derates to 25A - 2kW (8A 240V)
        self._BATTERY_TEMP_LIMIT = 43
        # Define the cooled off temperature
        self._BATTERY_COOLED_OFF_TEMP = 35
        # Define the throttled current limit (at 240V)
        self._THROTTLED_BATTERY_CURRENT = 8
        # Define a boolean for whether or not we are currently in a temperature throttled state
        self._TEMP_THROTTLED = False

        # Define the lower limit for the BT SOC
        self._BTSOC_LOWER_LIMIT = 5
        # Define the upper limit for the BT SOC
        self._BTSOC_UPPER_LIMIT = 17
        # Define a boolean for whether or not we are currently in a BT SOC throttled state
        self._BTSOC_THROTTLED = False

        # Define the BT SOC that once exceeded, should trigger drain mode to activate
        self._DRAIN_MODE_UPPER_LIMIT = 98
        # Define the BT SOC that will deactivate drain mode if we go below it
        self._DRAIN_MODE_LOWER_LIMIT = 88
        # Define a boolean for whether or not we are currently in drain mode
        self._DRAIN_MODE_ACTIVATED = False

        # Initialize the 'smudge factor'
        self._BUFFER = float()

        # Do we want to log? Only log when we are testing standalone modes
        self._LOG = True

        self._CALIBRATE_DONE = False

        # Create a deque of length windowsize. Will allow us to automatically pop and insert values
        self.pv_window = deque([], self._WINDOWSIZE)

        # Todo: need to find a use for this
        self.charging_wind_down_dict = dict()

        # Define the queues coming into analyse process
        self.firebase_to_analyse_queue = firebase_to_analyse_queue

        # Define the queues going out of the analyze process
        self.analyze_to_modbus_queue = analyze_to_modbus_queue

        # Todo: fix the no analyze logs folder on start up issue
        # # Logging the csv headers. First check if the file exists though
        # _LOG_FILE_NAME = datetime.now().strftime('%Y-%m-%d')
        # if self._LOG and not os.path.isfile('analyse_logs/' + _LOG_FILE_NAME + '.csv'):
        #     with open('analytics_logs/' + _LOG_FILE_NAME + '.csv', 'a') as f:
        #         writer = csv.writer(f)
        #         writer.writerow(
        #             ['time', 'dc1p', 'dc2p', 'dc1v', 'dc2v', 'dc1c', 'dc2c',
        #              'available_current', 'weighted avg buffered', 'sd', 'coeff var', 'buffer', 'selected charge rate',
        #              'floored charge rate'])

        print('Analyze initialized!')

    @staticmethod
    def condition_data(data):
        inverter_data_temp = data[0]

        final_data = dict()
        final_data['ac1v'] = inverter_data_temp['ac1_voltage'] / 10
        final_data['ac1c'] = inverter_data_temp['ac1_current'] / 100
        final_data['ac1p'] = inverter_data_temp['ac1_power']

        final_data['ac2v'] = inverter_data_temp['ac2_voltage'] / 10
        final_data['ac2c'] = inverter_data_temp['ac2_current'] / 100
        final_data['ac2p'] = inverter_data_temp['ac2_power']

        final_data['dc1v'] = inverter_data_temp['dc1_voltage'] / 10
        final_data['dc1c'] = inverter_data_temp['dc1_current'] / 100
        final_data['dc1p'] = inverter_data_temp['dc1_power']

        final_data['dc2v'] = inverter_data_temp['dc2_voltage'] / 10
        final_data['dc2c'] = inverter_data_temp['dc2_current'] / 100
        final_data['dc2p'] = inverter_data_temp['dc2_power']
        final_data['inverter_op_mode'] = inverter_data_temp['inverter_op_mode']
        final_data['inverter_status'] = inverter_data_temp['inverter_status']

        final_data['btsoc'] = data[1]['bt_soc'] / 10

        final_data['bt_module1_temp_max'] = data[1]['bt_module1_temp_max'] / 10

        return final_data

    @staticmethod
    def take_weighted_average(window, window_size, damping_factor):
        # Create an array out of the deque
        window = np.array(list(window))
        # Create an array for the weights
        weights = np.empty(window_size)
        for i in range(window_size):
            weights[i] = damping_factor * (1 - damping_factor) ** i

        return sum(np.multiply(window, np.flip(weights, 0)))

    def log_data(self, data, approx_dc_current, pv_window_mean, z_stats):
        # Logging code
        if self._LOG:
            current_time = datetime.now()
            _LOG_FILE_NAME = current_time.strftime('%Y-%m-%d')
            with open('../data/analytics_logs/' + _LOG_FILE_NAME + '.csv', 'a') as f:
                writer = csv.writer(f)
                writer.writerow(
                    [current_time.strftime('%Y-%M-%d %H:%M:%S'), str(data['dc1p']), str(data['dc2p']),
                     str(data['dc1v']), str(data['dc2v']), str(data['dc1c']), str(data['dc2c']), str(approx_dc_current),
                     str(pv_window_mean), z_stats[0], z_stats[1], str(self._BUFFER),
                     str(self._CURRENT_CHARGE_RATE), str(floor(self._CURRENT_CHARGE_RATE))
                     ])

    def update_dynamic_buffer(self):
        """ This method calculates a dynamic buffer for the charge rate """
        # This function will calculate a dynamic buffer for the charge rate

        # Take the coefficient of variance
        sd = stdev(self.pv_window)
        z = sd / mean(self.pv_window)

        # Add this here so we never divide by 0
        if z == 0:
            z = 0.001

        if self._BUFFER_AGGRESSIVENESS == "Aggressive":
            self._BUFFER = (np.polynomial.polynomial.polyval(np.log(z),
                                                             np.array([47.25, 11.48708905, 0.70729386]))) / 100
            if z > 0.1:
                self._BUFFER = 0.25
            # print('Aggressive buffer is', self._BUFFER)

        if self._BUFFER_AGGRESSIVENESS == "Balanced":
            self._BUFFER = (np.polynomial.polynomial.polyval(np.log(z),
                                                             np.array([49, 9.51104915, 0.47152924]))) / 100
            if z > 0.1:
                self._BUFFER = 0.30
            # print('Balanced buffer is', self._BUFFER)

        elif self._BUFFER_AGGRESSIVENESS == "Conservative":
            self._BUFFER = (np.polynomial.polynomial.polyval(np.log(z),
                                                             np.array([49.25, 6.53613195, 0.14145877]))) / 100
            if z > 0.1:
                self._BUFFER = 0.35
            # print('Conservative buffer is', self._BUFFER)

        elif self._BUFFER_AGGRESSIVENESS == "Ultra Conservative":
            self._BUFFER = (np.polynomial.polynomial.polyval(np.log(z),
                                                             np.array([49.5, 3.56121475, -0.1886117]))) / 100
            if z > 0.1:
                self._BUFFER = 0.40
            # print('Ultra conservative buffer is', self._BUFFER)

        # # Use our quadratic buffer curve
        # self._BUFFER = (-200000 * (z ** 2) + (5000 * z) + 5) / 100
        #
        # if z > 0.01:
        #     self._BUFFER = 0.35

        # # Now we have to dampen the buffer. Find the trend line and get the gradient
        # p = np.polyfit(list(range(0, len(self.pv_window))), self.pv_window, 1)
        # # If the gradient is above 0 (positive trend line)
        # if p[0] > 0:
        #     self._BUFFER /= 2

        return sd, z

    def analyze_charger_list(self):
        """ This function analyses the current charger list and finds the active chargers """

        active_charger_count = 0
        active_charger_list = []
        for charger_id, charger_payload in self.charger_list.items():

            # If this charger is not False, then we add to our count of alive chargers
            if charger_payload['charging']:
                active_charger_count += 1
                active_charger_list.append(charger_id)

            # Check if our chargerID is in the wind down dict
            if charger_id not in self.charging_wind_down_dict:
                # If it's not then we need to add a deque for that chargerID into the dict
                self.charging_wind_down_dict[charger_id] = deque([], self._WINDOWSIZE)

        return active_charger_count, active_charger_list

    def check_and_change_inverter_op_mode(self, current_mode, proposed_mode):
        # Set the inverter mode to standalone mode if it isn't already
        if current_mode != proposed_mode:
            self.analyze_to_modbus_queue.put(
                {'purpose': "inverter_op_mode", "inverter_op_mode": proposed_mode})

    def multiple_charger_calculate_max_charge_standalone(self, data, num_active_chargers):
        # ******************************************************************************************************
        # ******************************** FIRST TRACK OUR SOLAR ***********************************************
        # ******************************************************************************************************

        # print('Our current window is: ', list(self.pv_window))

        # Calculate a new buffer
        z_stats = self.update_dynamic_buffer()

        # Take the weighted average of the current window
        pv_window_mean = self.take_weighted_average(self.pv_window, self._WINDOWSIZE, damping_factor=0.6)

        # Apply the buffer to the weighted average if we are in standalone mode
        if data['inverter_status'] == "Stand Alone":
            pv_window_mean = pv_window_mean * (1 - self._BUFFER)

        # print('We are using', pv_window_mean, 'that includes a buffer of ', self._BUFFER)

        # Define our thresholds
        upper_threshold = self._CURRENT_CHARGE_RATE * (1 + self.UPPER_THRESHOLD)
        lower_threshold = self._CURRENT_CHARGE_RATE * (1 - self.LOWER_THRESHOLD)

        # print('Our threshold is ', lower_threshold, ' to ', upper_threshold)

        # Check if our window mean is greater than the upper threshold
        if pv_window_mean > upper_threshold:
            # Increase the charge rate if window mean is greater.
            self._CURRENT_CHARGE_RATE = self._CURRENT_CHARGE_RATE * self._CHARGE_RATE_INCREASE
            # print('Our mean is', pv_window_mean, "therefore we increase rate to", self._CURRENT_CHARGE_RATE)

        elif pv_window_mean < lower_threshold:
            self._CURRENT_CHARGE_RATE = self._CURRENT_CHARGE_RATE * self._CHARGE_RATE_DECREASE
            # print('Our mean is', pv_window_mean, "therefore we decrease rate to", self._CURRENT_CHARGE_RATE)

        else:
            # print('We are within the threshold! No change needed. Charge rate is ', self._CURRENT_CHARGE_RATE)
            pass

        # # Log all the data
        # self.log_data(data, approx_dc_current, pv_window_mean, z_stats)

        # ******************************************************************************************************
        # ***************************** CHECK FOR ANY TEMP THROTTLE CHANGES ************************************
        # ******************************************************************************************************

        # If we are not throttled and the temperatures go over the limit, then we turn throttle on
        if not self._TEMP_THROTTLED and data['bt_module1_temp_max'] > self._BATTERY_TEMP_LIMIT:
            print('We have reached', data['bt_module1_temp_max'], 'time to throttle')
            self._TEMP_THROTTLED = True

        # If we are throttled and the temperatures go under the cooled off temperature, we turn throttle off
        elif self._TEMP_THROTTLED and data['bt_module1_temp_max'] < self._BATTERY_COOLED_OFF_TEMP:
            print('We have reached', data['bt_module1_temp_max'], 'time to stop throttling')
            self._TEMP_THROTTLED = False

        # ******************************************************************************************************
        # ***************************** ADJUST CHARGE RATE BASED ON THROTTLE************************************
        # ******************************************************************************************************

        if self._TEMP_THROTTLED:
            print('Current charge rate is', self._CURRENT_CHARGE_RATE)
            print('We have reached a high battery temperature, time to throttle')
            self._THROTTLE_ADJUSTED_CHARGE_RATE = floor(
                self._CURRENT_CHARGE_RATE + self._THROTTLED_BATTERY_CURRENT)

        else:
            # PV tracking algorithm finished, now add the battery current that we have and floor
            self._THROTTLE_ADJUSTED_CHARGE_RATE = floor(self._CURRENT_CHARGE_RATE + self._BATTERY_CHARGE_RATE)

        # ******************************************************************************************************
        # *************************** CHECK FOR ANY BTSOC THROTTLE CHANGES *************************************
        # ******************************************************************************************************

        # If we are not BT SOC throttled and we go below the BTSOC lower limit, then we need to throttle
        if not self._BTSOC_THROTTLED and data['btsoc'] < self._BTSOC_LOWER_LIMIT:
            print('We have reached', data['btsoc'], 'SOC, time to throttle')
            self._BTSOC_THROTTLED = True

        # If we are BT SOC throttled and we go above the BTSOC lower limit, then we can disable throttling
        elif self._BTSOC_THROTTLED and data['btsoc'] > self._BTSOC_UPPER_LIMIT:
            print('We have reached', data['btsoc'], 'SOC, time to stop throttling')
            self._BTSOC_THROTTLED = False

        # ******************************************************************************************************
        # *************************** ADJUST CHARGE RATE BASED ON OBTAINED INFORMATION *************************
        # ******************************************************************************************************

        # If the inverter is in standalone mode
        if data['inverter_status'] == "Stand Alone":
            # Make sure we are in self consumption mode
            self.check_and_change_inverter_op_mode(data['inverter_op_mode'], 'SELF_CONSUMPTION_MODE_INTERNAL')

            # ... and we are BT SOC throttled
            if self._BTSOC_THROTTLED:
                # Then we need to stop charging immediately
                return 'stop'

            # If we are not BT SOC throttled, we check how much charge has been allocated

            # if our throttle adjusted charge rate is greater than our minimum, then we are happy
            elif self._THROTTLE_ADJUSTED_CHARGE_RATE > 6 * num_active_chargers:
                return self._THROTTLE_ADJUSTED_CHARGE_RATE

            # but if our throttled adjusted charge rate is LOWER than our minimum
            else:
                # There is no grid to draw from so we probably need to
                # Todo: we might need to stop charging on some cars
                # If we are temperature throttled
                if self._TEMP_THROTTLED:
                    pass

                # If we aren't temperature throttled
                else:
                    pass

        # If the inverter is in GRID CONNECTED MODE, we can draw from the grid to make up any needed power
        else:
            # ... and we are BT SOC throttled
            if self._BTSOC_THROTTLED:
                # Set the inverter mode to charge first so it doesn't use the battery
                self.check_and_change_inverter_op_mode(data['inverter_op_mode'], 'CHARGE_FIRST_MODE')

                # Set charge rate to our minimum charge rate
                return 6 * num_active_chargers

            # ... and we are not BT SOC throttled, we check how much charge is allocated

            # If our throttle adjusted charge rate is greater than our minimum,
            elif self._THROTTLE_ADJUSTED_CHARGE_RATE > 6 * num_active_chargers:
                # Set the inverter mode to standalone mode if it isn't already
                self.check_and_change_inverter_op_mode(data['inverter_op_mode'], 'SELF_CONSUMPTION_MODE_INTERNAL')

                # Let our throttled adjusted charge rate past
                return self._THROTTLE_ADJUSTED_CHARGE_RATE

            # If our throttled adjusted charge rate is LOWER than our minimum
            else:
                # If we are temperature throttled
                if self._TEMP_THROTTLED:
                    # Set the inverter mode to charge first so it doesn't use the battery
                    self.check_and_change_inverter_op_mode(data['inverter_op_mode'], 'CHARGE_FIRST_MODE')

                # Then we increase the charge rate to our minimum charge rate
                return 6 * num_active_chargers

    def calculate_charge_rate(self, data):
        """ This method takes all of the data from the inverter and makes a decision on what the charge rate should
        be taking into account what mode has been selected through the web interface/app """

        # See how munch the total DC generation for the last second was
        approx_dc_current = (data['dc1p'] + data['dc2p']) / data['ac2v']

        # If we have no PV power at all, just set it to 0.01 to prevent divide by 0 errors
        if approx_dc_current == 0:
            approx_dc_current = 0.01

        # Append this total DC generation into the window. We use this window for analysis
        self.pv_window.append(approx_dc_current)

        # Define the inverter's operation mode
        inverter_op_mode = data['inverter_op_mode']

        # Define the inverter's status
        inverter_status = data['inverter_status']

        charger_list_analysis = self.analyze_charger_list()
        num_active_chargers = charger_list_analysis[0]
        active_charger_list = charger_list_analysis[1]

        # Define a dictionary that will contain all of our charge rates
        charge_rate_dict = {"charge_rates": {}}

        # print('ac2p is: ', data['ac2p'])
        # print('Our current charger list is', self.charger_list)
        # print('We have available solar current of ', approx_dc_current)
        # print('Our charging mode is: ', self._CHARGING_MODE)
        # print('Our battery module 1 temperature is:', data['bt_module1_temp_max'])
        # print('Our battery SOC is', data['btsoc'])
        # print('Inverter operation mode is', inverter_op_mode)
        # print('Inverters status is', inverter_status)

        # If we have more than one active charger
        if num_active_chargers > 1:

            # If we have 3 or more chargers then we must be in grid connected mode
            if self._CHARGING_MODE == "MAX_CHARGE_GRID" or num_active_chargers > 2:
                # First make sure that we are in a grid connected mode
                if inverter_status == "Stand Alone":
                    final_charge_rate = self._MAX_STANDALONE_CURRENT

                else:
                    final_charge_rate = self._MAX_GRID_CONNECTED_CURRENT

                # Make sure we are in self consumption mode
                self.check_and_change_inverter_op_mode(data, 'SELF_CONSUMPTION_MODE_INTERNAL')

                # Split the charge rate evenly between all chargers
                split_charge_rate = floor(final_charge_rate / num_active_chargers)

                # Update the dictionary with all of the charge rates
                for charger in active_charger_list:
                    charge_rate_dict['charge_rates'].update({charger: split_charge_rate})

                # Update the available current
                charge_rate_dict.update({'available_current': self._MAX_GRID_CONNECTED_CURRENT - data['ac2c'] + 0.2})

            elif self._CHARGING_MODE == "MAX_CHARGE_STANDALONE" or self._CHARGING_MODE == "PV_with_BT":
                final_charge_rate = self.multiple_charger_calculate_max_charge_standalone(data, num_active_chargers)

                print('final charge rate coming out is', final_charge_rate)
                final_charge_rate = min(27, final_charge_rate)
                print('final charge rate after min fix is:', final_charge_rate)

                # If we do not have a string for the final charge rate then we just split it and go on
                if final_charge_rate is not str:
                    split_charge_rate = floor(final_charge_rate / num_active_chargers)

                # If we do have a string then we don't split it
                else:
                    split_charge_rate = final_charge_rate

                for charger in active_charger_list:
                    charge_rate_dict['charge_rates'].update({charger: split_charge_rate})

                charge_rate_dict.update({'available_current': self._MAX_STANDALONE_CURRENT - data['ac2c']})

            elif self.MULTIPLE_CHARGING_MODE == "PRIORITY_LIST":
                pass

            return charge_rate_dict

        # If there is one active charger
        elif num_active_chargers == 1:
            if self._CHARGING_MODE == "MAX_CHARGE_GRID":
                ''' This mode will use the max current possible - including drawing from grid '''
                self.check_and_change_inverter_op_mode(inverter_op_mode, 'SELF_CONSUMPTION_MODE_INTERNAL')

                # First make sure that we are in a grid connected mode
                if inverter_status == "Stand Alone":
                    final_charge_rate = self._MAX_STANDALONE_CURRENT

                else:
                    final_charge_rate = self._MAX_GRID_CONNECTED_CURRENT

            elif self._CHARGING_MODE == "MAX_CHARGE_STANDALONE":

                # ******************************************************************************************************
                # ******************************** FIRST TRACK OUR SOLAR ***********************************************
                # ******************************************************************************************************

                # print('Our current window is: ', list(self.pv_window))

                # Calculate a new buffer
                z_stats = self.update_dynamic_buffer()

                # Take the weighted average of the current window
                pv_window_mean = self.take_weighted_average(self.pv_window, self._WINDOWSIZE, damping_factor=0.6)

                # Apply the buffer to the weighted average if we are in standalone mode
                if inverter_status == "Stand Alone":
                    pv_window_mean = pv_window_mean * (1 - self._BUFFER)

                # print('We are using', pv_window_mean, 'that includes a buffer of ', self._BUFFER)

                # Define our thresholds
                upper_threshold = self._CURRENT_CHARGE_RATE * (1 + self.UPPER_THRESHOLD)
                lower_threshold = self._CURRENT_CHARGE_RATE * (1 - self.LOWER_THRESHOLD)

                # print('Our threshold is ', lower_threshold, ' to ', upper_threshold)

                # Check if our window mean is greater than the upper threshold
                if pv_window_mean > upper_threshold:
                    # Increase the charge rate if window mean is greater.
                    self._CURRENT_CHARGE_RATE = self._CURRENT_CHARGE_RATE * self._CHARGE_RATE_INCREASE
                    # print('Our mean is', pv_window_mean, "therefore we increase rate to", self._CURRENT_CHARGE_RATE)

                elif pv_window_mean < lower_threshold:
                    self._CURRENT_CHARGE_RATE = self._CURRENT_CHARGE_RATE * self._CHARGE_RATE_DECREASE
                    # print('Our mean is', pv_window_mean, "therefore we decrease rate to", self._CURRENT_CHARGE_RATE)

                else:
                    # print('We are within the threshold! No change needed. Charge rate is ', self._CURRENT_CHARGE_RATE)
                    pass

                # # Log all the data
                # self.log_data(data, approx_dc_current, pv_window_mean, z_stats)

                # ******************************************************************************************************
                # ***************************** CHECK FOR ANY TEMP THROTTLE CHANGES ************************************
                # ******************************************************************************************************

                # If we are not throttled and the temperatures go over the limit, then we turn throttle on
                if not self._TEMP_THROTTLED and data['bt_module1_temp_max'] > self._BATTERY_TEMP_LIMIT:
                    print('We have reached', data['bt_module1_temp_max'], 'time to throttle')
                    self._TEMP_THROTTLED = True

                # If we are throttled and the temperatures go under the cooled off temperature, we turn throttle off
                elif self._TEMP_THROTTLED and data['bt_module1_temp_max'] < self._BATTERY_COOLED_OFF_TEMP:
                    print('We have reached', data['bt_module1_temp_max'], 'time to stop throttling')
                    self._TEMP_THROTTLED = False

                # ******************************************************************************************************
                # ***************************** ADJUST CHARGE RATE BASED ON THROTTLE************************************
                # ******************************************************************************************************

                if self._TEMP_THROTTLED:
                    print('Current charge rate is', self._CURRENT_CHARGE_RATE)
                    print('We have reached a high battery temperature, time to throttle')
                    self._THROTTLE_ADJUSTED_CHARGE_RATE = floor(
                        self._CURRENT_CHARGE_RATE + self._THROTTLED_BATTERY_CURRENT)

                else:
                    # PV tracking algorithm finished, now add the battery current that we have and floor
                    self._THROTTLE_ADJUSTED_CHARGE_RATE = floor(self._CURRENT_CHARGE_RATE + self._BATTERY_CHARGE_RATE)

                # ******************************************************************************************************
                # *************************** CHECK FOR ANY BTSOC THROTTLE CHANGES *************************************
                # ******************************************************************************************************

                # If we are not BT SOC throttled and we go below the BTSOC lower limit, then we need to throttle
                if not self._BTSOC_THROTTLED and data['btsoc'] < self._BTSOC_LOWER_LIMIT:
                    print('We have reached', data['btsoc'], 'SOC, time to throttle')
                    self._BTSOC_THROTTLED = True

                # If we are BT SOC throttled and we go above the BTSOC lower limit, then we can disable throttling
                elif self._BTSOC_THROTTLED and data['btsoc'] > self._BTSOC_UPPER_LIMIT:
                    print('We have reached', data['btsoc'], 'SOC, time to stop throttling')
                    self._BTSOC_THROTTLED = False

                # ******************************************************************************************************
                # ********************** INVERTER STATUS AND BATTERY SOC CHECK AND ADJUSTMENTS *************************
                # ******************************************************************************************************

                # At this stage we have a charge rate ready to go. But first we check inverter status and battery SOC
                if inverter_status == "Stand Alone":
                    self.check_and_change_inverter_op_mode(inverter_op_mode, 'SELF_CONSUMPTION_MODE_INTERNAL')

                    # If we are in stand alone mode and we are battery throttled, we need to stop charging ASAP
                    if self._BTSOC_THROTTLED:
                        print('BT SOC is:', data['btsoc'],
                              ', we are throttled and we are in standalone mode, time to stop charging')
                        self._THROTTLE_ADJUSTED_CHARGE_RATE = 'stop'

                # If we are in grid connected mode then we can continue to charge at a reduced rate utilising the grid
                else:
                    # If we are in grid connected mode and we are BT SOC throttled then...
                    if self._BTSOC_THROTTLED:
                        # We reduce to 6A charge rate if our solar is only at 7A. Grid will be utilised when BT is 0%
                        if self._CURRENT_CHARGE_RATE < 6:
                            # Make sure we are in charge first mode
                            self.check_and_change_inverter_op_mode(inverter_op_mode, 'CHARGE_FIRST_MODE')

                            print('BT SOC is:', data['btsoc'], 'current charge rate is', self._CURRENT_CHARGE_RATE,
                                  'and we are in grid connected mode', 'limiting to 6A')
                            self._THROTTLE_ADJUSTED_CHARGE_RATE = 6

                        # If our solar is above 6A then we can just charge at that same rate
                        else:
                            self.check_and_change_inverter_op_mode(inverter_op_mode,
                                                                   'SELF_CONSUMPTION_MODE_INTERNAL')

                            print('BT SOC is:', data['btsoc'], 'current charge rate is', self._CURRENT_CHARGE_RATE,
                                  'and we are in grid connected mode', 'going for solar charging only')
                            self._THROTTLE_ADJUSTED_CHARGE_RATE = floor(self._THROTTLE_ADJUSTED_CHARGE_RATE)

                    # If we are NOT BT SOC throttled, then we don't touch the charge rate at all!
                    else:
                        self.check_and_change_inverter_op_mode(inverter_op_mode,
                                                               'SELF_CONSUMPTION_MODE_INTERNAL')
                # ******************************************************************************************************
                # **************************** MAKE SURE CHARGE RATE IS WITHIN LIMITS **********************************
                # ******************************************************************************************************

                # Now make sure we aren't above max standalone current and below 6A
                if self._THROTTLE_ADJUSTED_CHARGE_RATE is not str:
                    final_charge_rate = max(min(self._THROTTLE_ADJUSTED_CHARGE_RATE, self._MAX_STANDALONE_CURRENT), 6)
                else:
                    final_charge_rate = self._THROTTLE_ADJUSTED_CHARGE_RATE

            elif self._CHARGING_MODE == "PV_no_BT":
                # Todo: WIP
                final_charge_rate = 'stop'

            elif self._CHARGING_MODE == "PV_with_BT":
                ''' This mode will track the solar available using the battery as a fail safe '''

                self.check_and_change_inverter_op_mode(inverter_op_mode, 'SELF_CONSUMPTION_MODE_INTERNAL')

                # If we have at least 1A of solar power then we can run the whole analysis code
                if approx_dc_current > 1:

                    # **************************************************************************************************
                    # ******************************** FIRST TRACK OUR SOLAR *******************************************
                    # **************************************************************************************************

                    # print('Our current window is: ', list(self.pv_window))

                    # Calculate a new buffer which depends on the buffer aggressiveness setting
                    z_stats = self.update_dynamic_buffer()

                    # Take the weighted average of the current window
                    pv_window_mean = self.take_weighted_average(self.pv_window, self._WINDOWSIZE, damping_factor=0.6)
                    # Apply the buffer to the weighted average
                    pv_window_mean = pv_window_mean * (1 - self._BUFFER)

                    # print('We are using', pv_window_mean, 'that includes a buffer of ', self._BUFFER)

                    # Define our thresholds
                    upper_threshold = self._CURRENT_CHARGE_RATE * (1 + self.UPPER_THRESHOLD)
                    lower_threshold = self._CURRENT_CHARGE_RATE * (1 - self.LOWER_THRESHOLD)

                    # print('Our threshold is ', lower_threshold, ' to ', upper_threshold)

                    # Check if our window mean is greater than the upper threshold
                    if pv_window_mean > upper_threshold:
                        # Increase the charge rate if window mean is greater.
                        self._CURRENT_CHARGE_RATE = self._CURRENT_CHARGE_RATE * self._CHARGE_RATE_INCREASE
                        # print('Our mean is', pv_window_mean, "therefore we increase rate to", self._CURRENT_CHARGE_RATE)

                    elif pv_window_mean < lower_threshold:
                        self._CURRENT_CHARGE_RATE = self._CURRENT_CHARGE_RATE * self._CHARGE_RATE_DECREASE
                        # print('Our mean is', pv_window_mean, "therefore we decrease rate to", self._CURRENT_CHARGE_RATE)

                    else:
                        # print('We are within the threshold! No change needed. Charge rate is ', self._CURRENT_CHARGE_RATE)
                        pass

                    # # Log all the data
                    # self.log_data(data, approx_dc_current, pv_window_mean, z_stats)

                    # print('Now we floor it:', floor(self._CURRENT_CHARGE_RATE))

                else:
                    # We are below 1A of DC power, so just let the current charge rate be 1A
                    self._CURRENT_CHARGE_RATE = 1

                # ******************************************************************************************************
                # ************************ ADJUST CHARGE RATE BASED ON INVERTER STATUS/BATTERY SOC *********************
                # ******************************************************************************************************
                # If we are in stand alone mode...
                if inverter_status == "Stand Alone":

                    # and drain mode has been activated...
                    if self._DRAIN_MODE_ACTIVATED:

                        # ... and we are still above 88% BT SOC
                        if data['btsoc'] > self._DRAIN_MODE_LOWER_LIMIT:
                            # ... then we need to drain the battery

                            print('We are in standalone mode and drain mode, charge at max standalone current')
                            # self._CURRENT_CHARGE_RATE = floor(self._BATTERY_CHARGE_RATE + self._CURRENT_CHARGE_RATE)
                            # floor(self._BATTERY_CHARGE_RATE + self._CURRENT_CHARGE_RATE)
                            # if self._CURRENT_CHARGE_RATE > self._MAX_STANDALONE_CURRENT:
                            #     self._CURRENT_CHARGE_RATE = self._MAX_STANDALONE_CURRENT

                            self._CURRENT_CHARGE_RATE = min(
                                floor(self._BATTERY_CHARGE_RATE + self._CURRENT_CHARGE_RATE),
                                self._MAX_STANDALONE_CURRENT)

                            final_charge_rate = floor(self._CURRENT_CHARGE_RATE)

                        # If in drain mode and BT SOC is below SOC lower limit, we can deactivate drain mode
                        else:
                            print('We are no longer above 88% SOC - deactivate drain mode')
                            self._DRAIN_MODE_ACTIVATED = False
                            final_charge_rate = floor(self._CURRENT_CHARGE_RATE)

                    # If drain mode has not been activated
                    else:
                        # ...then we check if our BT SOC is above a certain level
                        if data['btsoc'] > self._DRAIN_MODE_UPPER_LIMIT:
                            print('Our BT SOC has gone above', self._DRAIN_MODE_UPPER_LIMIT, 'activate drain mode')
                            # Then we need to activate drain mode
                            self._DRAIN_MODE_ACTIVATED = True
                            final_charge_rate = floor(self._CURRENT_CHARGE_RATE)

                        # If we are not in drain mode and our BT SOC is below 5%, we tell the charger to stop
                        elif data['btsoc'] < 5:
                            final_charge_rate = 'stop'

                        # If no adjustments are needed then make sure charge rate isn't below 6A
                        else:
                            final_charge_rate = max(floor(self._CURRENT_CHARGE_RATE), 6)

                # If we are NOT in stand alone mode, just act normal
                else:
                    # If our calculated charge rate is below 6A
                    if floor(self._CURRENT_CHARGE_RATE) < 6:

                        # We maintain a charge rate of 6A since we are supported by the grid/battery
                        final_charge_rate = 6

                    # If our calculated charge rate is above 6A, we have enough solar so we are all good
                    else:
                        final_charge_rate = floor(self._CURRENT_CHARGE_RATE)

            charge_rate_dict.update({'charge_rates': {active_charger_list[0]: final_charge_rate}})

            # **********************************************************************************************************
            # ******************************** UPDATE OUR AVAILABLE CURRENT ********************************************
            # **********************************************************************************************************
            if self._CHARGING_MODE == "MAX_CHARGE_GRID":
                available_current = self._MAX_GRID_CONNECTED_CURRENT + 0.2

            # If we are in max charge standalone charging mode
            elif self._CHARGING_MODE == "MAX_CHARGE_STANDALONE":

                # And the inverter is in standalone mode
                if inverter_status == "Stand Alone":

                    # If the battery SOC is greater than 5%, then we can rely on the battery
                    if data['btsoc'] > 5:
                        available_current = self._MAX_STANDALONE_CURRENT + 0.2

                    # If the battery SOC is less than 5%, then we cannot rely on the battery
                    else:
                        available_current = floor(approx_dc_current)

                # If the inverter is in grid connected mode
                else:
                    # If the battery SOC is greater than 5%, then we can rely on the battery
                    if data['btsoc'] > 5:
                        available_current = self._MAX_STANDALONE_CURRENT + 0.2

                    # If the battery SOC is less than 5%, then we cannot rely on the battery but we can still charge
                    # with the grid. We will charge at the lowest charge rate
                    else:
                        available_current = 6.2

            elif self._CHARGING_MODE == "PV_with_BT":
                # available_current = floor(approx_dc_current)

                # If the battery SOC is greater than 10%, then we can rely on the battery
                if data['btsoc'] > 10:
                    available_current = self._MAX_STANDALONE_CURRENT + 0.2

                # If the battery SOC is less than 10%, then we cannot rely on the battery
                else:
                    available_current = 6.2

            else:
                available_current = floor(approx_dc_current)

            charge_rate_dict.update({'available_current': available_current})

            return charge_rate_dict

        # If there is no active chargers
        elif num_active_chargers == 0:
            ############################################################################################################
            # There are no active chargers, so we just take the charging mode and tell Firebase Methods how much
            # current we have available to start charging
            ############################################################################################################
            self.check_and_change_inverter_op_mode(data['inverter_op_mode'], 'SELF_CONSUMPTION_MODE_INTERNAL')

            if self._CHARGING_MODE == "MAX_CHARGE_GRID":
                available_current = 27

            elif self._CHARGING_MODE == "MAX_CHARGE_STANDALONE":
                # If the battery SOC is greater than 10%, then we can rely on the battery again
                if data['btsoc'] > 10:
                    available_current = approx_dc_current + self._MAX_STANDALONE_CURRENT

                # If the battery SOC is less than 10%, then we cannot rely on the battery
                else:
                    available_current = approx_dc_current

            elif self._CHARGING_MODE == "PV_with_BT":
                available_current = self._MAX_STANDALONE_CURRENT

            else:
                available_current = self._MAX_STANDALONE_CURRENT

            return {
                'available_current': available_current,
                'charge_rates': {}
            }

    def calibrate_charge_rate(self, data):

        # See how much the total DC generation for the last second was
        approx_dc_current = (data['dc1p'] + data['dc2p']) / data['ac2v']

        # Append this total DC generation into the window
        self.pv_window.append(approx_dc_current)

        # If we haven't reached the window size, we must skip this second
        if len(self.pv_window) != self._WINDOWSIZE:
            # print('We havent reached the required window size yet!')
            pass
        else:
            self._CALIBRATE_DONE = True

    def change_algorithm_variables(self):
        """ This function will adjust all of the important variables needed for the algorithm depending on the
        charging mode """
        print('Changing the algorithm variables')
        if self._CHARGING_MODE == "PV_with_BT":
            # Define the charging upper limit threshold
            self.UPPER_THRESHOLD = 0.07
            # Define the charging lower limit threshold
            self.LOWER_THRESHOLD = 0.04
            # Define the increase in charging rate
            self._CHARGE_RATE_INCREASE = 1.20
            # Define the decline in charging rate
            self._CHARGE_RATE_DECREASE = 0.90

            # Define the base buffer (the lowest possible buffer)
            self._BASE_BUFFER = 0.20
            # Define the max buffer (the highest possible buffer)
            self._MAX_BUFFER = 0.4

        elif self._CHARGING_MODE == "PV_no_BT":
            # These values are the stock values

            # Define the charging upper limit threshold
            self.UPPER_THRESHOLD = 0.07
            # Define the charging lower limit threshold
            self.LOWER_THRESHOLD = 0.04
            # Define the increase in charging rate
            self._CHARGE_RATE_INCREASE = 1.05
            # Define the decline in charging rate
            self._CHARGE_RATE_DECREASE = 0.80
            # Define the base buffer (the lowest possible buffer)
            self._BASE_BUFFER = 0.20
            # Define the max buffer (the highest possible buffer)
            self._MAX_BUFFER = 0.4

    def make_decision(self, modbus_data):

        # Check for any new data from Firebase/OCPP WS
        if not self.firebase_to_analyse_queue.empty():
            payload = self.firebase_to_analyse_queue.get()
            purpose = payload['purpose']

            # If our purpose is to change charge mode, then change charge mode
            if purpose == "change_single_charging_mode":
                self._CHARGING_MODE = payload['charge_mode']
                self.change_algorithm_variables()
                print('change in charging mode', self._CHARGING_MODE)

            # If our purpose is charge status, we have a new list of dicts for charging. This only happens when a
            # charger starts or stops charging.
            # self.charger_list will be a list of dicts: [{'chargerID': MEL-DCWB, ''}]
            elif purpose == "charge_status":
                print('We got a new charging list')
                self.charger_list = payload['charger_list']

            # If our purpose it to change the buffer aggressiveness mode, we just update the variable
            elif purpose == "buffer_aggro_change":
                print('Buffer aggressiveness changed to', payload['buffer_aggro_mode'])
                self._BUFFER_AGGRESSIVENESS = payload['buffer_aggro_mode']

            elif purpose == "metervalue_current":
                # Todo: might be exception here if chargerID hasn't been defined yet
                temp_charger_id = payload['chargerID']
                temp_metervalue_current = payload['metervalue_current']
                self.charging_wind_down_dict[temp_charger_id].append(temp_metervalue_current)

        # First we need to condition the data into the correct format
        data = self.condition_data(modbus_data)

        try:
            # If we have not completed calibration
            if not self._CALIBRATE_DONE:
                # We need to calibrate our system first
                self.calibrate_charge_rate(data)

                if self._CHARGING_MODE == "PV_no_BT":
                    return 'stop'
                else:
                    return {'available_current': 6,
                            'charge_rates': {}}

            # If we have calibrated already
            else:
                # Calculate and return the charge rate
                charge_rate = self.calculate_charge_rate(data)
                return charge_rate

        except ZeroDivisionError as error:
            print('We have no PV, just skip. Error:', error)
            return {
                'available_current': 0,
                'charge_rates': {}
            }
