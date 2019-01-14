import time
import pyrebase
import ast
import csv
from datetime import datetime


class WebAnalyticsMethods:
    def __init__(self):
        super().__init__()
        self.today = datetime.now().day
        self.current_analytics_data = dict()

        self.analyze_csv_integrity()

    @staticmethod
    def analyze_csv_integrity():
        """ This function analyzes the csv file and makes sure that there are no NULL bytes """

        def fix_nulls(s):
            """ This function takes a generator that replaces NULL bytes with blank """
            for line in s:
                yield line.replace('\0', '')

        try:
            current_csv = datetime.now().strftime('%Y-%m-%d') + '.csv'
            fixed_csv_list = None

            with open('../data/logs/' + current_csv) as csvfile:
                if '\0' in csvfile.read():
                    print('We found a null byte, lets fix it')

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
                with open('../data/logs/' + current_csv, mode='w', newline='') as f:
                    writer = csv.writer(f)

                    # Write the uncorrupt rows to it and replace the file
                    for row in fixed_csv_list:
                        if len(row) != 0:
                            writer.writerow(row)

                print('Fixed the csv file!!')

        except FileNotFoundError as e:
            print('File doesnt exist! Skipping integrity check')

    def sync_analytics_data(self):
        self.analyse_todays_history()

    def analyse_todays_history(self):
        print('Analysing todays past data...')

        try:
            current_csv = datetime.now().strftime('%Y-%m-%d') + '.csv'
            with open('../data/logs/' + current_csv, 'r') as f:
                dcp_total = 0
                utility_p_export_total = 0
                utility_p_import_total = 0
                btp_consumed_total = 0
                btp_charged_total = 0
                ac2p_total = 0

                reader = csv.reader(f)
                next(reader)
                for row in reader:
                    dc1p = float(row[7])
                    dc2p = float(row[10])
                    ac2p = float(row[4])
                    utility_p = float(row[17])
                    btp = float(row[13])

                    dcp_total += ((dc1p + dc2p) * (2 / 3600)) / 1000
                    ac2p_total += (ac2p * (2 / 3600)) / 1000

                    if utility_p >= 0:
                        utility_p_export_total += (utility_p * (2 / 3600)) / 1000
                    else:
                        utility_p_import_total += (utility_p * (2 / 3600)) / 1000

                    if btp >= 0:
                        btp_consumed_total += (btp * (2 / 3600)) / 1000
                    else:
                        btp_charged_total += (btp * (2 / 3600)) / 1000
                print('Done!')
                print('DC Power: ', dcp_total)
                print('Utility export/import: ', utility_p_export_total, utility_p_import_total)
                print('Battery Consumed/Charged: ', btp_consumed_total, btp_charged_total)
                print('AC2 Power: ', ac2p_total)

            # Final synchronised data:
            self.current_analytics_data = {'dcp_t': dcp_total,
                                           'utility_p_export_t': utility_p_export_total,
                                           'utility_p_import_t': utility_p_import_total,
                                           'btp_charged_t': btp_charged_total,
                                           'btp_consumed_t': btp_consumed_total,
                                           'ac2p_t': ac2p_total}

        except FileNotFoundError as e:
            print(e, 'creating blank analytics object')
            # Final synchronised data:
            self.current_analytics_data = {'dcp_t': 0,
                                           'utility_p_export_t': 0,
                                           'utility_p_import_t': 0,
                                           'btp_charged_t': 0,
                                           'btp_consumed_t': 0,
                                           'ac2p_t': 0}

    def update_analytics(self, new_data):
        day = datetime.now().day
        # Check if we have gone to a new day, if we have then we have to reset the current_analytics dict
        if day != self.today:
            print('New day - reset analytics!')
            self.current_analytics_data = {'dcp_t': 0,
                                           'utility_p_export_t': 0,
                                           'utility_p_import_t': 0,
                                           'btp_consumed_t': 0,
                                           'btp_charged_t': 0,
                                           'ac2p_t': 0}
            self.today = day

        inverter_data_temp = new_data[0]
        self.current_analytics_data['dcp_t'] += ((inverter_data_temp['dc1_power'] + inverter_data_temp['dc2_power']) * (
                1 / 3600)) / 1000
        self.current_analytics_data['ac2p_t'] += (inverter_data_temp['ac2_power'] * (1 / 3600)) / 1000

        bt_data_temp = new_data[1]
        if bt_data_temp['utility_power'] >= 0:
            self.current_analytics_data['utility_p_export_t'] += (bt_data_temp['utility_power'] * (1 / 3600)) / 1000
        else:
            self.current_analytics_data['utility_p_import_t'] += (bt_data_temp['utility_power'] * (1 / 3600)) / 1000

        if float(-1 * bt_data_temp['bt_wattage']) >= 0:
            self.current_analytics_data['btp_consumed_t'] += (float(-1 * bt_data_temp['bt_wattage']) * (
                    1 / 3600)) / 1000
        else:
            self.current_analytics_data['btp_charged_t'] += (float(-1 * bt_data_temp['bt_wattage']) * (1 / 3600)) / 1000


if __name__ == '__main__':
    test = WebAnalyticsMethods()
    test.sync_analytics_data()
