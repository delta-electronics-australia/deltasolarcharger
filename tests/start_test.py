# import pyrebase
#
# config = {
#     "apiKey": "AIzaSyCaxTOBofd7qrnbas5gGsZcuvy_zNSi_ik",
#     "authDomain": "smart-charging-app.firebaseapp.com",
#     "databaseURL": "https://smart-charging-app.firebaseio.com",
#     "storageBucket": "",
# }
#
# firebase = pyrebase.initialize_app(config)
# db = firebase.database()
#
# latest_version = db.child('version_info/latest_version').get()
# latest_version = latest_version.val()
# print(latest_version)

from datetime import datetime, timedelta
from ftplib import FTP
import os
from io import StringIO
import pyrebase

port = 21
ip = "203.32.104.46"
user = 'delta'
password = 'P@ssw8rd'


# os.chdir("C:\\Users\\benja\\Desktop\\ftptest")
#
# # with FTP(host=ip) as ftp:
# #     ftp.login(user=user, passwd=password)
# #     directory = "/EVCS_Control_System"
# #     filematch = 'version.txt'
# #     ftp.cwd(directory)
# #
# #     for file_name in ftp.nlst(filematch):
# #         r = StringIO()
# #         ftp.retrlines('RETR ' + file_name, r.write)
# #         print(r.getvalue())
# #
# # print('done!')
# with FTP(host=ip) as ftp:
#     ftp.login(user=user, passwd=password)
#     directory = "/EVCS_portal/logs/4nRoT4gFnzg8sIwKwLXZsYjrRzI3/"
#     filematch = '*.py'
#     ftp.cwd(directory)
#     a = ftp.nlst()
#     print(a)
#
#     print('2018-05-28.csv' in a)
#     print(ftp.size('2018-05-28.csv'))


# for filename in ftp.nlst('*.py'):
#     with open(filename, 'wb') as file:
#         print('getting ' + filename)
#         ftp.retrbinary('RETR ' + filename, file.write)
#
# for filename in ftp.nlst('version.txt'):
#     with open(filename, 'wb') as file:
#         print('getting ' + filename)
#         ftp.retrbinary('RETR ' + filename, file.write)


class test:
    def authenticate(self):
        config = {
            "apiKey": "AIzaSyCaxTOBofd7qrnbas5gGsZcuvy_zNSi_ik",
            "authDomain": "smart-charging-app.firebaseapp.com",
            "databaseURL": "https://smart-charging-app.firebaseio.com",
            "storageBucket": "",
        }

        print('Attempting to authenticate with Firebase')
        firebase = pyrebase.initialize_app(config)
        self.auth = firebase.auth()
        self.user = self.auth.sign_in_with_email_and_password("jgv11@gmail.com", "test123")
        self.uid = self.user['localId']
        self.idToken = self.user['idToken']
        self.refreshToken = self.user['refreshToken']

        self.db = firebase.database()

    def handle_database(self, end_of_day=True):
        """ This method organises our Firebase and server at 12am every day so that everything is organised """

        # Todo: think about the logic here
        # Post the current date to history_keys
        if end_of_day:
            self.db.child("users").child(self.uid).child("history_keys").update(
                {datetime.now().strftime("%Y-%m-%d"): True}, self.idToken)

        # Download 30 history_keys. These keys will be the ones that will be in the history database
        raw_list = list(
            self.db.child("users").child(self.uid).child("history_keys").order_by_key().limit_to_last(30).get(
                self.idToken).val().items())
        valid_dates = [date[0] for date in raw_list]
        print('The dates we will be keeping are:', valid_dates)

        # Now download 40 history_keys - 'extended_dates'
        extended_list = list(
            self.db.child("users").child(self.uid).child("history_keys").order_by_key().limit_to_last(40).get(
                self.idToken).val().items())
        extended_dates = [date[0] for date in extended_list]

        # Finally, download a list of .csv files that exist for the user
        with FTP(host=ip) as ftp:
            ftp.login(user=user, passwd=password)
            ftp_directory = "/EVCS_portal/logs/" + self.uid + '/'
            ftp.cwd(ftp_directory)
            csv_list = ftp.nlst()

            # Loop through all of the 40 latest history_keys and check these against our valid dates
            for date in extended_dates:
                print('checking', date)
                filename = date + '.csv'
                # If we have discovered that a date that is not in our list of 30 valid_dates, it must be archived
                if date not in valid_dates:
                    print(date, 'no good')
                    # Check if we have a csv backup on our server before deleting it from Firebase
                    if (date + '.csv') not in csv_list:
                        print(date, 'is not in csv_list, lets upload it')
                        with open('logs/' + filename, 'rb') as file:
                            # ftp.storbinary('STOR ' + ftp_directory + filename, file)
                            ftp.storbinary('STOR ' + filename, file)

                    self.db.child("users").child(self.uid).child("history_keys").update({date: 'archived'})
                    self.db.child('users').child(self.uid).child('history').child(date).remove()

                # If our date IS WITHIN valid_dates, check the integrity of the csv
                else:
                    # todo: add code here that exempts the most current day from integrity checking (pointless)
                    # If the looped date is NOT today
                    if date != datetime.now().strftime("%Y-%m-%d"):
                        print(date, 'is a valid date. Checking integrity of csv on our server...')

                        # First check if the file exists in the ftp server
                        if (date + '.csv') in csv_list:
                            # Get size of ftp csv file for that valid date, compare to size of local csv file.
                            # If they mismatch, upload the local one of that date to the ftp server
                            filesize_ftp = ftp.size(date + '.csv')
                            filesize_local = os.path.getsize('logs/' + filename)
                            print('ftp filesize =', filesize_ftp, 'compared to local:', filesize_local)
                            if filesize_ftp != filesize_local:
                                print('local and ftp are not the same, updating ftp')
                                with open('logs/' + filename, 'rb') as file:
                                    # ftp.storbinary('STOR ' + ftp_directory + filename, file)
                                    ftp.storbinary('STOR ' + filename, file)
                            else:
                                # File sizes are the same. Don't need to do anything
                                pass
                        # If the file does not exist on the ftp server (something has gone wrong), we need to upload it
                        else:
                            print(date, 'does not exist on ftp server, upload it now')
                            with open('logs/' + filename, 'rb') as file:
                                ftp.storbinary('STOR ' + filename, file)


a = test()
a.authenticate()
a.handle_database()
