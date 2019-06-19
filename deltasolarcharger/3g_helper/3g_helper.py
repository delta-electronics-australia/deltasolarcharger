import sys
import datetime
import time
import os
import select
import pprint
from systemd import journal

# Create a systemd.journal.Reader instance
j = journal.Reader()

# Set the reader's default log level
j.log_level(journal.LOG_INFO)

# Only include entries since the current box has booted.
j.this_boot()
j.this_machine()

# Filter log entries
j.add_match(_SYSTEMD_UNIT='NetworkManager.service')

# Move to the end of the journal
j.seek_tail()

# Important! - Discard old journal entries
j.get_previous()

# Create a poll object for journal entries
p = select.poll()

# Register the journal's file descriptor with the polling object.
journal_fd = j.fileno()
poll_event_mask = j.get_events()
p.register(journal_fd, poll_event_mask)

# Poll for new journal entries every 150ms
while True:
    if p.poll(150):
        if j.process() == journal.APPEND:
            for entry in j:
                if "ClientIdsExhausted" in entry["MESSAGE"]:
                    print('Got a ClientIDsExhausted message!')
                    os.system(
                        'sudo qmicli -d /dev/cdc-wdm0 --wds-get-packet-service-status --device-open-sync -p; reboot')
                elif "No DHCPOFFERS received" in entry["MESSAGE"] or ('dhcp4' in entry['MESSAGE'] and 'request timed out' in entry['MESSAGE']):
                    print("Received no DHCP offers, let's restart the system")
                    os.system(
                        'sudo dhclient -r; sudo dhclient; sudo qmicli -d /dev/cdc-wdm0 --wds-get-packet-service-status --device-open-sync -p; reboot')
                else:
                    pprint.pprint(entry)
                    print('*********************************')
