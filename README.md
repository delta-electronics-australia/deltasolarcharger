# Delta Solar Charger

Welcome to the repository for the Delta Solar Charger

## Setting up a Delta Solar Charger
This section will contains instructions on how to properly setup the Delta Solar Charger Hardware and install the Delta 
Solar Charger software.

### Hardware Requirements
#### Computing Hardware Requirements
- Compulab IoT Gateway
- CAT 6 cable
- RJ11 cable
- An ethernet cable or an active 3G micro SIM card (if your IoT device has a cellular modem)

#### Non Computing Hardware Requirements
- Solar panels installed on your roof
- Delta E5 5kW Hybrid Inverter
- Delta BX6.0 Battery
- Up to 4 Delta AC Mini Plus chargers

### Installation Steps
Now that you have all of your hardware, you are now ready to setup the Delta Solar Charger system

#### Hardware Setup
##### Step 1: Setup internet connectivity for your IoT device
The first step will be to ensure that your Raspberry Pi powered computer has an internet connection available. This can 
be done by connecting an ethernet cable from your router to the IoT Gateway or inserting an activated SIM
card into the modem

##### Step 2: Wire up the RS485 and power connection between the E5 inverter and your IoT device 
The next step will be to establish a power and RS485 connection between the inverter and the IoT device. The best way to do it is
using a CAT6 cable. Reserve 3 cores for the positive DC line, 3 cores for the negative DC line, 1 core for positive data
line and the last core for the negative data line.

The aim is to be able to handle both power transfer and data transfer through the E5 communications card.

##### Step 3: Label the IoT Gateway device
Every Delta Solar Charger should have a unique ID. The list of unique IDs will be listed in the 'Solar Charger List.xlsx'
file. If setting up a new Solar Charger, print a label with the ID number (eg. 0004) and stick it on the bottom of the 
IoT gateway. This ID number should also be used as the password of the custom on board router.

#### Software Setup
Software setup is really simple. Simply clone the Delta Solar Charger GitHub repository to your home directory (/home/pi):

`cd /home/pi/; git clone https://github.com/delta-electronics-australia/deltasolarcharger; cd deltasolarcharger; chmod +x install.sh;`

`install.sh` is an install script that installs of the dependencies and configures all of the files needed to run the deltasolarcharger.
 Here is a total list of what this script performs:
- Installs dependencies for the on board Wi-Fi router
- Configures all of the necessary files for the on board Wi-Fi router to work
- Installs the Python dependencies to run the deltasolarcharger code
- Allows the deltasolarcharger program to run every time the device boots up 

The install script takes in two arguments:
- The desired Wi-FI SSID for the in built router: use Delta_Solar_Charger_*id*
- The password for this SSID: use DELTA*id*

For example, if the unit you are setting up is ID 0005, the SSID you would use if `Delta_Solar_Charger_0005` and the 
password you would use is `DELTA0005`

In this example, the command you would use is:

`sudo ./install.sh Delta_Solar_Charger_0005 DELTA0005`

Once this script finishes running, the system should reboot by itself. Once the system has rebooted, the on board Wi-Fi 
router will have been activated and the Solar Charger program and OCPP backend will also be running.

