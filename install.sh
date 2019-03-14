#!/bin/bash

if [ "$EUID" -ne 0 ]
	then echo "Must be root"
	exit
fi

if [[ $# -lt 1 ]]; 
	then echo "You need to pass a password!"
	echo "Usage:"
	echo "sudo $0 yourChosenPassword [apName]"
	exit
fi

apt-get update
apt-get install hostapd isc-dhcp-server iptables-persistent -y

sed -i 's/^option domain-name/#option domain-name/' /etc/dhcp/dhcpd.conf
sed -i 's/#authoritative/authoritative/' /etc/dhcp/dhcpd.conf

cat >> /etc/dhcp/dhcpd.conf <<EOF
subnet 192.168.10.0 netmask 255.255.255.0 {
	range 192.168.10.10 192.168.10.150;
	option broadcast-address 192.168.10.255;
	option routers 192.168.10.1;
	default-lease-time 600;
	max-lease-time 7200;
	option domain-name "local";
	option domain-name-servers 8.8.8.8, 8.8.4.4;
}
EOF

sed -i 's/INTERFACESv4=""/INTERFACESv4="wlan0"/' /etc/default/isc-dhcp-server

cat >> /etc/network/interfaces <<EOF
allow-hotplug wlan0
iface wlan0 inet static
	address 192.168.10.1
	netmask 255.255.255.0
EOF

ifconfig wlan0 192.168.10.1

cat > /etc/hostapd/hostapd.conf <<EOF
interface=wlan0
driver=nl80211
ssid=$1
country_code=AU
hw_mode=g
channel=10
ieee80211d=1
wmm_enabled=1
auth_algs=1
wpa=2
wpa_passphrase=$2
wpa_key_mgmt=WPA-PSK
wpa_pairwise=CCMP
ieee80211n=1
EOF

sed -i -- 's/#DAEMON_CONF=""/DAEMON_CONF="\/etc\/hostapd\/hostapd.conf"/g' /etc/default/hostapd
sed -i -- 's/DAEMON_CONF=/DAEMON_CONF=\/etc\/hostapd\/hostapd.conf/g' /etc/init.d/hostapd

cat >> /etc/sysctl.conf << EOF
net.ipv4.ip_forward=1
EOF

echo 1 > /proc/sys/net/ipv4/ip_forward

update-rc.d hostapd enable
update-rc.d isc-dhcp-server enable

sudo service hostapd start
sudo service isc-dhcp-server-start

apt-get install network-manager network-manager-gnome -y
nmcli c add con-name "mycon" type gsm ifname "*" apn "live.vodafone.com"
nmcli c mod mycon connection.autoconnect yes

reboot
