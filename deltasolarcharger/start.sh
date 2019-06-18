lxterminal --working-directory=/home/pi/deltasolarcharger/deltasolarcharger -e sudo python3 deltasolarchargerlauncher.py &
lxterminal --working-directory=/home/pi/deltasolarcharger/deltasolarcharger/ocppserver -e sudo python3 ocppserver.py
lxterminal --working-directory=/home/pi/deltasolarcharger/deltasolarcharger/3g_helper -e sudo python3 3g_helper.py