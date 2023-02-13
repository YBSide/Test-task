import os

def get_external_ip():
    ip = os.popen('curl -s ifconfig.me').readline()
    return ip