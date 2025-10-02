#!/usr/bin/env python3

import threading
import socket
import json
import uuid

SO_NON_ZERO_VALUE = 1
BROADCAST_PORT = 50000


def heartbeat_sender():
    heartbeat_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    heartbeat_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, SO_NON_ZERO_VALUE)
    

def heartbeat_receiver():
    socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    pass

def job_sender():
    pass

def job_receiver():
    pass

def main():
    pass

if __name__ == "__main__":
    main()