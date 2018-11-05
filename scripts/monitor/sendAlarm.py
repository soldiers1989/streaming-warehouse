#!/usr/bin/python
# _*_ encoding:utf-8 _*_

import os
import sys
import socket

HOST = ['10.1.2.198', '10.1.2.199']


def isOpen(ip):
    sc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sc.settimeout(2)
    try:
        sc.connect((ip, int(8091)))
        return True
    except socket.error as e:
        return False


def getHost():
    for ip in HOST:
        if isOpen(ip):
            return ip


def send(alarm_name, msg, weChatName):
    IP = getHost()
    NamesList = weChatName.split(',')
    weChatStr = str(NamesList).replace('\'', '\"')
    HTTP = "http://" + IP + ":8091/notify/operation/warning"
    cmd = """curl -H 'Content-Type: application/json' -d '{"weChatList":%s, "subject":"%s", "content":"%s"}' %s""" % (weChatStr, alarm_name,  msg, HTTP)
    # print(cmd)
    os.system(cmd)


if __name__ == '__main__':
    alarm_name = sys.argv[1]
    msg = sys.argv[2]
    send_people = sys.argv[3]
    send(alarm_name, msg, send_people)