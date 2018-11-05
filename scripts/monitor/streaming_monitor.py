#!/usr/bin/python
# _*_ encoding:utf-8 _*_

import os
import sys
import socket
import sendAlarm

alertPeople = "WuJianYang,ZhengShengJun"

pioneer_ips = ["application1", "application2", "application3", "application4"]
streaming_hive_ips = ["application5"]
schema_registry_ips = ["application1", "application2", "application3", "application4", "application5"]

streaming_pioneer_id = "com.tree.finance.bigdata.kafka.connect.sink.fs.StreamingPioneer"
streaming_hive_id = "com.tree.finance.bigdata.hive.streaming.StreamingWarehouse"
schema_registry_id = "io.confluent.kafka.schemaregistry.rest.SchemaRegistryMain"

streaming_pioneer_start_cmd = "/usr/local/streaming-avro/bin/start.sh"
streaming_hive_start_cmd = "/usr/local/streaming-hive/bin/start.sh"
schema_registry_start_cmd = "nohup /data0/confluent/bin/schema-registry-start /data0/confluent/etc/schema-registry/schema-registry.properties >/dev/null 2>&1  &"


def isOpen(ip, port):
    sc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sc.settimeout(1)
    try:
        sc.connect((ip, int(port)))
        return True
    except socket.error as e:
        return False


def get_host_ip():
    try:
        soc = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        soc.connect(('8.8.8.8', 80))
        ip = soc.getsockname()[0]
    finally:
        soc.close()

    return ip


def check_cron(op):
    cmd = """/usr/bin/crontab -l|grep streaming_monitor.py | grep %s""" % op
    p = os.popen(cmd)
    result = p.read().strip('\n')
    p.close()
    if not result:
        os.popen('echo "*/5 * * * * /home/python/streaming_monitor.py %s >/dev/null 2>&1" >> /var/spool/cron/root' % op)


def monitor_remote_process(ips, process_identity, start_cmd):
    """
    :param start_cmd: restart process command
    :type process_identity: process identity
    :type ips: remote ips
    """
    for ip in ips:
        cmd = """ssh root@%s ps ax|grep java|grep -i %s |grep -v grep|awk '{print $1}'""" % (ip, process_identity)
        p = os.popen(cmd)
        result = p.read().strip('\n')
        print(result)
        p.close()
        if not result:
            os.system("ssh root@%s %s" % (ip, start_cmd))
            content = process_identity + ': ' + ip + ' process is down.'
            sendAlarm.send('StreamingAlarm', content, alertPeople)


if __name__ == '__main__':
    action = sys.argv[1]
    if action == 'streaming_pioneer':
        check_cron(action)
        monitor_remote_process(pioneer_ips, streaming_pioneer_id, streaming_pioneer_start_cmd)
    elif action == 'streaming_hive':
        check_cron(action)
        monitor_remote_process(streaming_hive_ips, streaming_hive_id, streaming_hive_start_cmd)
    elif action == 'schema_registry':
        check_cron(action)
        monitor_remote_process(schema_registry_ips, schema_registry_id, schema_registry_start_cmd)
    else:
        print(action)
        print("illegal argument, support: %s %s" % ("streaming_pioneer", "streaming_hive"))


