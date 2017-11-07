# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 10:35:35 2017
@author: xiongz

kafka-console-consumer --bootstrap-server  jp-bigdata-07:9092 --topic ltest --from-beginning
kafka-console-producer --broker-list jp-bigdata-07:9092 --topic ltest
"""
import sys
from kafka import KafkaConsumer
import logging

reload(sys)
sys.setdefaultencoding('utf-8')
# To consume latest messages and auto-commit offsets
server_list = ['jp-bigdata-03:9092', 'jp-bigdata-04:9092', 'jp-bigdata-05:9092',
               'jp-bigdata-06:9092', 'jp-bigdata-07:9092', 'jp-bigdata-08:9092',
               'jp-bigdata-09:9092']

consumer = KafkaConsumer('ltest', group_id='groupltest',bootstrap_servers=server_list)

def listenTopic():
    count = 0
    for message in consumer:
        pass
