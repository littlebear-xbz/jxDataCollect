# -*- coding: utf-8 -*-
"""

@author: xiongz
"""
from hdfs import Client
from hdfs import InsecureClient
import threading
import time

client = InsecureClient('http://jp-bigdata-03:50070', user='hdfs')

def writeToHdfs():
    while True:
        try:
            with client.write(hdfs_path="/user/xiongz/a.txt2", encoding='utf-8',append=True) as writer:
                writer.write("I am C\n")
        except:
            client.write(hdfs_path="/user/xiongz/a.txt2",data="I am A" ,encoding='utf-8')

writeToHdfs()