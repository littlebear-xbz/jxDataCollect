#coding: utf-8
import hdfs
from kafka import KafkaConsumer
from hdfs import InsecureClient
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import sys
import ConfigParser
reload(sys)
import json
sys.setdefaultencoding('utf-8')




def initKafkaConsumer(bootstrap_servers,group_id,topic):
    consumer = KafkaConsumer(topic,group_id=group_id,bootstrap_servers=bootstrap_servers)
    return consumer

def initHDFSclient(url_master , path):
    user = "hdfs"
    hdfs_client = InsecureClient(url_master,user=user)
    hdfs_client.makedirs(path)
    return hdfs_client


def process_dir(hdfs_client,pass_time,hdfs_path):
    pass_time = datetime.strptime(pass_time, "%Y-%m-%d %H:%M:%S.%f")
    ymd = pass_time.strftime('%Y%m%d')
    dir_path = hdfs_path + "%s"%ymd
    hdfs_client.makedirs(dir_path)
    hour = pass_time.strftime('%H')
    file_path = dir_path + "/" + hour + ".txt"
    print file_path
    return file_path

def saveToHDFS(hdfs_client,hdfs_path,kafka_consumer):
    for message in kafka_consumer:
        data = json.loads(message.value)
        file_path = process_dir(hdfs_client,data["passTime"],hdfs_path)
        line = ""
        line_list = []
        line_list.append(data["vechileID"])
        line_list.append(data["kakouID"])
        line_list.append(data["deviceID"])
        line_list.append(data["passTime"])
        line_list.append(data["roadNum"])
        line_list.append(data["plateNum"])
        line_list.append(data["plateColour"])
        line_list.append(data["backPlateNum"])
        line_list.append(data["backPlateColour"])
        line_list.append(data["plateMatch"])
        line_list.append(data["speed"])
        line_list.append(data["speedLimit"])
        line_list.append(data["vechileLength"])
        line_list.append(data["driveStatus"])
        line_list.append(data["vechileBrand"])
        line_list.append(data["vechileLook"])
        line_list.append(data["vechileColour"])
        line_list.append(data["vechileLogo"])
        line_list.append(data["vechileKind"])
        line_list.append(data["plateType"])
        line_list.append(data["plateRegion"])
        line_list.append(data["pictureNum"])
        line_list.append(data["jssj"])
        line_list.append(data["picturepath"][0])
        line_list.append(data["xxbh"])
        line_list.append(data["Belt"])
        line_list.append(data["Call"])
        line_list.append(data["CDLX"])
        line_list.append(data["WFZT"])
        line_list.append(data["score"])
        line_list.append(data["cltz"])
        line_list.append(data["rect"])
        line_list.append(data["tagnum"])
        line_list.append(data["boxnum"])
        line_list.append(data["caryear"])
        line_list.append(data["dropnum"])
        line_list.append(data["id"])
        line_list.append(data["rksj"])
        line_list.append(data["fxbh"])
        line_list.append(data["sbmc"])
        line_list.append(data["type"])
        line_list.append(data["clzl"])
        line_list.append(data["sunflag"])
        line_list.append(data["clpp"])
        line_list.append(data["carGNum"])
        line_list.append(data["carANum"])
        line_list.append(data["carBNum"])
        line_list.append(data["carCNum"])
        line_list.append(data["carDNum"])
        line_list.append(data["carENum"])
        line_list.append(data["carFNum"])
        line_list.append(data["take"])
        line_list.append(data["tx1"])
        line = "|".join(line_list)
        print line
        # with hdfs_client.write(hdfs_path=file_path, encoding='utf-8', append=True) as writer:
        #     writer.write(line+"\n")
        try:
            hdfs_client.write(hdfs_path=file_path,data=line+"\n",encoding="utf-8",append=True)
        except:
            hdfs_client.write(file_path, data=line+"\n", encoding='utf-8')
        # print message.value
        # json_data = json.loads(message.value)
        # print json_data['take']


def main():
    CF = ConfigParser.ConfigParser()
    CF.read('../conf/saveRecDataToHdfs.conf')
    hdfs_url_master = CF.get("HDFS","url_master")
    hdfs_path = CF.get("HDFS","path")
    hdfs_client = initHDFSclient(hdfs_url_master,hdfs_path)
    bootstrap_servers = CF.get("kafka", "bootstrap").split(",")
    kafka_topic = CF.get("kafka", "topic")
    kafka_group_id = CF.get("kafka", "group_id")
    kafka_consumer = initKafkaConsumer(topic=kafka_topic,group_id=kafka_group_id,bootstrap_servers=bootstrap_servers)
    saveToHDFS(hdfs_client=hdfs_client,hdfs_path=hdfs_path,kafka_consumer=kafka_consumer)

if __name__ == '__main__':
    main()