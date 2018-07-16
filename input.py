#!/usr/bin/env pythn3
#-*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from mqtt import MQTTUtils
from elasticsearch import Elasticsearch

import json
import sys
import time
import sys

def saveToEs(indexPath, doc):
	Elasticsearch().index(index = "laser", doc_type = doc["machine name"], body = doc )

def convert(x):
	saveTime = time.strftime('%Y%m%d%H', time.localtime())
	data = x.split('\n')

	programName = data[0].split(':', 1)[1]
	machineName = data[1].split(':', 1)[1]
	materialName = data[2].split(':', 1)[1]
	workCenter = data[3].split(':', 1)[1]
	orderOfProcessing = data[4].split(':', 1)[1]
	startTime = data[5].split(':', 1)[1]
	finishTime = data[6].split(':', 1)[1]
	processTime = data[7].split(':', 1)[1]
	status = data[8].split(':', 1)[1]
	schedule = data[9].split(':', 1)[1]
	saveingDateAndTime = data[10].split(':', 1)[1]
	processingDivision = data[11].split(':', 1)[1]	

	doc = {
		"program name":programName,
		"machine name":machineName,
		"material name":materialName,
		"work center":workCenter,
		"order of processing":orderOfProcessing,
		"start time":startTime,
		"finish time":finishTime,
		"process time":processTime,
		"status":status,
		"schedule":schedule,
		"saveing date or time":saveingDateAndTime,
		"processing division":processingDivision
	}

	jsonfd = open(saveTime + '.json', 'a')
	jsonfd.write(json.dumps(doc, indent=4))
	jsonfd.close()

	csvfd = open(saveTime + '.csv', 'a')
	csv.writer(csvfd).writerow([programName, machineName, workCenter, orderOfProcessing, startTime, finishTime, processTime, status,     schedule, saveingDateAndTime, processingDivision])
	csvfd.close()

	return doc
 
def load(x):

	programName = x[0].split(":")[1]
	machineName = x[1].split(":")[1]
	materialName = x[2].split(":")[1]
	workCenter = x[3].split(":")[1]
	orderOfProcessing = x[4].split(":")[1]
	numberOfSheets = x[5].split(":")[1]
	startTime = x[6] + ' '+ x[7]
	finishTime = x[8] + ' ' + x[9]
	processTime = x[10].split(":",1)[1]
	status = x[11].split(":")[1]
	schedule = x[12].split(":")[1]
	saveingDateAndTime = x[13] + ' ' + x[14]	
	processingDivision = x[15].split(":")[1]


	return "program name : " + programName +\
 			"\nmachine name : " + machineName +\
			"\nmaterial name : " + materialName +\
 			"\nwork center : " + workCenter +\
			"\norder of processing : " + orderOfProcessing +\
			"\nstart time : " + startTime.split(":",1)[1] +\
			"\nfinish time : " + finishTime.split(":",1)[1] +\
			"\nprocess time : " + processTime +\
			"\nstatus : " + status +\
			"\nschedule :" + schedule +\
			"\nsaveing date or time : " + saveingDateAndTime.split(":",1)[1] +\
			"\nprocessing division : " + processingDivision

def main():
	with SparkContext(appName='MQTTstreaming') as sc:
		ssc = StreamingContext(sc,2)
		
		broker = 'tcp://' + sys.argv[1]
		topic = sys.argv[2]

		dataInput = MQTTUtils.createStream(ssc, broker, topic)
		result = dataInput.map(lambda x: x.split(" ")).map(load).map(convert)
		result.foreachRDD(lambda x: foreach(saveToEs))
		result.pprint()

		ssc.start()
		ssc.awaitTermination()
		ssc.stop()

if __name__ == '__main__':
	main()
