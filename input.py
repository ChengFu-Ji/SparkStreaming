#!/usr/bin/env pythn3
#-*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from mqtt import MQTTUtils
from elasticsearch import Elasticsearch
import json
import sys

def saveToEs(indexPath, doc):
	Elasticsearch().index(index = indexPath, doc_type = doc["machine name"], body = doc )
	
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

	doc = {
		"program name":programName,
 		"machine name":machineName,
		"material name":materialName,
 		"work center":workCenter,
		"order of processing":orderOfProcessing,
		"start time":startTime.split(":",1)[1],
		"finish time":finishTime.split(":",1)[1],
		"process time":processTime,
		"status":status,
		"schedule":schedule,
		"saveing date or time":saveingDateAndTime.split(":",1)[1],
		"processing division":processingDivision
	}

	saveToEs("laser", doc)

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
		ssc = StreamingContext(sc,5)
		
		broker = 'tcp://' + sys.argv[1]
		topic = sys.argv[2]

		dataInput = MQTTUtils.createStream(ssc, broker, topic)
		dataInput.map(lambda x: x.split(" ")).map(load).pprint()

		ssc.start()
		ssc.awaitTermination()
		ssc.stop()

if __name__ == '__main__':
	main()
