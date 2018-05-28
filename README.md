# SparkStreaming
使用 pyspark 當中的 streaming API 來承接 MQTT 的資料，並且儲存到 elasticsearch and json。

叢集環境如下：

|host name|IP address |OS                |
|---------|---------- |------------------|
|master   |192.168.0.1|Linux Ubuntu 16.04|
|slave1   |192.168.0.2|Linux Ubuntu 16.04|
|slave2   |192.168.0.3|Linux Ubuntu 16.04|

Hadoop 為 2.7 版，Spark 為 2.0.0 版

```shell
$ ./publisher <CSV_file>
```

```shell
$ ./subscriber <pythonFile>
```
