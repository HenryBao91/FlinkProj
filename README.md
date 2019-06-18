FlinkProj 案例开发
=============
**应用场景：**
- 数据清洗【实时ETL】
- 数据报表
      
# 1、数据清洗【实时ETL】
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190525170515165.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2hvbmd6aGVuOTE=,size_14,color_FFFFFF,t_70)      

## 1.1、需求分析
 针对算法产生的日志数据进行清洗拆分
1.  算法产生的日志数据是嵌套大JSON格式（json嵌套json），需要拆分打平
2.  针对算法中的国家字段进行大区转换
3.  最后把不同类型的日志数据分别进行存储

## 1.2、数据清理 DataClean 结构
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190618162959634.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2hvbmd6aGVuOTE=,size_16,color_FFFFFF,t_70)

## 1.3、实践运行
### 1.3.1、Redis
**启动redis：**

1. 先从一个终端启动redis服务
```shell
./redis-server
```
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190614150542868.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2hvbmd6aGVuOTE=,size_16,color_FFFFFF,t_70)
&#8195;	2. 先从一个终端启动redis客户端，并插入数据
```shell
./redis-cli
127.0.0.1:6379> hset areas   AREA_US   US
(integer) 1
127.0.0.1:6379> hset areas   AREA_CT   TW,HK
(integer) 1
127.0.0.1:6379> hset areas   AREA_AR   PK,SA,KW
(integer) 1
127.0.0.1:6379> hset areas   AREA_IN   IN
(integer) 1
127.0.0.1:6379>
```
hgetall查看插入数据情况：

<center><img src="https://img-blog.csdnimg.cn/20190614150949735.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2hvbmd6aGVuOTE=,size_16,color_FFFFFF,t_70">

### 1.3.2、Kafka
**启动kafka：**
```shell
./kafka-server-start.sh -daemon ../config/server.properties
```


jps查看启动进程：

<center><img src="https://img-blog.csdnimg.cn/20190614145056974.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2hvbmd6aGVuOTE=,size_16,color_FFFFFF,t_70">


**kafka创建topc：**
```shell
./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 5 --topic allData
```
创建topic成功：

<center><img src="https://img-blog.csdnimg.cn/2019061414513644.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2hvbmd6aGVuOTE=,size_16,color_FFFFFF,t_70">

**监控kafka topic：**
```shell
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic allDataClean
```
<center><img src="https://img-blog.csdnimg.cn/20190614155034309.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2hvbmd6aGVuOTE=,size_16,color_ffffff,t_70">

### 1.3.3、启动程序
先启动 DataClean 程序，再启动生产者程序，**kafka生产者产生数据如下：**
<center><img src="https://img-blog.csdnimg.cn/20190614153556845.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2hvbmd6aGVuOTE=,size_16,color_B00BBB,t_70">

**最后终端观察处理输出的数据：**
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190614161359765.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2hvbmd6aGVuOTE=,size_16,color_FBB00F,t_70)
**只有部分数据正确处理输出的原因是**：代码中没有设置并行度，默认是按机器CPU核数跑的，所以有的线程 allMap 没有数据，有的有数据，所以会导致部分正确，这是**需要通过 broadcast() 进行广播**，让所有线程都接收到数据：


**运行结果**：
<center><img src="https://img-blog.csdnimg.cn/20190614165818581.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2hvbmd6aGVuOTE=,size_16,color_FFFFFF,t_70">

控制台打印结果：
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190614165950201.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2hvbmd6aGVuOTE=,size_16,color_FFFFFF,t_70)

## 3.4、Flink yarn集群启动
**向yarn提交任务：**
```shell
./bin/flink run -m yarn-cluster -yn 2 -yjm 1024 -ytm 1024 -c henry.flink.DataClean /root/flinkCode/DataClean-1.0-SNAPSHOT-jar-with-dependencies.jar
```
任务成功运行启动：
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190617150540780.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2hvbmd6aGVuOTE=,size_16,color_FFFFFF,t_70)
**通过 yarn UI 查看任务，并进入Flink job：**
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190617151608947.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2hvbmd6aGVuOTE=,size_19,color_F00AFF,t_70)
程序中设置的并行度：
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190617151733631.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2hvbmd6aGVuOTE=,size_16,color_F00AFF,t_70)
**启动kafka生产者：**
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190617152318346.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2hvbmd6aGVuOTE=,size_16,color_FFFFFF,t_70)
监控topic消费情况：
```shell
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic allDataClean
```
**最终终端输出结果，** 同IDEA中运行结果：
<center><img src="https://img-blog.csdnimg.cn/2019061715251942.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2hvbmd6aGVuOTE=,size_16,color_FFFFFF,t_70">