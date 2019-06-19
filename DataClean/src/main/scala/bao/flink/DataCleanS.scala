package bao.flink

import java.util.{HashMap, Properties}

import bao.flink.customSource.MyRedisSourceS
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSource}
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * @Author: Henry
  * @Description: ${description}
  * @Date: Create in 2019/6/17 15:36
  **/
object DataCleanS {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 修改并行度
    env.setParallelism(5)

    //checkpoint配置
    env.enableCheckpointing(60000) // 设置 1分钟=60秒
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(30000)
    env.getCheckpointConfig.setCheckpointTimeout(10000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //设置statebackend
    env.setStateBackend(new RocksDBStateBackend(
      "hdfs://master:9000/flink/checkpoints", true))

    import org.apache.flink.api.scala._
    //  指定 Kafka Source
    val topic = "allData"
    val prop = new Properties
    prop.setProperty("bootstrap.servers", "master:9092")
    prop.setProperty("group.id", "consumer2")
    val myConsumer = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema, prop)

    //  获取 Kafka 中的数据，Kakfa 数据格式如下：
    //  {"dt":"2019-01-01 11:11:11", "countryCode":"US","data":[{"type":"s1","score":0.3},{"type":"s1","score":0.3}]}
    val data= env.addSource(myConsumer) // 并行度根据 kafka topic partition数设定

    //  mapData 中存储最新的国家码和大区的映射关系
    val mapData = env.addSource(new MyRedisSourceS)
      .broadcast    //  可以把数据发送到后面算子的所有并行实际例中进行计算，否则处理数据丢失数据

    //  通过 connect 方法将两个数据流连接在一起,然后再flatMap
    val resData = data.connect(mapData).flatMap(
      new CoFlatMapFunction[String, mutable.Map[String,String],String] {

        //  存储国家和大区的映射关系
        var allMap = mutable.Map[String, String]()

        override def flatMap1(value: String, out: Collector[String]): Unit = {
          //  原数据是 Json 格式
          val jsonObject = JSON.parseObject(value)
          val dt: String = jsonObject.getString("dt")
          val countryCode: String = jsonObject.getString("countryCode")
          //  获取大区
          val area = allMap.get(countryCode)

          //  迭代取数据，jsonArray每个数据都是一个jsonobject
          val jsonArray = jsonObject.getJSONArray("data")
          for(i <- 0 to jsonArray.size()-1){
            val jsonObject1: JSONObject = jsonArray.getJSONObject(i)
            jsonObject1.put("area", area)
            jsonObject1.put("dt", dt)
            out.collect(jsonObject1.toJSONString)
          }
        }

        override def flatMap2(value: mutable.Map[String, String], out: Collector[String]): Unit = {
          this.allMap = value
        }
      })

    val outTopic: String = "allDataClean"
    val outprop: Properties = new Properties()
    outprop.setProperty("bootstrap.servers", "master:9092")
    //第一种解决方案，设置FlinkKafkaProducer011里面的事务超时时间
    //设置事务超时时间
    outprop.setProperty("transaction.timeout.ms", 60000 * 15 + "")
    //第二种解决方案，设置kafka的最大事务超时时间

    val myProducer = new FlinkKafkaProducer011[String](
      outTopic,
      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema),
      outprop,
      FlinkKafkaProducer011.Semantic.EXACTLY_ONCE)

    resData.addSink(myProducer)

    env.execute("Data Clean Scala")
  }
}
