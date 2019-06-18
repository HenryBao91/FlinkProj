package bao.flink.customSource

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.collection.mutable


class MyRedisSourceS extends SourceFunction[mutable.Map[String, String]] {

  val logger= LoggerFactory.getLogger(classOf[MyRedisSourceS])

  val SLEEP_MILLION: Long = 60000

  var isrunning = true
  var jedis:Jedis = _

  override def run(ctx: SourceFunction.SourceContext[mutable.Map[String, String]]): Unit = {
    this.jedis = new Jedis("master", 6379)
    // 隐式转换
    import scala.collection.JavaConversions.mapAsScalaMap

    //  存储所有国家和大区的对应关系
    var keyValueMap= mutable.Map[String, String]()
    while (isrunning) {
      try {
        //  每次执行前先清空，去除旧数据
        keyValueMap.clear()
        //  取出数据
        keyValueMap = jedis.hgetAll("areas")
        //  进行迭代

        for ( key <- keyValueMap.keys.toList) {
          val value = keyValueMap.get(key).get //  大区：AREA_AR
          val splits = value.split(",")
          for (split <- splits) { //  这里 split 相当于key， key 是 value
            keyValueMap += (key -> split)
          }
        }
        //  防止取到空数据
        if (keyValueMap.nonEmpty)
          ctx.collect(keyValueMap)
        else
          logger.warn("从Redis中获取到的数据为空！")

        //  一分钟提取一次
        Thread.sleep(SLEEP_MILLION)
      }catch {  // 捕获其他异常处理，通过日志记录
        case e: JedisConnectionException => {
          logger.error("Redis链接异常，重新获取链接", e.getCause)
          //  重新获取链接
          jedis = new Jedis("master", 6379)
        }
        case e: Exception =>{
          logger.error("Source数据源异常", e.getCause)
        }
      }
    }
  }

  /**
    * 任务停止，设置 false
    **/
  override def cancel() = {
    isrunning = false
    // 这样可以只获取一次连接在while一直用
    if (jedis != null)
      jedis.close()
  }
}
