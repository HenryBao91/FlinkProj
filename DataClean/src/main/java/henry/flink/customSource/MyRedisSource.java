package henry.flink.customSource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.HashMap;
import java.util.Map;

// 自动补全变量名称 : Ctrl + Alt + v
// 自动补全属性名称 : Ctrl + Alt + f
// 按 Alt + Enter，选择 “Intruduce local variable”
/**
 * @Author: Henry
 * @Description: 自定义 Redis Source
 *              由于存储的是 国家大区和编码的映射关系
 *              类似于 k-v ，所以返回 HashMap 格式比较好
 *
 * 在 Redis 中保存的国家和大区的关系
 * Redis中进行数据的初始化，数据格式：
 *   Hash        大区    国家
 * hset areas   AREA_US   US
 * hset areas   AREA_CT   TW,HK
 * hset areas   AREA_AR   PK,SA,KW
 * hset areas   AREA_IN   IN
 *
 * 需要把大区和国家的对应关系组装成 java 的 hashmap
 *
 * @Date: Create in 2019/5/25 18:12
 **/

public class MyRedisSource implements SourceFunction<HashMap<String,String>>{
    private Logger logger = LoggerFactory.getLogger(MyRedisSource.class);

    private final long SLEEP_MILLION = 60000 ;

    private boolean isrunning = true;
    private Jedis jedis = null;

    public void run(SourceContext<HashMap<String, String>> ctx) throws Exception {

        this.jedis = new Jedis("master", 6379);
        //  存储所有国家和大区的对应关系
        HashMap<String, String> keyValueMap = new HashMap<String, String>();
        while (isrunning){
            try{
                //  每次执行前先清空，去除旧数据
                keyValueMap.clear();
                //  取出数据
                Map<String, String> areas = jedis.hgetAll("areas");
                //  进行迭代
                for (Map.Entry<String, String> entry : areas.entrySet()){
                    String key = entry.getKey();      //  大区：AREA_AR
                    String value = entry.getValue();  //  国家：PK,SA,KW
                    String[] splits = value.split(",");
                    for (String split : splits){
                        //  这里 split 相当于key， key 是 value
                        keyValueMap.put(split, key); // 即 PK，AREA_AR
                    }
                }
                //  防止取到空数据
                if(keyValueMap.size() > 0){
                    ctx.collect(keyValueMap);
                }
                else {
                    logger.warn("从Redis中获取到的数据为空！");
                }
                //  一分钟提取一次
                Thread.sleep(SLEEP_MILLION);
            }
            // 捕获 Jedis 链接异常
            catch (JedisConnectionException e){
                //  重新获取链接
                jedis = new Jedis("master", 6379);
                logger.error("Redis链接异常，重新获取链接", e.getCause());
            }// 捕获其他异常处理，通过日志记录
            catch (Exception e){
                logger.error("Source数据源异常", e.getCause());
            }
        }
    }

    /**
     *  任务停止，设置 false
     * */
    public void cancel() {
        isrunning = false;
        // 这样可以只获取一次连接在while一直用
        if(jedis != null){
            jedis.close();
        }
    }
}
