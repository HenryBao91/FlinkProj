package henry.flink.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * @Author: HongZhen
 * @Description: 创建kafka topic的命令
 *
 * bin/kafka-topics.sh  --create --topic auditLog --zookeeper localhost:2181 --partitions 5 --replication-factor 1
 *
 * @Date: Create in 2019/5/29 11:05
 **/

public class kafkaProducerDataReport {

    public static void main(String[] args) throws Exception{
        Properties prop = new Properties();
        //指定kafka broker地址
        prop.put("bootstrap.servers", "master:9092");
        //指定key value的序列化方式
        prop.put("key.serializer", StringSerializer.class.getName());
        prop.put("value.serializer", StringSerializer.class.getName());
        //指定topic名称
        String topic = "allData";

        //创建producer链接
        KafkaProducer<String, String> producer = new KafkaProducer<String,String>(prop);

        //{"dt":"2018-01-01 10:11:22","type":"shelf","username":"shenhe1","area":"AREA_US"}

        //生产消息
        while(true){
            String message = "{\"dt\":\""+getCurrentTime()+"\",\"type\":\""+getRandomType()+"\",\"username\":\""+getRandomUsername()+"\",\"area\":\""+getRandomArea()+"\"}";
            System.out.println(message);
            producer.send(new ProducerRecord<String, String>(topic,message));
            Thread.sleep(1000);
        }
        //关闭链接
        //producer.close();
    }

    public static String getCurrentTime(){
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        return sdf.format(new Date());
    }

    public static String getRandomArea(){
        String[] types = {"AREA_US","AREA_CT","AREA_AR","AREA_IN","AREA_ID"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }


    public static String getRandomType(){
        String[] types = {"shelf","unshelf","black","chlid_shelf","child_unshelf"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }


    public static String getRandomUsername(){
        String[] types = {"shenhe1","shenhe2","shenhe3","shenhe4","shenhe5"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }


}
