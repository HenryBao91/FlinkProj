package henry.flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import henry.flink.function.MyAggFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.OutputTag;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Author: Henry
 * @Description: 数据报表
 * @Date: Create in 2019/5/29 11:05
 **/
public class DataReport {

    public static Logger logger = LoggerFactory.getLogger(DataReport.class);

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //  设置使用eventtime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //checkpoint配置
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置statebackend
        //env.setStateBackend(new RocksDBStateBackend("hdfs://master:9000/flink/checkpoints",true));


        /*
        *   配置 kafkaSource
        * */
        String topic = "auditlog";
        Properties prop = new Properties();
        prop.setProperty("boostrap.servers", "master:9092");
        prop.setProperty("group.id", "con1");

        FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), prop);


       /*
       *    获取到kafka的数据
       *
       *    审核数据的格式：
       *   {"dt":"审核时间{年月日 时分秒}", "type":"审核类型","username":"审核人姓名","area":"大区"}
       * */
        DataStreamSource<String> data = env.addSource(myConsumer);

        /*
        *   对数据进行清洗
        * */
        DataStream<Tuple3<Long, String, String>> mapData = data.map(
                new MapFunction<String, Tuple3<Long, String, String>>() {

                @Override
                public Tuple3<Long, String, String> map(String value) throws Exception {

                    JSONObject jsonObject = JSON.parseObject("line");

                    String dt = jsonObject.getString("dt");
                    long time = 0;
                    try {
                        SimpleDateFormat sdf = new SimpleDateFormat("YYYY MM dd HH:mm:ss");
                        Date parse = sdf.parse(dt);
                        time = parse.getTime();
                    } catch (ParseException e) {
                        //  也可以把这个日志存储到其他介质中
                        logger.error("时间解析异常，dt: " + dt, e.getCause());
                    }

                    String type = jsonObject.getString("type");
                    String area = jsonObject.getString("area");

                    return new Tuple3<Long, String, String>(time, type, area);

                }
        });

        /*
        *   过滤掉异常数据
        * */
        DataStream<Tuple3<Long, String, String>> filterData = mapData.filter(new FilterFunction<Tuple3<Long, String, String>>() {
            @Override
            public boolean filter(Tuple3<Long, String, String> value) throws Exception {
                boolean flag = true;
                if (value.f0 == 0) {
                    flag = false;
                }
                return flag;
            }
        });

        //  保存迟到太久的数据
        OutputTag<Tuple3<Long, String, String>> outputTag = new OutputTag<Tuple3<Long, String, String>>("late-data"){};

        /*
         *  窗口统计操作
         *
         * */
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> resultData = filterData.assignTimestampsAndWatermarks(new henry.flink.function.watermark.MyWatermark())
                .keyBy(1, 2)   // 根据第1、2个字段，即type、area分组，第0个字段是timestamp
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))  //  每隔一分钟统计前一分钟的数据
                .allowedLateness(Time.seconds(30))  // 允许迟到30s
                .sideOutputLateData(outputTag)  // 记录迟到太久的数据
                .apply(new MyAggFunction());

        //  获取迟到太久的数据
        DataStream<Tuple3<Long, String, String>> sideOutput = resultData.getSideOutput(outputTag);

        //  存储迟到太久的数据到kafka中
        String outTopic = "lateLog";
        Properties outprop = new Properties();
        outprop.setProperty("bootstrap.servers ", "master:9092");
        //	设置事务超时时间
        prop.setProperty("transaction.timeout.ms", 60000*15+"");
        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<String>(
                outTopic,
                new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),
                outprop,
                FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);
        sideOutput.map(new MapFunction<Tuple3<Long, String, String>, String>() {

            @Override
            public String map(Tuple3<Long, String, String> value) throws Exception {
                return value.f0+"\t"+value.f1+"\t"+value.f2;
            }
        }).addSink(myProducer);

        /*
        *   把计算的结存储到ES中
        * */
        List<HttpHost> httpHost = new ArrayList<>();
        httpHost.add(new HttpHost("master", 9200, "http"));
        // use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<Tuple4<String, String, String, Long>> esSinkBuilder =
                new ElasticsearchSink.Builder<Tuple4<String, String, String, Long>>(
                httpHost,
                new ElasticsearchSinkFunction<Tuple4<String, String, String, Long>>() {
                    public IndexRequest createIndexRequest(Tuple4<String, String, String, Long> element) {
                        Map<String, Object> json = new HashMap<>();   // Map参数改成Object原因是：
                        json.put("time", element.f0);           // 下面json.put的数据有String和long不同类型
                        json.put("type", element.f1);
                        json.put("area", element.f2);
                        json.put("count", element.f3);

                        return Requests.indexRequest()
                                .index("auditindex")
                                .type("audittype")
                                .source(json);
                    }

                    @Override
                    public void process(Tuple4<String, String, String, Long> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

        //  设置批量写数据的缓冲区大小，测试可以为1，实际工作中看时间，一般需要调大
        // ES是有缓冲区的，这里设置1代表，每增加一条数据直接就刷新到ES
        esSinkBuilder.setBulkFlushMaxActions(1);
        resultData.addSink(esSinkBuilder.build());

        env.execute("DataReport");
    }

}
