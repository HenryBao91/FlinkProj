package henry.flink.function.watermark;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @Author: HongZhen
 * @Description: 自定义Watermark
 * @Date: Create in 2019/5/29 11:53
 **/
public class MyWatermark implements AssignerWithPeriodicWatermarks<Tuple3<Long, String, String>> {

    Long currentMaxTimestamp = 0L;
    final Long maxOutOfOrderness = 10000L;// 最大允许的乱序时间是10s，具体时间需要根据实际测

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp-maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Tuple3<Long, String, String> element, long previousElementTimestamp) {
        Long timestamp = element.f0;
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }
}
