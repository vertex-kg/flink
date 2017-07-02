package streamtobatch;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class CustomTimeAssigner implements AssignerWithPeriodicWatermarks {

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(System.currentTimeMillis());
    }

    @Override
    public long extractTimestamp(Object element, long previousElementTimestamp) {
        return System.currentTimeMillis();
    }
}
