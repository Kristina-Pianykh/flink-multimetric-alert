import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple2;

// import org.apache.flink.streaming.api.watermark.Watermark;

public class CustomWatermarkStrategy implements WatermarkStrategy<Tuple2<String, Integer>> {

  @Override
  public WatermarkGenerator<Tuple2<String, Integer>> createWatermarkGenerator(
      WatermarkGeneratorSupplier.Context context) {
    return new CustomWatermarkGenerator();
  }

  @Override
  public TimestampAssigner<Tuple2<String, Integer>> createTimestampAssigner(
      TimestampAssignerSupplier.Context context) {
    return new CustomTimestampAssigner(); // extract timestamp from the event
  }

  public static class CustomTimestampAssigner
      implements TimestampAssigner<Tuple2<String, Integer>> {
    @Override
    public long extractTimestamp(Tuple2<String, Integer> element, long recordTimestamp) {
      return System.currentTimeMillis();
    }
  }

  public static class CustomWatermarkGenerator
      implements WatermarkGenerator<Tuple2<String, Integer>> {
    @Override
    public void onEvent(
        Tuple2<String, Integer> event, long eventTimestamp, WatermarkOutput output) {
      long currentSystemTime = System.currentTimeMillis();
      System.out.println(
          "Emitting watermark for event: " + event.f0 + " with system time: " + currentSystemTime);
      output.emitWatermark(
          new Watermark(currentSystemTime)); // Emit the current system time as watermark
      // long timestamp_us = LocalTime.now().toNanoOfDay() / 1000L;
      // long watermark_ = System.currentTimeMillis();
      // System.out.println(
      //     "Emitting on event watermark: " + event.f0 + " " + event.f1 + " " + watermark_);
      // output.emitWatermark(new Watermark(watermark_));
      // output.emitWatermark(new Watermark(timestamp_us));
      //      output.emitWatermark(new Watermark(eventTimestamp));
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {}
  }
}
