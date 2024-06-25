import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;

public class MonitorInputRate extends RichMapFunction<Event, Tuple2<Double, Long>> {
  public transient Meter inputEventRateMeter;
  ContinuousProcessingTimeTrigger trigger;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    // System.out.println("MonitorInputRate open");
    inputEventRateMeter =
        getRuntimeContext().getMetricGroup().meter("inputEventRate", new MeterView(10));
  }

  @Override
  public Tuple2<Double, Long> map(Event event) {
    inputEventRateMeter.markEvent();
    System.out.println(
        event.getName() + ": " + inputEventRateMeter.getRate() + " input events per second\n");
    long timestamp = System.currentTimeMillis();
    return new Tuple2<>(inputEventRateMeter.getRate(), timestamp);
  }
}
