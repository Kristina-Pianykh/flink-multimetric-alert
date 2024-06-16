import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;

public class MonitorInputRate extends RichMapFunction<Event, Tuple2<Double, Long>> {
  public transient Meter inputEventRateMeter;

  // public ConcurrentHashMap<String, Double> eventRates;

  // public MonitorInputRate(ConcurrentHashMap<String, Double> eventRates) {
  //   this.eventRates = eventRates;
  // }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    System.out.println("MonitorInputRate open");
    // this.eventRates = new HashMap<>();
    inputEventRateMeter =
        getRuntimeContext().getMetricGroup().meter("inputEventRate", new MeterView(10));
  }

  @Override
  public Tuple2<Double, Long> map(Event event) {
    if (event.getName().equals("pepe") || event.getName().equals("popo")) {
      inputEventRateMeter.markEvent();
      // System.out.println("Marked input event for inputEventRate" + event);
      System.out.println(inputEventRateMeter.getRate() + " input events per second\n");
      long timestamp = System.currentTimeMillis();
      return new Tuple2<>(inputEventRateMeter.getRate(), timestamp);
      // inputEventRateMeter.markEvent();
      // this.eventRates.put(event.getName(), inputEventRateMeter.getRate());
      // this.eventRates.put("hello", 420.69);
      // System.out.println("Count of input events marked: " + inputEventRateMeter.getCount());
      // System.out.println("Rate of input events marked: " + inputEventRateMeter.getRate());
    }
    return new Tuple2<>(0.0, 0L);
  }
}
