import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;

public class MonitorMatchRate extends RichMapFunction<String, Tuple2<Double, Long>> {
  public transient Meter matchRateMeter;

  // public ConcurrentHashMap<String, Double> eventRates;

  // public MonitorMatchRate(ConcurrentHashMap<String, Double> eventRates) {
  //   this.eventRates = eventRates;
  // }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    // this.eventRates = new HashMap<>();
    System.out.println("MonitorMatchRate initialized");
    matchRateMeter = getRuntimeContext().getMetricGroup().meter("matchRate", new MeterView(10));
  }

  @Override
  public Tuple2<Double, Long> map(String event) {
    matchRateMeter.markEvent();
    // this.eventRates.put("pepe_popo", matchRateMeter.getRate());
    // this.eventRates.put("hello", 420.69);
    // System.out.println(this.eventRates);
    System.out.println(matchRateMeter.getRate() + " match events per second\n");
    long timestamp = System.currentTimeMillis();
    return new Tuple2<>(matchRateMeter.getRate(), timestamp);
    // System.out.println("Count of match events marked: " + matchRateMeter.getCount());
    // System.out.println("Rate of match events marked: " + matchRateMeter.getRate());
  }
}
