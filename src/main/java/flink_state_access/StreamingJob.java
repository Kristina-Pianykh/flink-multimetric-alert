import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class StreamingJob {
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    int port = 6666;
    ConcurrentHashMap<String, Double> inputRates = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, Double> matchRates = new ConcurrentHashMap<>();

    env.setParallelism(1);
    DataStream<Event> inputEventStream = env.addSource(new SocketSource(port), "Socket Source");
    KeyedStream<Event, String> monitoredInputSteam = inputEventStream.keyBy(e -> "inputs");

    MonitorInputRate inputRateMonitor = new MonitorInputRate();
    DataStream<Tuple2<Double, Long>> inputRatesStream = inputEventStream.map(inputRateMonitor);
    inputRatesStream.print();

    // trigger on control events
    // DataStream<Tuple2<Event, Double>> controlStream =
    //     monitoredInputSteam.filter(
    //         new FilterFunction<Tuple2<Event, Double>>() {
    //           @Override
    //           public boolean filter(Tuple2<Event, Double> item) throws Exception {
    //             Event event = item.f0;
    //             if (event.getName().equals("control")) {
    //               System.out.println("Control event: " + event);
    //               return true;
    //             }
    //             return false;
    //           }
    //         });

    // Define a pattern: looking for a sequence of "pepe" -> "popo"
    Pattern<Event, ?> pattern2 =
        Pattern.<Event>begin("start")
            .where(
                new SimpleCondition<Event>() {
                  @Override
                  public boolean filter(Event event) {
                    return event.getName().equals("pepe") && event.isPattern2Enabled();
                  }
                })
            .followedByAny("end")
            .where(
                new SimpleCondition<Event>() {
                  @Override
                  public boolean filter(Event event) {
                    return event.getName().equals("popo") && event.isPattern2Enabled();
                  }
                });

    // Apply pattern to input stream
    // PatternStream<Event> patternStream1 =
    //     CEP.pattern(monitoredInputSteam, pattern1).inProcessingTime();

    PatternStream<Event> patternStream2 =
        CEP.pattern(inputEventStream, pattern2).inProcessingTime();

    MonitorMatchRate matchRateMonitor = new MonitorMatchRate();
    DataStream<Tuple2<Double, Long>> result2 =
        patternStream2
            .flatSelect(
                new PatternFlatSelectFunction<Event, String>() {
                  @Override
                  public void flatSelect(
                      Map<String, List<Event>> patternMatches, Collector<String> out)
                      throws Exception {
                    System.out.println("Match found: " + patternMatches);
                    out.collect("Match found: " + patternMatches);
                  }
                })
            .map(matchRateMonitor);
    // result2.print();

    SingleOutputStreamOperator<Tuple2<Double, Long>> latestInputRate =
        inputRatesStream.keyBy(e -> "dummy").maxBy(1);
    // latestInputRate.print();
    SingleOutputStreamOperator<Tuple2<Double, Long>> latestMatchRate =
        result2.keyBy(e -> "dummy").maxBy(1);
    // latestMatchRate.print();

    latestInputRate
        .connect(latestMatchRate)
        .keyBy(e -> "dummy", e -> "dummy")
        .flatMap(
            new RichCoFlatMapFunction<Tuple2<Double, Long>, Tuple2<Double, Long>, String>() {
              private transient ValueState<Double> latestInputRate;
              private transient ValueState<Double> latestMatchRate;

              @Override
              public void open(Configuration parameters) throws Exception {
                latestInputRate =
                    getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("latestInputRate", Double.class, 0.0));
                latestMatchRate =
                    getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("latestMatchRate", Double.class, 0.0));
              }

              @Override
              public void flatMap1(Tuple2<Double, Long> value, Collector<String> out)
                  throws Exception {
                // Double currentRate = latestInputRate == null ? latestInputRate.value();
                // currentRate = value.f0;
                latestInputRate.update(value.f0);
                // this.latestInputRate = value.f0;
                // System.out.println("Input rate1: " + value);
                out.collect("Input rate1: " + value);
                // latestInputRate = value;
                // compareAndOutput(out);
              }

              @Override
              public void flatMap2(Tuple2<Double, Long> value, Collector<String> out)
                  throws Exception {
                latestMatchRate.update(value.f0);
                if (latestInputRate.value() <= latestMatchRate.value()) {
                  out.collect(
                      "Match rate is higher than input rate: "
                          + latestMatchRate.value()
                          + " > "
                          + latestInputRate.value());
                  System.out.println("Trigger switch");
                }
                out.collect("Input rate2: " + value);
              }
            })
        .print();

    // SingleOutputStreamOperator<String> comparisonResult =
    //     latestInputRate
    //         .connect(latestMatchRate)
    //         .keyBy(e -> "dummy", e -> "dummy")
    //         .process(new CompareRatesProcessFunction());
    // comparisonResult.print();

    // DataStream<String> result3 = result2.map(new CompareRates(inputRates, matchRates));

    // DataStream<String> result3 =
    //     result2.map(
    //         new RichMapFunction<String, String>(
    //             ConcurrentHashMap <String, Double> inputRates,
    //             ConcurrentHashMap <String, Double> matchRates
    //   ) {
    //
    //           @Override
    //           public String map(String val) throws Exception {
    //             System.out.println("Input Rates: " + inputRates);
    //             System.out.println("Match Rates: " + matchRates);
    //             if (inputRates != null) {
    //               System.out.println(inputRates.values());
    //             }
    //             if (matchRates != null) {
    //               System.out.println(matchRates.values());
    //             }
    //             return val;
    //           }
    //         });
    // for (String event : inputRateMonitor.eventRates.keySet()) {
    //   System.out.println("Input rate for " + event + ": " +
    // inputRateMonitor.eventRates.get(event));
    // }
    // System.out.println("Match rate: " + matchRateMonitor.eventRates.get("pepe_popo"));

    // metricListener.getMeter("inputEventRate").getRate();
    // System.out.println("Input rate: " + metricListener.getMeter("inputEventRate").getRate());
    // System.out.println("Match rate: " + metricListener.getMeter("matchRate").getRate());
    // System.out.println("Match rate: " + matchRateMonitor.matchRateMeter.getRate());
    // System.out.println("Input rate: " + inputRateMonitor.inputEventRateMeter.getRate());

    // result2.map(
    //     new CompareRates(inputRateMonitor.inputEventRateMeter, matchRateMonitor.matchRateMeter));

    // result2.print();

    env.execute("Flink CEP Example");
  }

  public static class CompareRatesProcessFunction
      extends CoProcessFunction<Tuple2<Double, Long>, Tuple2<Double, Long>, String> {
    private Tuple2<Double, Long> latestInputRate;
    private Tuple2<Double, Long> latestMatchRate;

    @Override
    public void processElement1(Tuple2<Double, Long> value, Context ctx, Collector<String> out)
        throws Exception {
      latestInputRate = value;
      compareAndOutput(out);
    }

    @Override
    public void processElement2(Tuple2<Double, Long> value, Context ctx, Collector<String> out)
        throws Exception {
      latestMatchRate = value;
      compareAndOutput(out);
    }

    private void compareAndOutput(Collector<String> out) {
      if (latestInputRate != null && latestMatchRate != null) {
        if (latestInputRate.f0 <= latestMatchRate.f0) {
          out.collect(
              "Match rate is higher than input rate: "
                  + latestMatchRate.f0
                  + " > "
                  + latestInputRate.f0);
          System.out.println("Trigger switch");
        }
        // if (latestInputRate.f0.equals(latestMatchRate.f0)) {
        //   out.collect("Values are equal: " + latestInputRate.f0);
        // } else {
        //   out.collect(
        //       "Values are different. Input Rate: "
        //           + latestInputRate.f0
        //           + ", Match Rate: "
        //           + latestMatchRate.f0);
        // }
      }
    }
  }
}
