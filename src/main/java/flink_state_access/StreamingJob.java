import java.util.*;
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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class StreamingJob {
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    int port = 6666;
    env.setParallelism(1);
    DataStream<Event> inputEventStream = env.addSource(new SocketSource(port), "Socket Source");

    // filter for the relevant input events (contributing to the match of a multi-sink query)
    DataStream<Event> pepePopoStream =
        inputEventStream.filter(e -> (e.getName().equals("pepe") || e.getName().equals("popo")));

    // attach a rate monitor to the input stream of relevant events
    DataStream<Tuple2<Double, Long>> inputRatesStream = pepePopoStream.map(new MonitorInputRate());
    inputRatesStream.print();

    // Define a pattern: looking for a sequence of "pepe" -> "popo"
    Pattern<Event, ?> pattern =
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

    PatternStream<Event> patternStream = CEP.pattern(inputEventStream, pattern).inProcessingTime();

    DataStream<Tuple2<Double, Long>> result =
        patternStream
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
            .map(new MonitorMatchRate());
    // result2.print();

    // determine the tuple with the latest input rate (i.e. biggest timestamp)
    SingleOutputStreamOperator<Tuple2<Double, Long>> latestInputRate =
        inputRatesStream.keyBy(e -> "dummy").maxBy(1);
    latestInputRate.print();
    SingleOutputStreamOperator<Tuple2<Double, Long>> latestMatchRate =
        result.keyBy(e -> "dummy").maxBy(1);
    latestMatchRate.print();

    // perform stateful comparison of the latest input rate and the latest match rate
    latestInputRate
        .connect(latestMatchRate)
        .keyBy(
            e -> "dummy",
            e -> "dummy") // cast from ConnectedStreams to KeyedConnectedStreams, the key is dummy
        .flatMap(
            new RichCoFlatMapFunction<Tuple2<Double, Long>, Tuple2<Double, Long>, String>() {
              private transient ValueState<Double> latestInputRate;
              private transient ValueState<Double> latestMatchRate;

              @Override
              public void open(Configuration ignored) throws Exception {
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
                latestInputRate.update(value.f0);
                // System.out.println("Input rate1: " + value);
                out.collect("Input rate1: " + value);
              }

              // simulate the case when the result of comparison fires a trigger
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

    env.execute("Flink CEP Example");
  }
}
