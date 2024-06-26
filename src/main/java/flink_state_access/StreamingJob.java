import java.io.*;
import java.util.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamingJob {
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    int port = 6666;
    env.setParallelism(1);
    DataStream<Event> inputEventStream = env.addSource(new SocketSource(port), "Socket Source");
    DataStream<Event> timerStream = env.addSource(new DummySource(), "Dummy Source");

    // DataStream<Event> allInputEvents = inputEventStream.union(timerStream);

    // filter for the relevant input events (contributing to the match of a multi-sink query)
    ArrayList<DataStream<Tuple2<Double, Long>>> nonPartInputRates = new ArrayList<>();
    for (String s : new String[] {"popo", "kris", "nick"}) {
      DataStream<Tuple2<Double, Long>> nonPartInputRate =
          GenerateContinuousRates.generateContinuousRatesFromEvents(
              inputEventStream.filter(e -> e.getName().equals(s)).union(timerStream));
      nonPartInputRate.filter(
          new FilterFunction<Tuple2<Double, Long>>() {
            @Override
            public boolean filter(Tuple2<Double, Long> value) throws Exception {
              System.out.println("Non-partitioning input rate " + s + ": " + value.f0);
              return true;
            }
          });
      // nonPartInputRate.print();
      nonPartInputRates.add(nonPartInputRate);
    }

    DataStream<Tuple2<Double, Long>> partInputRates =
        GenerateContinuousRates.generateContinuousRatesFromEvents(
            inputEventStream.filter(e -> e.getName().equals("pepe")).union(timerStream));
    // partInputRates.print();

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

    DataStream<Event> matches =
        patternStream.flatSelect(
            new PatternFlatSelectFunction<Event, Event>() {
              @Override
              public void flatSelect(Map<String, List<Event>> patternMatches, Collector<Event> out)
                  throws Exception {
                System.out.println("Match found: " + patternMatches);
                out.collect(new Event("match", true));
                // out.collect("Match found: " + patternMatches);
              }
            });
    matches = matches.union(timerStream);

    DataStream<Tuple2<Double, Long>> matchRates =
        GenerateContinuousRates.generateContinuousRatesFromEvents(matches);
    // matchRates.print();

    // determine the tuple with the latest input rate (i.e. biggest timestamp)
    SingleOutputStreamOperator<Tuple2<Double, Long>> latestPartInputRates =
        partInputRates.keyBy(e -> "dummy").maxBy(1);
    latestPartInputRates.filter(
        new FilterFunction<Tuple2<Double, Long>>() {
          @Override
          public boolean filter(Tuple2<Double, Long> value) throws Exception {
            System.out.println("Latest partitioning input rate: " + value.f0);
            return true;
          }
        });
    // latestPartInputRates.print();
    SingleOutputStreamOperator<Tuple2<Double, Long>> latestMatchRates =
        matchRates.keyBy(e -> "dummy").maxBy(1);
    latestMatchRates.filter(
        new FilterFunction<Tuple2<Double, Long>>() {
          @Override
          public boolean filter(Tuple2<Double, Long> value) throws Exception {
            System.out.println("Latest match rate: " + value.f0);
            return true;
          }
        });
    // latestMatchRates.print();

    ArrayList<SingleOutputStreamOperator<Tuple2<Double, Long>>> latestNonPartInputRates =
        new ArrayList<>();
    for (DataStream<Tuple2<Double, Long>> stream : nonPartInputRates) {
      latestNonPartInputRates.add(stream.keyBy(e -> "dummy").maxBy(1));
    }

    // perform stateful comparison of the latest input rate and the latest match rate
    SingleOutputStreamOperator<Double> connectingStream = null;
    SingleOutputStreamOperator<Double> computeRatesStream =
        latestNonPartInputRates.get(0).map(e -> e.f0);

    for (int k = 1; k < (nonPartInputRates.size() + 2); k++) {

      if (k < nonPartInputRates.size()) {
        connectingStream = latestNonPartInputRates.get(k).map(e -> e.f0);
      } else if (k == nonPartInputRates.size()) {
        connectingStream = latestMatchRates.map(e -> e.f0);
      } else {
        connectingStream = latestPartInputRates.map(e -> e.f0);
      }
      // SingleOutputStreamOperator<Double> connectingStream = allRates.get(k).map(e -> e.f0);

      computeRatesStream =
          computeRatesStream
              .connect(connectingStream)
              .keyBy(
                  e -> "dummy",
                  e -> "dummy") // cast from ConnectedStreams to KeyedConnectedStreams, the key is
              // dummy
              .flatMap(new StatefulCoEvaluation(k, nonPartInputRates.size()));
    }

    env.execute("Flink CEP Example");
  }
}
