import java.io.*;
import java.util.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class StreamingJob {
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    int port = 6666;
    env.setParallelism(1);
    DataStream<Tuple2<String, Integer>> inputStream =
        env.addSource(new SocketSource(port), "Socket Source")
            .assignTimestampsAndWatermarks(new CustomWatermarkStrategy());

    // For demonstration, print the output
    // inputStream.print();

    ArrayList<Tuple2<String, String>> patternList = new ArrayList<>();
    patternList.add(new Tuple2<>("pepe", "popo"));
    patternList.add(new Tuple2<>("pepepopo", "kris"));
    ArrayList<DataStream<Tuple2<String, Integer>>> matchStreams = new ArrayList<>();
    for (int i = 0; i < patternList.size(); i++) {
      int prevIdx = i - 1;

      if (prevIdx >= 0) {
        System.out.println(patternList.get(i) + " uses " + patternList.get(prevIdx) + " as input");
        DataStream<Tuple2<String, Integer>> unionStream =
            inputStream.union(
                matchStreams
                    .get(prevIdx)
                    .assignTimestampsAndWatermarks(new CustomWatermarkStrategy()));
        matchStreams.add(
            generateMatchStream(unionStream, patternList.get(i).f0, patternList.get(i).f1));
      }

      matchStreams.add(
          generateMatchStream(inputStream, patternList.get(i).f0, patternList.get(i).f1));
    }

    if (matchStreams.isEmpty()) System.out.println("No match streams yet");
    else {
      DataStream<Tuple2<String, Integer>> unionStream =
          matchStreams.stream()
              .reduce(DataStream<Tuple2<String, Integer>>::union)
              .get()
              .union(inputStream);
      unionStream
          .keyBy(
              new KeySelector<Tuple2<String, Integer>, String>() {
                @Override
                public String getKey(Tuple2<String, Integer> value) throws Exception {
                  return value.toString().hashCode() + "";
                }
              },
              TypeInformation.of(String.class))
          .process(
              new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                private transient ValueState<Boolean> hasSeen;

                @Override
                public void open(Configuration parameters) {
                  ValueStateDescriptor<Boolean> descriptor =
                      new ValueStateDescriptor<>("hasSeen", TypeInformation.of(Boolean.class));
                  hasSeen = getRuntimeContext().getState(descriptor);
                }

                @Override
                public void processElement(
                    Tuple2<String, Integer> value,
                    Context ctx,
                    Collector<Tuple2<String, Integer>> out)
                    throws Exception {
                  // System.out.println("Processing element: " + value);
                  if (hasSeen.value() == null) {
                    // System.out.println("First time seeing element: " + value);
                    hasSeen.update(true);
                    out.collect(value);
                    // } else {
                    //   System.out.println("Already seen element: " + value);
                  }
                }
              })
          .print();
      // .filter(
      //     new FilterFunction<Tuple2<String, Integer>>() {
      //       @Override
      //       public boolean filter(Tuple2<String, Integer> value) throws Exception {
      //         System.out.println("Deduplicated union stream: " + value);
      //         return true;
      //       }
      //     });
    }

    env.execute("Flink CEP Example");
  }

  public static DataStream<Tuple2<String, Integer>> generateMatchStream(
      DataStream<Tuple2<String, Integer>> inputStream, String startName, String endName) {

    Pattern<Tuple2<String, Integer>, ?> pattern =
        Pattern.<Tuple2<String, Integer>>begin("start")
            .where(
                new SimpleCondition<Tuple2<String, Integer>>() {
                  @Override
                  public boolean filter(Tuple2<String, Integer> event) {
                    return event.f0.equals(startName);
                  }
                })
            .followedByAny("end")
            .where(
                new IterativeCondition<Tuple2<String, Integer>>() {
                  @Override
                  public boolean filter(
                      Tuple2<String, Integer> value, Context<Tuple2<String, Integer>> ctx)
                      throws Exception {
                    Iterable<Tuple2<String, Integer>> events = ctx.getEventsForPattern("start");
                    for (Tuple2<String, Integer> e : events) {
                      // System.out.println("event from start condition: " + e.f0 + " " + e.f1);
                      if (value.f0.equals(endName) && (e.f1 < value.f1)) {
                        return true;
                      }
                    }
                    return false;
                  }
                });

    PatternStream<Tuple2<String, Integer>> patternStream = CEP.pattern(inputStream, pattern);

    DataStream<Tuple2<String, Integer>> matches =
        patternStream.flatSelect(
            new PatternFlatSelectFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
              @Override
              public void flatSelect(
                  Map<String, List<Tuple2<String, Integer>>> patternMatches,
                  Collector<Tuple2<String, Integer>> out)
                  throws Exception {
                // System.out.println("Match found for pattern1: " + patternMatches);
                String concat =
                    patternMatches.get("start").get(0).f0 + patternMatches.get("end").get(0).f0;
                System.out.println(
                    "Match found for pattern " + startName + "_" + endName + ": " + patternMatches);
                out.collect(new Tuple2<>(concat, 0));
              }
            });
    return matches;
  }
}
