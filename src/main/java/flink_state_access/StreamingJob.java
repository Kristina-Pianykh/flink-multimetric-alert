import java.io.*;
import java.util.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
    inputStream.print();

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

    // Define a pattern: looking for a sequence of "pepe" -> "popo"
    Pattern<Tuple2<String, Integer>, ?> pattern1 =
        Pattern.<Tuple2<String, Integer>>begin("start")
            .where(
                new SimpleCondition<Tuple2<String, Integer>>() {
                  @Override
                  public boolean filter(Tuple2<String, Integer> event) {
                    return event.f0.equals("pepe");
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
                      if (value.f0.equals("popo") && (e.f1 < value.f1)) {
                        return true;
                      }
                    }
                    return false;
                    // return value.f1.equals("popo");
                  }
                });

    Pattern<Tuple2<String, Integer>, ?> pattern2 =
        Pattern.<Tuple2<String, Integer>>begin("start")
            .where(
                new SimpleCondition<Tuple2<String, Integer>>() {
                  @Override
                  public boolean filter(Tuple2<String, Integer> event) {
                    // System.out.println("event start for pattern2: " + event);
                    if (event.f0.equals("pepepopo")) {
                      System.out.println("event start for pattern2: " + event);
                      return true;
                    }
                    return false;
                  }
                })
            .followedByAny("end")
            .where(
                new SimpleCondition<Tuple2<String, Integer>>() {
                  @Override
                  public boolean filter(Tuple2<String, Integer> value) {
                    if (value.f0.equals("kris")) {
                      System.out.println("event end for pattern2: " + value);
                      return true;
                    }
                    return false;
                  }
                });

    PatternStream<Tuple2<String, Integer>> patternStream1 = CEP.pattern(inputStream, pattern1);

    DataStream<Tuple2<String, Integer>> matches1 =
        patternStream1.flatSelect(
            new PatternFlatSelectFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
              @Override
              public void flatSelect(
                  Map<String, List<Tuple2<String, Integer>>> patternMatches,
                  Collector<Tuple2<String, Integer>> out)
                  throws Exception {
                // System.out.println("Match found for pattern1: " + patternMatches);
                String concat =
                    patternMatches.get("start").get(0).f0 + patternMatches.get("end").get(0).f0;
                System.out.println("Match found for pattern1: " + patternMatches);
                out.collect(new Tuple2<>(concat, 0));
              }
            });

    matches1.print();

    DataStream<Tuple2<String, Integer>> unionStream =
        inputStream.union(matches1).assignTimestampsAndWatermarks(new CustomWatermarkStrategy());

    PatternStream<Tuple2<String, Integer>> patternStream2 = CEP.pattern(unionStream, pattern2);

    DataStream<String> matches2 =
        patternStream2.flatSelect(
            new PatternFlatSelectFunction<Tuple2<String, Integer>, String>() {
              @Override
              public void flatSelect(
                  Map<String, List<Tuple2<String, Integer>>> patternMatches, Collector<String> out)
                  throws Exception {
                // System.out.println("Match found: " + patternMatches);
                out.collect("Match found for pattern 2: " + patternMatches);
              }
            });
    matches2.print();
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
