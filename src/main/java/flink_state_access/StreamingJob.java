import java.io.*;
import java.util.*;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamingJob {
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    int port = 6666;
    env.setParallelism(1);
    DataStream<Event> inputEventStream = env.addSource(new SocketSource(port), "Socket Source");

    for (String s : new String[] {"popo", "kris", "nick"}) {
      inputEventStream
          .filter(e -> e.getName().equals(s))
          .addSink(new SocketSink<>())
          .name("Socket Sink " + s);
    }

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
              }
            });
    matches.addSink(new SocketSink<>()).name("Socket Sink Match");

    env.execute("Flink CEP Example");
  }
}
