import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;

// import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;

public class GenerateContinuousRates {
  public static Integer timeWindowSizeInSec = 10;
  public static Integer timeWindowSlideSizeInSec = 1;

  // implementation of an aggregation function for an 'average'
  public static class AverageEvents implements AggregateFunction<Event, Integer, Double> {

    public Integer createAccumulator() {
      return 0;
    }

    public Integer merge(Integer a, Integer b) {
      return a + b;
    }

    public Integer add(Event event, Integer acc) {
      // System.out.println("agg func on event " + event.getName());
      // System.out.println("acc: " + acc);
      if (event.getName().equals("timer")) {
        return acc;
      }
      return acc + 1;
    }

    public Double getResult(Integer acc) {
      return (double) acc / (double) GenerateContinuousRates.timeWindowSizeInSec;
    }
  }

  public static DataStream<Tuple2<Double, Long>> generateContinuousRatesFromEvents(
      DataStream<Event> input) {
    long timestamp = System.currentTimeMillis();
    // fire the rate every second no matter whether there are new events or not
    DataStream<Double> rates =
        input
            .windowAll(
                SlidingProcessingTimeWindows.of(
                    Time.seconds(GenerateContinuousRates.timeWindowSizeInSec),
                    Time.seconds(GenerateContinuousRates.timeWindowSlideSizeInSec)))
            .trigger(ProcessingTimeTrigger.create())
            .aggregate(new AverageEvents());
    return rates.map(
        new MapFunction<Double, Tuple2<Double, Long>>() {
          @Override
          public Tuple2<Double, Long> map(Double value) {
            return new Tuple2<Double, Long>(value, System.currentTimeMillis());
          }
        });
  }
}
