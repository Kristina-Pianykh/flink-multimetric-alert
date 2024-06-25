import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;

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

    public Integer add(Event ignored, Integer acc) {
      return acc + 1;
    }

    public Double getResult(Integer acc) {
      return (double) acc / (double) GenerateContinuousRates.timeWindowSizeInSec;
    }
  }

  public static DataStream<Double> generateContinuousRatesFromEvents(DataStream<Event> input) {
    return input
        .windowAll(
            SlidingProcessingTimeWindows.of(
                Time.seconds(GenerateContinuousRates.timeWindowSizeInSec),
                Time.seconds(GenerateContinuousRates.timeWindowSlideSizeInSec)))
        .trigger(
            ContinuousProcessingTimeTrigger.of(
                Time.seconds(GenerateContinuousRates.timeWindowSlideSizeInSec)))
        .aggregate(new AverageEvents());
  }

  public static class AverageStrings implements AggregateFunction<String, Integer, Double> {

    public Integer createAccumulator() {
      return 0;
    }

    public Integer merge(Integer a, Integer b) {
      return a + b;
    }

    public Integer add(String ignored, Integer acc) {
      return acc + 1;
    }

    public Double getResult(Integer acc) {
      return (double) acc / (double) GenerateContinuousRates.timeWindowSizeInSec;
    }
  }

  public static DataStream<Double> generateContinuousRatesFromStrings(DataStream<String> input) {
    return input
        .windowAll(
            SlidingProcessingTimeWindows.of(
                Time.seconds(GenerateContinuousRates.timeWindowSizeInSec),
                Time.seconds(GenerateContinuousRates.timeWindowSlideSizeInSec)))
        .trigger(
            ContinuousProcessingTimeTrigger.of(
                    Time.seconds(GenerateContinuousRates.timeWindowSlideSizeInSec))
                .onProcessingTime())
        .aggregate(new AverageStrings());
  }
}
