import java.io.*;
import java.util.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class StatefulCoEvaluation extends RichCoFlatMapFunction<Double, Double, Double> {
  private transient ValueState<Double> currentSum;
  private final int nonPartInputSize;
  private final int k;

  public StatefulCoEvaluation(int iter, int nonPartInputSize) {
    this.nonPartInputSize = nonPartInputSize;
    this.k = iter;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    currentSum =
        getRuntimeContext().getState(new ValueStateDescriptor<>("currentSum", Double.class, 0.0));
  }

  @Override
  public void flatMap1(Double value, Collector<Double> out) throws Exception {
    currentSum.update(value);
  }

  // Simulate the case when the result of comparison fires a trigger.
  // flatMap2 only because it's performed on match events
  // and we need to recalculate on every match
  @Override
  public void flatMap2(Double value, Collector<Double> out) throws Exception {

    Double tmpSum = currentSum.value();
    System.out.println("Current sum: " + tmpSum);

    if (k > nonPartInputSize) {
      Double partInputRate = value;
      boolean nonZeroRates = tmpSum >= 0.0 && value >= 0.0;
      System.out.println("Comparing the LHS of the inequality to the RHS");
      if (partInputRate <= tmpSum && nonZeroRates) {
        System.out.println(
            "The rate of the partitioning input is too low for the multi-node"
                + " placement of the query");
        System.out.println("Partitioning input rate: " + value + " <= " + tmpSum);
        System.out.println("Trigger switch");
      } else if (partInputRate > tmpSum && nonZeroRates) {
        System.out.println("The rate of the partitioning input is high enough for the multi-node");
        System.out.println("Partitioning input rate: " + value + " > " + tmpSum);
      }
    } else {
      Double updatedSum = tmpSum + value;
      currentSum.update(updatedSum);
      out.collect(updatedSum);
      System.out.println("Updated sum: " + updatedSum + "\n");
    }
  }
}
