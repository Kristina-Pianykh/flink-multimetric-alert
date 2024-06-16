// import org.apache.flink.api.common.functions.RichMapFunction;
// import org.apache.flink.api.java.tuple.Tuple2;
// import org.apache.flink.configuration.Configuration;
//
// public class CompareRates extends RichMapFunction<Tuple2<Double, Long>, Double> {
//   // private transient ConcurrentHashMap<String, Double> inputRateMeter;
//   // private transient ConcurrentHashMap<String, Double> matchRateMeter;
//
//   // public CompareRates(
//   //     ConcurrentHashMap<String, Double> matchRateMeter,
//   //     ConcurrentHashMap<String, Double> inputRateMeter) {
//   //   this.matchRateMeter = matchRateMeter;
//   //   this.inputRateMeter = inputRateMeter;
//   // }
//
//   @Override
//   public void open(Configuration parameters) throws Exception {
//     super.open(parameters);
//   }
//
//   @Override
//   public Double map(Tuple2<Double, Long> event) {
//     if (!inputRateMeter.isEmpty()) {
//       System.out.println("Input Rates: " + inputRateMeter.get("hello"));
//     }
//     if (!matchRateMeter.isEmpty()) {
//       System.out.println("Match Rates: " + matchRateMeter.get("hello"));
//     }
//     // System.out.println("Count of input events marked: " + inputRateMeter.getCount());
//     // System.out.println("Count of match events marked: " + matchRateMeter.getCount());
//     // if (inputRateMeter.getRate() >= matchRateMeter.getRate()) {
//     //   System.out.println(
//     //       "Input rate is higher than match rate: "
//     //           + inputRateMeter.getRate()
//     //           + " > "
//     //           + matchRateMeter.getRate());
//     // }
//     return event;
//   }
// }
