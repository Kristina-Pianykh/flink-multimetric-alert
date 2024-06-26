import java.io.*;
import java.net.*;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;

public class DummySource implements SourceFunction<Event> {

  private volatile boolean isRunning = true;

  @Override
  public void run(SourceContext<Event> ctx) throws Exception {
    while (isRunning) {
      synchronized (ctx.getCheckpointLock()) {
        Event event = new Event("timer", true);
        ctx.collect(event);
      }
      Thread.sleep(500); // Generate an event every 3 seconds
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
  }

  // public static void main(String[] args) throws Exception {
  //   final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  //   env.addSource(new DummyEventSource()).print();
  //   env.execute("Dummy Event Source Example");
  // }
}
