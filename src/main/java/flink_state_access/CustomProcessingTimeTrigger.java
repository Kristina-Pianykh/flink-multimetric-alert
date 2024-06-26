import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class CustomProcessingTimeTrigger<W extends Window> extends Trigger<Object, W> {
  private final long interval;

  public CustomProcessingTimeTrigger(long interval) {
    this.interval = interval;
  }

  @Override
  public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) {
    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) {
    ctx.registerProcessingTimeTimer(time + interval);
    return TriggerResult.FIRE;
  }

  @Override
  public TriggerResult onEventTime(long time, W window, TriggerContext ctx) {
    return TriggerResult.CONTINUE;
  }

  @Override
  public void clear(W window, TriggerContext ctx) {
    ctx.deleteProcessingTimeTimer(window.maxTimestamp());
  }

  public static <W extends Window> CustomProcessingTimeTrigger<W> create(long interval) {
    return new CustomProcessingTimeTrigger<>(interval);
  }
}
