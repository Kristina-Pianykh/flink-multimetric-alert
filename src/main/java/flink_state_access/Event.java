public class Event {
  private String name;
  private long timestamp;
  private boolean pattern2Enabled;

  public Event() {}

  public Event(String name, long timestamp, boolean pattern2Enabled) {
    this.name = name;
    this.timestamp = timestamp;
    this.pattern2Enabled = pattern2Enabled;
  }

  public String getName() {
    return name;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public boolean isPattern2Enabled() {
    return pattern2Enabled;
  }

  public void setPatternFlag(boolean pattern2Enabled) {
    this.pattern2Enabled = pattern2Enabled;
  }

  @Override
  public String toString() {
    return "Event{"
        + "name='"
        + name
        + '\''
        + ", timestamp="
        + timestamp
        + '}'
        + ", pattern2Enabled="
        + pattern2Enabled;
  }
}
