import java.io.Serializable;

public class Event implements Serializable {
  private static final long serialVersionUID = 1L;
  private String name;
  private boolean pattern2Enabled;

  public Event() {}

  public Event(String name, boolean pattern2Enabled) {
    this.name = name;
    this.pattern2Enabled = pattern2Enabled;
  }

  public String getName() {
    return name;
  }

  public boolean isPattern2Enabled() {
    return pattern2Enabled;
  }

  public void setPatternFlag(boolean pattern2Enabled) {
    this.pattern2Enabled = pattern2Enabled;
  }

  @Override
  public String toString() {
    return "Event{" + "name='" + name + '\'' + ", pattern2Enabled=" + pattern2Enabled + '}';
  }
}
