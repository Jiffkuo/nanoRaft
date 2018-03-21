package message;

public class LogEntryData {
  private final String[] args;
  private final int term;

  public LogEntryData(String... args) {
    this.args = args;
    term = Integer.valueOf(args[0]);
  }

  public int getTerm() {
    return term;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (String arg : args) {
      sb.append(arg).append(' ');
    }
    return sb.toString().trim();
  }
}
