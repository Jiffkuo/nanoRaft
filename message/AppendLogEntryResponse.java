package message;

public class AppendLogEntryResponse extends Message {

  // Arguments.
  // See Figure 2 of the raft paper.
  private final boolean success;
  private final int term;

  public AppendLogEntryResponse(String... args) {
    super(MessageType.APPEND_LOG_ENTRY_RESPONSE, args);
    success = Boolean.parseBoolean(args[0]);
    term = Integer.parseInt(args[1]);
  }

  public AppendLogEntryResponse(boolean success, int term) {
    this(String.valueOf(success), String.valueOf(term));
  }

  public boolean isSuccess() {
    return success;
  }

  public int getTerm() {
    return term;
  }
}
