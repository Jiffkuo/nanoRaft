package message;

import java.util.Arrays;

public class AppendLogEntry extends Message {

  private final String leaderId;
  private final int term;
  private final int prevLogIndex;
  private final int prevLogTerm;
  private final int leaderCommit;
  private final LogEntryData logEntryData; // TODO

  public AppendLogEntry(String... args) {
    super(MessageType.APPEND_LOG_ENTRY, args);
    leaderId = args[0];
    term = Integer.parseInt(args[1]);
    prevLogIndex = Integer.parseInt(args[2]);
    prevLogTerm = Integer.parseInt(args[3]);
    leaderCommit = Integer.parseInt(args[4]);
    logEntryData = new LogEntryData(Arrays.copyOfRange(args, 5, args.length));
  }

  public AppendLogEntry(
      String leaderId,
      int term,
      int prevLogIndex,
      int prevLogTerm,
      int leaderCommit,
      LogEntryData logEntryData) {
    this(
        leaderId,
        String.valueOf(term),
        String.valueOf(prevLogIndex),
        String.valueOf(prevLogTerm),
        String.valueOf(leaderCommit),
        logEntryData.toString());
  }

  public String getLeaderId() {
    return leaderId;
  }

  public int getTerm() {
    return term;
  }

  public int getPrevLogIndex() {
    return prevLogIndex;
  }

  public LogEntryData getLogEntryData() {
    return logEntryData;
  }

  public int getPrevLogTerm() {
    return prevLogTerm;
  }

  public int getLeaderCommit() {
    return leaderCommit;
  }
}
