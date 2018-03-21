package message;

public class VoteRequest extends Message {

  // Arguments.
  // See Figure 2 of the raft paper.
  private final String candidateId;
  private final int term; // candidate's term
  private final int lastLogIndex; // index of candidate's last log entry
  private final int lastLogTerm; // term of candidate's last log entry

  VoteRequest(String... args) {
    super(MessageType.VOTE_REQUEST, args);
    if (args.length != 4) {
      throw new IllegalArgumentException("args");
    }

    candidateId = args[0];
    term = Integer.parseInt(args[1]);
    lastLogIndex = Integer.parseInt(args[2]);
    lastLogTerm = Integer.parseInt(args[3]);
  }

  public VoteRequest(String candidateId, int term, int lastLogIndex, int lastLogTerm) {
    this(
        candidateId,
        String.valueOf(term),
        String.valueOf(lastLogIndex),
        String.valueOf(lastLogTerm));
  }

  public String getCandidateId() {
    return candidateId;
  }

  public int getTerm() {
    return term;
  }

  public int getLastLogIndex() {
    return lastLogIndex;
  }

  public int getLastLogTerm() {
    return lastLogTerm;
  }
}
