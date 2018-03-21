package message;

public class VoteResponse extends Message {

  // Arguments.
  // See Figure 2 of the raft paper.
  private final boolean voteGranted;
  private final int term;

  public VoteResponse(String... args) {
    super(MessageType.VOTE_RESPONSE, args);

    if (args.length != 2) {
      throw new IllegalStateException("args");
    }

    voteGranted = Boolean.parseBoolean(args[0]);
    term = Integer.parseInt(args[1]);
  }

  public VoteResponse(boolean voteGranted, int term) {
    this(String.valueOf(voteGranted), String.valueOf(term));
  }

  public boolean isVoteGranted() {
    return voteGranted;
  }

  public int getTerm() {
    return term;
  }
}
