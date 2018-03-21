package message;

public class Redirect extends Message {

  private final String lastKnownLeader;

  public Redirect(String... args) {
    super(MessageType.REDIRECT, args);
    if (args.length != 0) {
      throw new IllegalStateException("args");
    }
    lastKnownLeader = args[0];
  }
}
