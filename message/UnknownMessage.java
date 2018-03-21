package message;

public class UnknownMessage extends Message {
  public UnknownMessage(String... args) {
    super(MessageType.UNKNOWN, args);
  }
}
