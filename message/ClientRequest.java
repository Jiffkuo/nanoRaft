package message;

public class ClientRequest extends Message {

  public ClientRequest(String... args) {
    super(MessageType.CLIENT_REQUEST, args);
  }
}
