package message;

public class ClientRequestResponse extends Message {
  public ClientRequestResponse(String... args) {
    super(MessageType.CLIENT_REQUEST_RESPONSE, args);
  }
}
