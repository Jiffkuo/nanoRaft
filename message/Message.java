package message;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public abstract class Message {
  private final MessageType messageType;
  private final String[] args;

  protected Message(MessageType messageType, String... args) {
    this.messageType = messageType;
    this.args = args;
  }

  /** Deserializes the message. */
  public static Message parse(String line) {
    String[] entries = line.trim().split("\\s+");
    if (entries.length == 0) {
      return new UnknownMessage();
    }

    MessageType messageType;
    try {
      messageType = MessageType.valueOf(entries[0]);
    } catch (IllegalArgumentException iae) {
      return new UnknownMessage(entries);
    }

    String[] args = Arrays.copyOfRange(entries, 1, entries.length);

    switch (messageType) {
      case CLIENT_REQUEST:
        return new ClientRequest(args);
      case CLIENT_REQUEST_RESPONSE:
        return new ClientRequestResponse(args);
      case REDIRECT:
        return new Redirect(args);
      case VOTE_REQUEST:
        return new VoteRequest(args);
      case VOTE_RESPONSE:
        return new VoteResponse(args);
      case APPEND_LOG_ENTRY:
        return new AppendLogEntry(args);
      case APPEND_LOG_ENTRY_RESPONSE:
        return new AppendLogEntryResponse(args);
      default:
        return new UnknownMessage(args);
    }
  }

  /**
   * Serializes the message.
   *
   * <p>"MessageType args[0] args[1] ... "
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(messageType.name());
    for (String s : args) {
      sb.append(" ").append(s);
    }
    return sb.toString();
  }

  public MessageType getMessageType() {
    return messageType;
  }

  public List<String> getArgs() {
    return Collections.unmodifiableList(Arrays.asList(args));
  }

  public ClientRequest asClientRequest() {
    if (this instanceof ClientRequest) {
      return (ClientRequest) this;
    }
    throw new IllegalStateException();
  }

  public ClientRequestResponse asClientRequestResponse() {
    if (this instanceof ClientRequestResponse) {
      return (ClientRequestResponse) this;
    }
    throw new IllegalStateException();
  }

  public VoteRequest asRequestVote() {
    if (this instanceof VoteRequest) {
      return (VoteRequest) this;
    }
    throw new IllegalStateException();
  }

  public VoteResponse asVoteResponse() {
    if (this instanceof VoteResponse) {
      return (VoteResponse) this;
    }
    throw new IllegalStateException();
  }

  public AppendLogEntry asAppendLogEntry() {
    if (this instanceof AppendLogEntry) {
      return (AppendLogEntry) this;
    }
    throw new IllegalStateException();
  }

  public AppendLogEntryResponse asAppendLogEntryResponse() {
    if (this instanceof AppendLogEntryResponse) {
      return (AppendLogEntryResponse) this;
    }
    throw new IllegalStateException();
  }

  public Redirect asRedirect() {
    if (this instanceof Redirect) {
      return (Redirect) this;
    }
    throw new IllegalStateException();
  }
}
