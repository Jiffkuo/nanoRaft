package message;

public enum MessageType {
  UNKNOWN, // typically when wrong input is typed in testing.
  VOTE_REQUEST,
  VOTE_RESPONSE,
  APPEND_LOG_ENTRY, // or heart beat
  APPEND_LOG_ENTRY_RESPONSE,
  CLIENT_REQUEST,
  CLIENT_REQUEST_RESPONSE,
  REDIRECT
}
