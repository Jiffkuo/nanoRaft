package leaderelection;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

public class Peer {
  private static final Logger logger = Logger.getLogger(Peer.class.getName());

  private final String nodeId;
  private final InetSocketAddress socketAddress;

  // Volatile state on leaders.
  /**
   * Index of the next log entry to send. Initialized to leader last log index + 1.
   *
   * <p>See Figure 2 of the raft paper.
   */
  private int nextIndex;

  /**
   * Index of highest log entry known to be replicated on server. Initialized to 0, increases
   * monotonically.
   *
   * <p>See Figure 2 of the raft paper.
   */
  private int matchIndex;

  public Peer(String nodeId, InetSocketAddress socketAddress) {
    this.nodeId = nodeId;
    this.socketAddress = socketAddress;
  }

  public String getNodeId() {
    return nodeId;
  }

  public int getNextIndex() {
    return nextIndex;
  }

  public void setNextIndex(int nextIndex) {
    this.nextIndex = nextIndex;
  }

  public int getMatchIndex() {
    return matchIndex;
  }

  public void setMatchIndex(int matchIndex) {
    this.matchIndex = matchIndex;
  }

  @Override
  public String toString() {
    return "[node=" + nodeId + "][address=" + socketAddress + "]";
  }

  public String sendRpc(String line, int timeout) throws IOException {
    try (Socket socket = new Socket()) {
      try {
        socket.connect(socketAddress, timeout / 2);
      } catch (IOException ioe) {
        logger.warning("[Peer][" + nodeId + "] connection timeout");
        return null;
      }
      // socket's in/output stream will be auto-closed before socket is auto-closed.
      BufferedReader in =
          new BufferedReader(
              new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
      BufferedWriter out =
          new BufferedWriter(
              new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
      socket.setSoTimeout(timeout);

      out.write(line);
      out.newLine();
      out.flush();

      if (socket.isClosed()) {
        logger.warning("[Peer][" + nodeId + "] connection closed without any response");
        return null;
      }

      return in.readLine();
    }
  }
}
