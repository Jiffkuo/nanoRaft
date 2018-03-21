package leaderelection;

import static leaderelection.State.LEADER;
import static message.MessageType.APPEND_LOG_ENTRY;
import static message.MessageType.CLIENT_REQUEST;
import static message.MessageType.VOTE_REQUEST;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;
import message.AppendLogEntry;
import message.AppendLogEntryResponse;
import message.ClientRequest;
import message.Message;
import message.MessageType;
import message.Redirect;
import message.VoteRequest;
import message.VoteResponse;

public class Server {
  private static final Logger logger = Logger.getLogger(Server.class.getName());

  private final InetSocketAddress socketAddress;
  private final StateManager stateManager;
  private final ScheduledExecutorService executor;

  public Server(
      InetSocketAddress socketAddress,
      StateManager stateManager,
      ScheduledExecutorService executor) {
    this.socketAddress = socketAddress;
    this.stateManager = stateManager;
    this.executor = executor;
  }

  public void start() {
    stateManager.goToFollowerMode(0);
    try (ServerSocket serverSocket = new ServerSocket()) {
      serverSocket.bind(socketAddress);
      logger.info("[server started][" + socketAddress + "]");
      while (true) {
        Socket clientSocket = serverSocket.accept();
        logger.finest("[connected with][" + clientSocket.getRemoteSocketAddress() + "]");
        executor.execute(() -> new Listener(clientSocket).listen());
      }
    } catch (IOException ioe) {
      onUnexpectedException("[server failed][" + socketAddress + "]", ioe);
      System.exit(1);
    }
  }

  private void onUnexpectedException(String msg, Exception e) {
    logger.log(Level.WARNING, "[unexpected exception] " + msg, e);
  }

  private class Listener {
    final Socket clientSocket;

    BufferedReader in;
    BufferedWriter out;

    Listener(Socket clientSocket) {
      this.clientSocket = clientSocket;
    }

    void listen() {
      SocketAddress remoteSocketAddress = clientSocket.getRemoteSocketAddress();
      try (Socket socket = clientSocket) {
        // socket's in/output stream will be auto-closed before socket is auto-closed.
        in =
            new BufferedReader(
                new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        out =
            new BufferedWriter(
                new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
        String line;
        while (!socket.isClosed() && (line = in.readLine()) != null) {
          if (line.trim().isEmpty()) {
            continue;
          }
          Message message = Message.parse(line);

          logger.info("[received from][" + remoteSocketAddress + "] " + message);

          MessageType rpcType = message.getMessageType();
          if (rpcType == CLIENT_REQUEST) {
            onClientRequest(message.asClientRequest());
          } else if (rpcType == VOTE_REQUEST) {
            onVoteRequest(message.asRequestVote());
          } else if (rpcType == APPEND_LOG_ENTRY) {
            onAppendLogEntry(message.asAppendLogEntry());
          } else {
            onUnknownMessage(message);
          }
        }
      } catch (Exception e) {
        onUnexpectedException("[" + remoteSocketAddress + "]", e);
      } finally {
        logger.finest("[connection closed][" + remoteSocketAddress + "]");
      }
    }

    public void onVoteRequest(VoteRequest voteRequest) throws IOException {
      VoteResponse voteResponse;

      synchronized (stateManager) {
        int candidateTerm = voteRequest.getTerm();
        int candidateLogIndex = voteRequest.getLastLogIndex();
        int candidateLogTerm = voteRequest.getLastLogTerm();
        String candidateId = voteRequest.getCandidateId();

        int myTerm = stateManager.getCurrentTerm();
        int myLogIndex = stateManager.getLastApplied();
        int myLogTerm = stateManager.getLog().get(myLogIndex).getTerm();
        String votedFor = stateManager.getVotedFor();
        if ( // See Figure 2 of the raft paper, RequestVote RPC, for details
            candidateTerm > myTerm
            //candidateTerm >= myTerm
            //&& (votedFor == null || votedFor.equals(candidateId))
            && candidateLogIsMoreUpToDate(
                candidateLogTerm, candidateLogIndex, myLogTerm, myLogIndex)) {
          stateManager.goToFollowerMode(candidateTerm);
          stateManager.setVotedFor(candidateId);
          voteResponse = new VoteResponse(true, myTerm);
        } else {
          if (candidateTerm > myTerm) {
            stateManager.goToFollowerMode(candidateTerm);
            stateManager.setVotedFor(null);
          }
          voteResponse = new VoteResponse(false, myTerm);
        }
      }

      out.write(voteResponse.toString());
      out.flush();
      clientSocket.close();
    }

    private boolean candidateLogIsMoreUpToDate(
        int candidateLogTerm, int candidateLogIndex, int myLogTerm, int myLogIndex) {
      if (candidateLogTerm < myLogTerm) {
        return false;
      }
      if (candidateLogTerm == myLogTerm && candidateLogIndex < myLogIndex) {
        return false;
      }
      return true;
    }

    /** Returns false if the peer is not considered as the leader anymore. */
    public void onAppendLogEntry(AppendLogEntry appendLogEntry) throws IOException {
      clientSocket.setSoTimeout(500); // keep connection alive for 500ms until next message is read

      AppendLogEntryResponse appendLogEntryResponse;

      synchronized (stateManager) {
        int leaderTerm = appendLogEntry.getTerm();
        int leaderPrevLogIndex = appendLogEntry.getPrevLogIndex();
        int leaderPrevLogTerm = appendLogEntry.getPrevLogTerm();
        int myTerm = stateManager.getCurrentTerm();
        int myLogIndex = stateManager.getLastApplied();
        if (leaderTerm >= myTerm
            && leaderPrevLogIndex >= myLogIndex
            && stateManager.getLog().get(leaderPrevLogIndex).getTerm() == leaderPrevLogTerm) {

          // TODO:
          // If an eisting entry conflicts with a new one (same index but different terms),
          // delete the existing entry and all that follow it.
          // Apppend any new entries not already in the log.
          // If leaderCommit > commitIndex, update commitIndex.

          stateManager.goToFollowerMode(leaderTerm);
          stateManager.setLastKnownLeader(appendLogEntry.getLeaderId());
          appendLogEntryResponse = new AppendLogEntryResponse(true, myTerm);
        } else {
          appendLogEntryResponse = new AppendLogEntryResponse(false, myTerm);
          if (leaderTerm > myTerm) {
            stateManager.goToFollowerMode(leaderTerm);
          }
        }

        if (leaderTerm > myTerm) {
          stateManager.setVotedFor(null);
        }
      }

      out.write(appendLogEntryResponse.toString());
      out.newLine();
      out.flush();
    }

    public void onClientRequest(ClientRequest clientRequest) throws IOException {
      State state;
      String lastKnownLeader;
      synchronized (stateManager) {
        state = stateManager.getState();
        lastKnownLeader = stateManager.getLastKnownLeader();
      }

      if (state == LEADER) {
        handleClientRequest(clientRequest);
      } else {
        Redirect redirect = new Redirect(lastKnownLeader);
        out.write(redirect.toString());
        out.flush();
        clientSocket.close();
      }
    }

    private void handleClientRequest(ClientRequest clientRequest) {
      // TODO: implementation
    }

    public void onUnknownMessage(Message message) throws IOException {
      logger.warning("[unrecognized message][" + message + "]");
      out.write(message.toString());
      out.flush();
      clientSocket.close();
    }
  }
}
