package leaderelection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import message.AppendLogEntry;
import message.AppendLogEntryResponse;
import message.LogEntryData;
import message.Message;
import message.VoteRequest;
import message.VoteResponse;

public class StateManager {
  private static final Logger logger = Logger.getLogger(StateManager.class.getName());

  private static final int HEART_BEAT_TIMEOUT_MILLIS = 20;
  private static final int BASE_ELECTION_TIMEOUT_MILLIS = 12;
  private final String nodeId;
  private final ScheduledExecutorService executor;
  private final PeersConnectionManager peersConnectionManager;
  private State state = State.FOLLOWER;
  private ElectionTimeout electionTimeout;
  private HeartBeatTimeout heartBeatTimeout;
  private int rpcId;

  // Persistent state. Updated on stable storage before responding to RPCs.
  // See Figure 2 of the raft paper.
  private int currentTerm;
  private String votedFor;
  private List<LogEntryData> log = new ArrayList<>();

  // Volatile state on leaders
  // See Figure 2 of the raft paper.
  private int commitIndex;

  private String lastKnownLeader;

  private CountDownLatch latch;

  public StateManager(
      String nodeId,
      ScheduledExecutorService executor,
      PeersConnectionManager peersConnectionManager) {
    this.nodeId = nodeId;
    this.executor = executor;
    this.peersConnectionManager = peersConnectionManager;
    log.add(new LogEntryData("0")); // term 0 log entry.
  }

  public int getLastApplied() {
    return log.size() - 1;
  }

  private void sendVoteRequests(final ElectionTimeout electionTimeout) {
    logger.info("[" + nodeId + "][" + state + "][RequsetVote][sending requests]");
    int lastAppliedIndex = getLastApplied();
    int lastAppliedTerm = log.get(getLastApplied()).getTerm();
    VoteRequest voteRequest =
        new VoteRequest(nodeId, currentTerm, lastAppliedIndex, lastAppliedTerm);

    for (Peer peer : peersConnectionManager.getPeers()) {
      logger.info(
          "["
              + nodeId
              + "]["
              + state
              + "][RequsetVote][sending to]["
              + peer.getNodeId()
              + "]"
              + voteRequest);
      executor.execute( // run asynchronously
          () -> {
            try {

              String response =
                  peer.sendRpc(voteRequest.toString(), BASE_ELECTION_TIMEOUT_MILLIS * 2);

              if (response != null && !response.trim().isEmpty()) {
                VoteResponse voteResponse = Message.parse(response).asVoteResponse();

                logger.info(
                    "[" + nodeId + "][" + state + "][RequsetVote][received]" + voteResponse);

                synchronized (this) {
                  if (this.electionTimeout == electionTimeout && voteResponse.isVoteGranted()) {
                    electionTimeout.votes++;
                    if (electionTimeout.votes >= peersConnectionManager.getPeers().size() / 2) {
                      goToLeaderMode();
                    }
                  }

                  int responseTerm = voteResponse.getTerm();
                  if (responseTerm > currentTerm) {
                    goToFollowerMode(responseTerm);
                    setVotedFor(null);
                  }
                }
              }
            } catch (Exception e) {
              onUnexpectedException(
                  "[" + nodeId + "][" + state + "][sendVoteRequests][peer]" + peer.getNodeId(), e);
            }
          });
    }
  }

  private void sendAppendLogEntry(HeartBeatTimeout heartBeatTimeout) {
    LogEntryData logEntryData = new LogEntryData("0"); // TODO

    AppendLogEntry appendLogEntry =
        new AppendLogEntry(
            nodeId,
            currentTerm,
            getLastApplied(),
            log.get(getLastApplied()).getTerm(),
            commitIndex,
            logEntryData);

    for (Peer peer : peersConnectionManager.getPeers()) {
      executor.execute( // run asynchronously
          () -> {
            try {
              logger.info(
                  "[" + nodeId + "][" + state + "][AppendLogEntry][sending]" + appendLogEntry);

              String response = peer.sendRpc(appendLogEntry.toString(), HEART_BEAT_TIMEOUT_MILLIS);
              if (response != null && !response.trim().isEmpty()) {
                AppendLogEntryResponse appendLogEntryResponse =
                    Message.parse(response).asAppendLogEntryResponse();

                logger.info(
                    "["
                        + nodeId
                        + "]["
                        + state
                        + "][AppendLogEntry][received]"
                        + appendLogEntryResponse);
                synchronized (this) {
                  if (this.heartBeatTimeout == heartBeatTimeout
                      && appendLogEntryResponse.isSuccess()) {
                    heartBeatTimeout.votes++;
                    if (heartBeatTimeout.votes >= peersConnectionManager.getPeers().size() / 2) {
                      // TODO:
                    }
                  }

                  int responseTerm = appendLogEntryResponse.getTerm();
                  if (responseTerm > currentTerm) {
                    goToFollowerMode(responseTerm);
                    setVotedFor(null);
                  }
                }
              }
            } catch (Exception e) {
              onUnexpectedException(
                  "[" + nodeId + "][" + state + "][sendAppendLogEntry][peer]" + peer.getNodeId(),
                  e);
            }
          });
    }
  }

  public State getState() {
    return state;
  }

  public int getCurrentTerm() {
    return currentTerm;
  }

  public String getVotedFor() {
    return votedFor;
  }

  public void setVotedFor(String votedFor) {
    this.votedFor = votedFor;
  }

  /** The last known leader or a random peer if not available. */
  public String getLastKnownLeader() {
    if (lastKnownLeader == null) {
      List<Peer> peers = peersConnectionManager.getPeers();
      Peer peer = peers.get(new Random().nextInt(peers.size()));
      return peer.getNodeId();
    }
    return lastKnownLeader;
  }

  public void setLastKnownLeader(String lastKnownLeader) {
    this.lastKnownLeader = lastKnownLeader;
    latch.countDown();
  }

  public List<LogEntryData> getLog() {
    return Collections.unmodifiableList(log);
  }

  public int getCommitIndex() {
    return commitIndex;
  }

  public void updateCommitIndex(int newIndex) {
    commitIndex = Math.max(commitIndex, newIndex);
  }

  public synchronized void goToFollowerMode(int term) {
    logger.info("[" + nodeId + "][" + state + "][->][FOLLOWER][term]" + term);

    if (peersConnectionManager.getPeers().size() == 0) {
      goToLeaderMode();
      return;
    }

    if (electionTimeout != null) {
      electionTimeout.cancel();
    }

    if (heartBeatTimeout != null) {
      heartBeatTimeout.cancel();
    }

    currentTerm = term;

    state = State.FOLLOWER;
    electionTimeout = new ElectionTimeout();
  }

  public synchronized void goToCandidateMode() {
    logger.info("[" + nodeId + "][" + state + "][->][CANDIDATE][term]" + currentTerm);

    if (electionTimeout != null) {
      electionTimeout.cancel();
    }

    if (heartBeatTimeout != null) {
      heartBeatTimeout.cancel();
    }

    state = State.CANDIDATE;
    setVotedFor(nodeId);
    currentTerm++;

    electionTimeout = new ElectionTimeout();
    sendVoteRequests(electionTimeout);
  }

  public synchronized void goToLeaderMode() {
    logger.info("[" + nodeId + "][" + state + "][->][LEADER][term]" + currentTerm);

    if (electionTimeout != null) {
      electionTimeout.cancel();
    }

    if (heartBeatTimeout != null) {
      heartBeatTimeout.cancel();
    }

    state = State.LEADER;
    setLastKnownLeader(nodeId);
    //setVotedFor(null);

    heartBeatTimeout = new HeartBeatTimeout();
    sendAppendLogEntry(heartBeatTimeout);
  }

  /** A random number between BASE_ELECTION_TIMEOUT_MILLIS and 2 x BASE_ELECTION_TIMEOUT_MILLIS. */
  private int getElectionTimeoutMillis() {
    return BASE_ELECTION_TIMEOUT_MILLIS + new Random().nextInt(BASE_ELECTION_TIMEOUT_MILLIS);
  }

  private void onUnexpectedException(String msg, Exception e) {
    logger.log(Level.WARNING, "[unexpected exception] " + msg, e);
  }

  public void setLatch(CountDownLatch latch) {
    this.latch = latch;
  }

  private class ElectionTimeout {
    final Future<?> electionTimeoutTask;
    boolean cancelled;
    int votes;

    ElectionTimeout() {
      synchronized (StateManager.this) {
        electionTimeoutTask =
            executor.schedule(
                () -> {
                  try {
                    onElectionTimeout();
                  } catch (Exception e) {
                    onUnexpectedException("[" + nodeId + "][" + state + "][onElectionTimeout]", e);
                  }
                },
                getElectionTimeoutMillis(),
                TimeUnit.MILLISECONDS);
      }
    }

    void onElectionTimeout() {
      // Must be called under synchronized block to guarantee the task won't run if cancelled.
      synchronized (StateManager.this) {
        if (cancelled) {
          return;
        }

        logger.info("[" + nodeId + "][" + state + "][ElectionTimeout]");

        StateManager.this.electionTimeout = null;

        goToCandidateMode();
      }
    }

    /** Cancels the timeout task. */
    void cancel() {
      // Must be called under synchronized block to guarantee the timeout task won't run if cancel()
      // is called.
      synchronized (StateManager.this) {
        cancelled = true;
        electionTimeoutTask.cancel(false);
        electionTimeout = null;
      }
    }
  }

  private class HeartBeatTimeout {
    final Future<?> heartBeatTimeoutTask;
    boolean cancelled;
    int votes;

    HeartBeatTimeout() {
      synchronized (StateManager.this) {
        heartBeatTimeoutTask =
            executor.schedule(
                () -> {
                  try {
                    onHeartBeatTimeout();
                  } catch (Exception e) {
                    onUnexpectedException("[" + nodeId + "][" + state + "][onHeartBeatTimeout]", e);
                  }
                },
                HEART_BEAT_TIMEOUT_MILLIS,
                TimeUnit.MILLISECONDS);
      }
    }

    void onHeartBeatTimeout() {
      // Must be called under synchronized block to guarantee the task won't run if cancelled.
      synchronized (StateManager.this) {
        if (cancelled) {
          return;
        }

        logger.info("[" + nodeId + "][" + state + "][HeartBeatTimeout]");

        StateManager.this.heartBeatTimeout = null;

        goToLeaderMode();
      }
    }

    /** Cancels the timeout task. */
    void cancel() {
      // Must be called under synchronized block to guarantee the timeout task won't run if cancel()
      // is called.
      synchronized (StateManager.this) {
        cancelled = true;
        heartBeatTimeoutTask.cancel(false);
        heartBeatTimeout = null;
      }
    }
  }
}
