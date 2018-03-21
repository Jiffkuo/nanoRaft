/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package leaderelection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Server node.
 *
 * @author Melody
 */
public class Node {

  private final String nodeId;
  private final Server listener;
  private final ScheduledExecutorService executor; // executor asynchronously or schedule a task
  private final StateManager stateManager;

  public Node(String nodeId, InetSocketAddress selfAddr, Map<String, InetSocketAddress> peerAddrs) {
    this.nodeId = nodeId;
    executor = Executors.newScheduledThreadPool(peerAddrs.size());
    PeersConnectionManager peersConnectionManager = new PeersConnectionManager(peerAddrs);
    stateManager = new StateManager(nodeId, executor, peersConnectionManager);
    listener = new Server(selfAddr, stateManager, executor);
  }

  public void start() throws IOException {
    listener.start();
  }

  public void startUntilLeaderElected() throws Exception {
    CountDownLatch countDownLatch = new CountDownLatch(1); // count down once then leave
    stateManager.setLatch(countDownLatch);
    executor.execute(()->listener.start());
    countDownLatch.await();
  }

  public String getNodeId() {
    return nodeId;
  }

  public StateManager getStateManager() {
    return stateManager;
  }
}
