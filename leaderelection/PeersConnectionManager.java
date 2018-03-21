package leaderelection;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PeersConnectionManager {
  private final List<Peer> peers;

  public PeersConnectionManager(Map<String, InetSocketAddress> peerAddrs) {
    List<Peer> peers = new ArrayList<>(peerAddrs.size());
    for (Map.Entry<String, InetSocketAddress> peerAddr : peerAddrs.entrySet()) {
      peers.add(new Peer(peerAddr.getKey(), peerAddr.getValue()));
    }
    this.peers = Collections.unmodifiableList(peers);
  }

  public List<Peer> getPeers() {
    return peers;
  }
}
