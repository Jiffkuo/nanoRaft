package Communicator;

import definedtype.CommType;
import definedtype.RPCs;
import host.HostAddress;
import host.HostManager;
import message.MessageHandler;

import java.util.Map;

public class TCP_Communicator {
    private boolean global_DEBUG = false;
    private String commOwner;

    public TCP_Communicator(String hostAddressName) {
        commOwner = hostAddressName;
    }

    /**
     * Broadcast message to all hosts, count if replies reach majority
     * @param hostManager give host manager who has all hosts infor
     * @param tcp_ReplyMsg_All reply nodes' info will be put into this object
     * @param msg message to be sent to all hosts
     * @return if replies reach majority
     */
    public boolean initSendToAll(HostManager hostManager, TCP_ReplyMsg_All tcp_ReplyMsg_All, MessageHandler msg) {
        boolean local_DEBUG = true;
        boolean DEBUG = global_DEBUG ? (local_DEBUG) : global_DEBUG;
        if (DEBUG) System.out.println("[" + commOwner + "] From communicator: enter initSendToAll()");
        for (Map.Entry<String, HostAddress> a : hostManager.getHostList().entrySet()) {
            HostAddress targetHost = a.getValue();
            // tkuo: doesn't need to connect and send msg to itself
            if (targetHost.getHostIp().equals(commOwner)) continue;
            // tkuo: if RPC is FORWARD, don't need to connect and forward to leader
            if (hostManager.getLeaderAddress() != null && msg.getMessageType().equals(RPCs.FORWARD)) {
                if (targetHost.getHostIp().equals(hostManager.getLeaderAddress().getHostIp())) continue;
            }

            TCP_Worker worker = new TCP_Worker(targetHost, tcp_ReplyMsg_All, msg, CommType.sentToAll);
            worker.start();
            if (DEBUG) System.out.println("[" + commOwner + "] From communicator: worker started with " +
                                          targetHost.getHostName() + ", " + targetHost.getHostIp());
        }
        try {
            // TODO
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (DEBUG) System.out.println("From communicator: 500 ms timeout, return from initSendToAll()");
        if (DEBUG) System.out.println("[Info] # of replied host = " + tcp_ReplyMsg_All.getList_repliedHost().size());
        // TODO
        //return tcp_ReplyMsg_All.getList_repliedHost().size() >= hostManager.getHostList().size() / 2 + 1;
        return tcp_ReplyMsg_All.getList_repliedHost().size() >= hostManager.getHostList().size() / 2;
    }

    public boolean initSendToAll(HostManager hostManager, TCP_ReplyMsg_All tcp_ReplyMsg_All, MessageHandler msg, int time) {
        boolean local_DEBUG = true;
        boolean DEBUG = global_DEBUG ? (local_DEBUG) : global_DEBUG;
        if (DEBUG) System.out.println("From communicator: enter initSendToAll()");
        for (Map.Entry<String, HostAddress> a : hostManager.getHostList().entrySet()) {
            HostAddress targetHost = a.getValue();
            // tkuo: doesn't need to send itself
            if (targetHost.getHostIp().equals(commOwner)) continue;
            // tkuo: if RPC is FORWARD, don't need to connect and forward to leader
            if (hostManager.getLeaderAddress() != null && msg.getMessageType().equals(RPCs.FORWARD)) {
                if (targetHost.getHostIp().equals(hostManager.getLeaderAddress().getHostIp())) continue;
            }

            TCP_Worker worker = new TCP_Worker(targetHost, tcp_ReplyMsg_All, msg, CommType.sentToAll);
            worker.start();
            if (DEBUG) System.out.println("From communicator: worker started with " +
                                          targetHost.getHostName() + ", " + targetHost.getHostIp());
        }
        try {
            if (time > 0){
                Thread.sleep(time);
            }
            //Thread.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (DEBUG) System.out.println("From communicator: 200 ms timeout, return from initSendToAll()");
        return tcp_ReplyMsg_All.getList_repliedHost().size() >= hostManager.getHostList().size() / 2 + 1;
    }

    /**
     * Send message to a specific host. No check of message content in this method.
     * @param targetHost host address
     * @param tcp_ReplyMsg_One reply message will be put into this object
     * @param msg message to be sent to target host
     * @return true if there is message; false if no reply or target node fails
     */
    public boolean initSendToOne(HostAddress targetHost, TCP_ReplyMsg_One tcp_ReplyMsg_One, MessageHandler msg) {
        boolean local_DEBUG = true;
        boolean DEBUG = global_DEBUG ? (local_DEBUG) : global_DEBUG;
        if (DEBUG) System.out.println("From communicator: enter initSendToOne()");

        TCP_Worker worker = new TCP_Worker(targetHost, tcp_ReplyMsg_One, msg, CommType.sentToOne);
        worker.start();
        if (DEBUG) System.out.println("From communicator: worker started with " +
                                      targetHost.getHostName() + ", " + targetHost.getHostIp());
        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (DEBUG) System.out.println("From communicator: 200 ms timeout, return from initSendToOne()");
        return tcp_ReplyMsg_One.getMessage() != null;
    }

    /**
     * Got a connected socket, use this method to get the message it sends.
     * @param pkg a wrapper object to maintain one round-trip communication
     * @return received message
     */
    public MessageHandler receiveFromOne(OnewayCommunicationPackage pkg) {
        return pkg.receiveFromOne();
    }

    /**
     * Send reply to whoever sends you a message before.
     * @param pkg a wrapper object to maintain one round-trip communication, should be the same with the one used in receiveFromOne()
     * @param msg message you want to send
     * @return true if message send successfully
     */
    public boolean replyToOne(OnewayCommunicationPackage pkg, MessageHandler msg) {
        return pkg.replyToOne(msg);
    }
}
