package Communicator;

import definedtype.CommType;
import host.HostAddress;
import message.MessageHandler;

import java.io.Serializable;
import java.io.IOException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.security.interfaces.RSAPublicKey;

public class TCP_Worker extends Thread implements Serializable {
    private Socket clientSocket;
    private HostAddress target;
    private TCP_ReplyMsg_All tcp_ReplyMsg_All;
    private TCP_ReplyMsg_One tcp_ReplyMsg_One;
    private String jobType;
    private MessageHandler msg;
    private ObjectInputStream in;
    private ObjectOutputStream out;

    private boolean global_DEBUG = false;

    public TCP_Worker(HostAddress target, TCP_ReplyMsg_All tcp_ReplyMsg_All, MessageHandler msg, String jobType) {
        this.target = target;
        this.tcp_ReplyMsg_All = tcp_ReplyMsg_All;
        this.msg = msg;
        this.jobType = jobType;
    }

    public TCP_Worker(HostAddress target, TCP_ReplyMsg_One tcp_replyMsg_One, MessageHandler msg, String jobType) {
        this.target = target;
        this.tcp_ReplyMsg_One = tcp_replyMsg_One;
        this.msg = msg;
        this.jobType = jobType;
    }

    public TCP_Worker(Socket clientSocket, TCP_ReplyMsg_One tcp_replyMsg_One, String jobType) {
        this.clientSocket = clientSocket;
        this.tcp_ReplyMsg_One = tcp_replyMsg_One;
        this.jobType = jobType;
    }

    /**
     * Start running this thread
     */
    public void run() {
        boolean local_DEBUG = true;
        boolean DEBUG = global_DEBUG ? (local_DEBUG) : global_DEBUG;
        if (DEBUG) System.out.println("From TCP_worker: enter run()");

        openConnection();

        try {
            // Jiff: check sockect is connected and in/outstream is built
            if (out != null && in != null) {
                handleRequest();
            }
        } catch (IOException | ClassNotFoundException e) {
            if (DEBUG)
                System.out.println("\nERROR!!! IOexception caught @ handleRequest()\n");
                e.printStackTrace();
                //TODO
                if (DEBUG) closeConnection();
                if (DEBUG) System.exit(0);
            if (tcp_ReplyMsg_All != null)
                this.tcp_ReplyMsg_All.addFailedNode(this.target); // if connection fail, record the failed node
        }
    }

    /**
     * Open connection with target host, if no connection build yet. If already connected, use connected socket.
     */
    public void openConnection() {
        boolean local_DEBUG = true;
        boolean DEBUG = global_DEBUG ? (local_DEBUG) : global_DEBUG;
        if (DEBUG) System.out.println("From TCP_worker: enter openConnection()");

        try {
            // receiveMsg doesn't need to create socket becasue Host.java does it
            if (this.jobType.equals(CommType.sentToOne) || this.jobType.equals(CommType.sentToAll)) {
                clientSocket = new Socket(target.getHostIp(), target.getHostPort());
            }
            if (DEBUG) System.out.println("From TCP_worker: socket " + clientSocket.getInetAddress() + " is created ");
            //out = new ObjectOutputStream(new BufferedOutputStream(clientSocket.getOutputStream()));
            out = new ObjectOutputStream(clientSocket.getOutputStream());
            in = new ObjectInputStream(new BufferedInputStream(clientSocket.getInputStream()));
        } catch (IOException e) {
            if (DEBUG) System.out.println("Warning!!! Cannot open connection : " + target.getHostIp());
            if (tcp_ReplyMsg_All != null)
                this.tcp_ReplyMsg_All.addFailedNode(this.target); // if connection fail, record the failed node
        }
        if (DEBUG) System.out.println("From TCP_worker: return from openConnection()");
    }

    public void handleRequest() throws IOException, ClassNotFoundException {
        boolean local_DEBUG = true;
        boolean DEBUG = global_DEBUG ? (local_DEBUG) : global_DEBUG;
        if (DEBUG) System.out.println("From TCP_worker: enter handleRequest()");

        // sent to one, round trip
        if (this.jobType.equals(CommType.sentToOne)) {
            if (DEBUG) System.out.println("From TCP_worker: jobType is sentToOne = " + this.jobType);
            out.writeObject(msg);
            out.flush();
            if (DEBUG) System.out.println("From TCP_worker: msg is sent ");

            // out.flush();
            try {
                MessageHandler replyMsg = (MessageHandler) in.readObject();
                if (DEBUG) System.out.println("From TCP_worker: reply is received ");
                if (replyMsg.getMessageType().equals(this.msg.getMessageType())) {
                    this.tcp_ReplyMsg_One.setMessage(replyMsg);
                    if (DEBUG) System.out.println("From TCP_worker: reply message type match! ");

                }
            } catch (Exception e) {
                System.out.println("[Warning]: This host cannot read object in handleRequest() ...");
                System.out.println("[Warning]: Probabaly " + clientSocket.getInetAddress()  + " host is not online!!");
            }
            closeConnection();
            if (DEBUG) System.out.println("From TCP_worker: return from handleRequest()");
            return;
        }

        // sent to all, round trip
        if (this.jobType.equals(CommType.sentToAll)){
            if (DEBUG) System.out.println("From TCP_worker: jobType is sentToAll = " + this.jobType);
            out.writeObject(msg);
            out.flush();
            if (DEBUG) System.out.println("From TCP_worker: msg is sent ");
            //  out.flush();
            try {
                MessageHandler replyMsg = (MessageHandler) in.readObject();
                if (DEBUG) System.out.println("From TCP_worker: reply is received ");
                if (replyMsg.getMessageType().equals(this.msg.getMessageType())) {
                    if (DEBUG) System.out.println("From TCP_worker: reply message type match ");
                    if (replyMsg.getMessageContent().equals("Yes")) {
                        if (DEBUG) System.out.println("From TCP_worker: reply message is \"Yes\" ");
                        this.tcp_ReplyMsg_All.addRepliedNode(this.target);
                    } else {
                        if (DEBUG) System.out.println("From TCP_worker: reply message is \"No\" or other");
                    }
                }
            } catch (Exception e) {
                System.out.println("[Warning]: This host cannot read object in handleRequest() ...");
                System.out.println("[Warning]: Probabaly " + clientSocket.getInetAddress()  + " host is not online!!");
            }
            closeConnection();
            if (DEBUG) System.out.println("From TCP_worker: return from handleRequest()");
            return;
        }

        // receive from one specified client socket, one trip
        if (this.jobType.equals(CommType.receiveFromOne)) {
            if (DEBUG) System.out.println("From TCP_worker: jobType is receiveFromOne = " + this.jobType);

            MessageHandler replyMsg = (MessageHandler)in.readObject();
            if (DEBUG) System.out.println("From TCP_worker: message is received ");

            this.tcp_ReplyMsg_One.setMessage(replyMsg);

            synchronized (this.tcp_ReplyMsg_One) {
                this.tcp_ReplyMsg_One.notifyAll();
                if (DEBUG) System.out.println("From TCP_worker: notify that message is received ");

            }

            // wait for jobType to be changed to replyToOne, then send reply, one trip
            synchronized (this.tcp_ReplyMsg_One) {
                while (this.msg == null) {
                    if (DEBUG) System.out.println("From TCP_worker: no message to be sent yet, waiting...");
                    try {
                        this.tcp_ReplyMsg_One.wait();
                        if (DEBUG) System.out.println("From TCP_worker: got notify job type is changed ! ");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            // reply to one specified client socket, one trip
            if (this.jobType.equals(CommType.replyToOne)) {
                if (DEBUG) System.out.println("From TCP_worker: job type is changed to " + this.jobType);
                out.writeObject(msg);
                out.flush();
                if (DEBUG) System.out.println("From TCP_worker: message is sent ");
                this.tcp_ReplyMsg_One.setMessage(new MessageHandler("", "sent")); // only indicate sent
            }

            synchronized(this.tcp_ReplyMsg_One) {
                this.tcp_ReplyMsg_One.notifyAll();
                if (DEBUG) System.out.println("From TCP_worker: notify message is sent ");
            }

            closeConnection();
            if (DEBUG) System.out.println("From TCP_worker: return from handleRequest()");
            return;
        }
    }

    //public void closeConnection() throws IOException {
    public void closeConnection() {
        boolean local_DEBUG = true;
        boolean DEBUG = global_DEBUG? (local_DEBUG): global_DEBUG;
        if (DEBUG) System.out.println("From TCP_worker: enter closeConnection()");

        try {
            if (in != null) {
                in.close();
            }
            if (out != null) {
                out.close();
            }
            this.clientSocket.close();
        } catch (IOException e) {
            System.out.println("Warning: In/Output Object cannot closed");
        }

        if (DEBUG) System.out.println("From TCP_worker: close successfully()");
    }

    public String getCommType() {
        return jobType;
    }

    public void setCommType(String jobType) {
        this.jobType = jobType;
    }

    public MessageHandler getMsg() {
        return msg;
    }

    public void setMsg(MessageHandler msg) {
        this.msg = msg;
    }

    public TCP_ReplyMsg_One getTcp_ReplyMsg_One() {
        return tcp_ReplyMsg_One;
    }

    public void setTcp_ReplyMsg_One(TCP_ReplyMsg_One tcp_ReplyMsg_One) {
        this.tcp_ReplyMsg_One = tcp_ReplyMsg_One;
    }
}
