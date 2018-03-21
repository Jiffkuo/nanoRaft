package host;

import definedtype.RoleType;
import definedtype.RPCs;
import definedtype.Protocol;
import Communicator.OnewayCommunicationPackage;
import Communicator.TCP_Communicator;
import Communicator.TCP_ReplyMsg_All;
import message.MessageHandler;
import leaderelection.Node;

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.BufferedOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.logging.LogManager;

public class Host extends Thread implements Observer{
    private String hostName;
    private StateManager stateManager;
    private Integer currentTerm;
    private int charactor;
    private Leader leader;
    private Follower follower;
    private Candidate candidate;
    private HostAddress votedFor;
    private int commitIndex;
    //need to store public keys of other hosts
    private ServerSocket aServer;
    private HostManager hostManager;
    // store my name, my ip, my port
    private HostAddress myAddress;
    private Thread followerThread;
    private Integer votedTerm;
    private ForwardCollector fCollector;
    private ArrayList<HostAddress> serverInfos; // save all members
    private int serverNums;
    private Node node; // for leaderelection usage
    private Map<String, InetSocketAddress> nodeMap; // for leaderelection usage
    private boolean _forceLeader; // for debugging purpose
    private boolean _enableHost;  // for debugging purpose
    private boolean _isLeader;
    private boolean DEBUG = false;

    public Host(int force) throws IOException {
        // read config file to get cluster IP information
        nodeMap = new HashMap<>();
        readConfig();
        // for resume/recovery host purpose
        if (!isFirstUsage()) {
           _enableHost = true;
        }
        hostName = InetAddress.getLocalHost().getHostAddress();
        stateManager = new StateManager(new String[]{"x", "y", "z"}, hostName, readHostList());
        commitIndex = stateManager.getLastIndex();
        currentTerm = 0;
        votedTerm = 0;
        charactor = RoleType.FOLLOWER;
        follower = new Follower(stateManager);
        follower.addObserver(this);
        candidate = new Candidate(this);
        candidate.addObserver(this);
        leader = new Leader(this, candidate.get_tcp_ReplyMsg_All());
        leader.addObserver(this);
        _isLeader = false;
        // tkuo: Test Purpose
        //_forceLeader = force == 1 ? true : false;
        //_enableHost = force == 2 ? true : false;
    }

    @Override
    public void run() {
        // TODO: for debugging purpose only by tkuo
        followerThread = new Thread(follower);
        if (_enableHost || _forceLeader) {
            followerThread.setDaemon(true);
            _isLeader = false;
            followerThread.start();
        }

        RequestResponse aRequest;
        while (true){
            try {
                aRequest = new RequestResponse(aServer.accept(), -1, null, DEBUG);
                aRequest.start();
                //aRequest.stop();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void update(Observable o, Object arg) {
        // TODO: test _enableHost can work
        //if (!_forceLeader) return;
        //if (!_enableHost) return;
        switch ((int)arg){
            case RoleType.F2C:
                System.out.println("Change from follower to Candidate.");
                currentTerm++;
                follower.leave();
                charactor = RoleType.CANDIDATE;
                Thread candidateThread = new Thread(candidate);
                candidateThread.setDaemon(true);
                candidateThread.start();
                _isLeader = false;
                break;
            case RoleType.C2L:
                System.out.println("Change from Candidate to Leader.");
                candidate.leave();
                charactor = RoleType.LEADER;
                //leader should take care of non-up-to-date followers.
                leader = new Leader(this, candidate.get_tcp_ReplyMsg_All());
                leader.addObserver(this);
                Thread leaderThread = new Thread(leader);
                leaderThread.setDaemon(true);
                leaderThread.start();
                _isLeader = true;
                break;
            case RoleType.C2F:
                // This only happens at the receiver side ...
                // When new leader is elected ...
                break;
            case RoleType.L2F:
                // This only happens at the receiver side ...
                // new leader wins the term
                break;
            default:
                System.out.println("Something wrong.");
                break;
        }
    }

    public Integer getCurrentTerm(){
        return currentTerm;
    }

    public void setCurrentTerm(int term){
        currentTerm = term;
    }

    public HostManager getHostManager(){
        return hostManager;
    }

    public String getHostName(){
        return hostName;
    }

    public StateManager getStateManager(){
        return stateManager;
    }

    public int getCommitIndex(){
        return commitIndex;
    }

    public void setCommitIndex(int index){
        commitIndex = index;
    }

    class RequestResponse extends Thread {
        private Socket aSocket;
        private ObjectOutputStream oOut;
        private ObjectInputStream oIn;
        private PrintWriter pOut;
        private BufferedReader bIn;
        private int command;
        private Object parameter;
        private boolean RPCFlag;
        private boolean DEBUG;

        RequestResponse(Socket aSocket, int command, Object parameter, boolean _debug) {
            this.aSocket = aSocket;
            this.parameter = parameter;
            this.command = command; // if command is -1, receive message first
            RPCFlag = true;
            DEBUG = _debug;
        }

        RequestResponse(HostAddress hostAddress, int command, Object parameter, boolean _debug) throws IOException {
            this.command = command;
            this.parameter = parameter;
            aSocket = new Socket(hostAddress.getHostIp(), hostAddress.getHostPort());
            RPCFlag = false;
            DEBUG = _debug;
        }

        private void closeAll() throws IOException {
           if (pOut != null) {
               pOut.close();
           }
           if (oOut != null) {
               oOut.close();
           }
           if (oIn != null) {
               oIn.close();
           }
           if (!aSocket.isClosed()) {
               aSocket.close();
           }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void run() {
            try {
                // if host address in HostList, use PRC to do communication else use TCP protocol
                if (RPCFlag && hostManager.isInHostList(aSocket.getInetAddress().getHostAddress()) && _enableHost) {
                    // host doesn't do RPC communication with itself
                    if (!aSocket.getInetAddress().getHostAddress().equals(myAddress.getHostIp())) {
                        command = Protocol.RPCREQUEST;
                    }
                }
                else{
                    //TODO: can only use one new function Object or Print
                    //oOut = new ObjectOutputStream(new BufferedOutputStream(aSocket.getOutputStream()));
                    //oIn = new ObjectInputStream(aSocket.getInputStream());
                    pOut = new PrintWriter(aSocket.getOutputStream(), true);
                    bIn = new BufferedReader(new InputStreamReader(aSocket.getInputStream()));
                    if (command == -1) {
                        String temp = bIn.readLine();
                        // check if command is integer
                        char[] ch = temp.toCharArray();
                        for (int k = 0; k < ch.length; k++) {
                            if (Character.digit(ch[k], 10) < 0) {
                                closeAll();
                                return;
                            }
                        }
                        command = Integer.parseInt(temp);
                        //command = oIn.readInt();
                    }
                }
                if (DEBUG) System.out.println("[HOST] Command: " + command); // tkuo
                switch (command) {
                    //TODO: can only use one new function Object or Print
                    case Protocol.ASKHOSTNAME:
                        oOut.writeInt(Protocol.REPLYHOSTNAME);
                        oOut.flush();
                        HostAddress temp = (HostAddress)(parameter);
                        temp.setHostName((String)oIn.readObject());
                        oOut.writeInt(Protocol.ACKOWLEDGEMENT);
                        oOut.flush();
                        hostManager.addHostToList(temp);
                        break;

                    case Protocol.REPLYHOSTNAME:
                        oOut.writeObject(hostName);
                        oOut.flush();
                        if (oIn.readInt() != Protocol.ACKOWLEDGEMENT){
                            System.out.println("Error happened when sending host name");
                        }
                        break;

                    case Protocol.UPDATEHOSTLIST:
                        oOut.writeInt(Protocol.REPLYHOSTLIST);
                        oOut.flush();
                        oOut.writeObject(hostManager.getHostList());
                        oOut.flush();
                        if(oIn.readInt() != Protocol.ACKOWLEDGEMENT){
                            System.out.println("Error happened when updating host list");
                        }
                        break;

                    case Protocol.REPLYHOSTLIST:
                        hostManager.replaceHostList((HashMap<String ,HostAddress>)oIn.readObject());
                        oOut.writeInt(Protocol.ACKOWLEDGEMENT);
                        oOut.flush();
                        System.out.println(hostManager);
                        //followerThread = new Thread( follower );
                        //followerThread.setDaemon(true);
                        //followerThread.start();
                        break;

                    case Protocol.REQUESTLEADERADDRESS:
                        if (_isLeader) {
                            System.out.println("[HOST] request leader address, leader");
                            // reply 0 if is leader
                            pOut.write("1\n");
                            pOut.flush();
                        } else {
                            System.out.println("[HOST] request leader address, not leader");
                            // reply list of hosts
                            for (Map.Entry<String, HostAddress> a : hostManager.getHostList().entrySet()) {
                                HostAddress targetHost = a.getValue();
                                if (targetHost.getHostIp().equals(myAddress.getHostIp())) continue;
                                pOut.write(targetHost.getHostIp()+"\n");
                                pOut.flush();
                            }
                        }
                        break;
                    
                    case Protocol.CHANGEVALUE:
                        String stateName = bIn.readLine();
                        System.out.println("stateName: " + stateName);
                        int newValue = Integer.parseInt(bIn.readLine());
                        System.out.println("value: " + newValue);
                        pOut.write(Protocol.ACKOWLEDGEMENT);
                        pOut.flush();
                        if (_isLeader && leader != null && leader.addState(new host.State(stateName, newValue))) {
                            pOut.write("Yes");
                            if (DEBUG) System.out.println("[HOST] ack Yes to client");
                        }
                        else {
                            pOut.write("No");
                            if (DEBUG) System.out.println("[HOST] ack No to client");
                        }
                        pOut.flush();
                        break;

                    // added by wt
                    // return latest data to client
                    case Protocol.GETVALUE:
                        // Get state name
                        String stateNameToGet = bIn.readLine();
                        System.out.println("stateName: " + stateNameToGet);

                        // Look for value of stateName in Log file
                        String value = getValueFromLog(stateNameToGet);

                        // Reply to client
                        //PrintWriter outputValue = new PrintWriter(aSocket.getOutputStream(), true);
                        pOut.write(Protocol.ACKOWLEDGEMENT);
                        pOut.flush();
                        pOut.write(value);
                        pOut.flush();
                        break;

                    // added by tkuo
                    // To invoke other hosts to start running follower thread at beginning
                    case Protocol.ENABLEHOST:
                        if (!_enableHost) {
                            _enableHost = true;
                            // broadcast to other host to invoke
                            if (!hostManager.isInHostList(aSocket.getInetAddress().getHostAddress())) {
                                for (Map.Entry<String, HostAddress> a : hostManager.getHostList().entrySet()) {
                                    HostAddress targetHost = a.getValue();
                                    // doesn't need to connect and send msg to itself
                                    if (targetHost.getHostIp().equals(hostName)) continue;
                                    Socket tmpSocket = new Socket(targetHost.getHostIp(), targetHost.getHostPort());
                                    PrintWriter tmpOut = new PrintWriter(tmpSocket.getOutputStream(), true);
                                    tmpOut.write(Integer.toString(Protocol.ENABLEHOST) + "\n");
                                    tmpOut.flush();
                                    tmpOut.close();
                                    tmpSocket.close();
                                }
                                System.out.println("[HOST] All host are invoked, you can use it right now");
                            }
                            // using leaderelection
                            System.out.println("[HOST] leaderelection is launched!");
                            long start_time = System.currentTimeMillis();
                            Node node = new Node(myAddress.getHostName(), new InetSocketAddress(myAddress.getHostIp(), myAddress.getHostPort()+1), nodeMap);
                            try {
                                node.startUntilLeaderElected();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            long end_time = System.currentTimeMillis();
                            long diff_time = end_time - start_time;
                            System.out.println("[HOST] LeaderElection consumes " + diff_time + " ms");
                            String leaderNode = node.getStateManager().getLastKnownLeader();
                            if (myAddress.getHostName().equals(leaderNode)) {
                                charactor = RoleType.LEADER;
                                //leader should take care of non-up-to-date followers.
                                Thread leaderThread = new Thread(leader);
                                leaderThread.setDaemon(true);
                                leaderThread.start();
                                _isLeader = true;
                            } else {
                                followerThread.setDaemon(true);
                                if(!followerThread.isAlive()) {
                                    followerThread.start();
                                }
                                _isLeader = false;
                            }
                            // get leader nodeid
                            pOut.write("OK\n");
                            pOut.flush();
                        } else {
                            System.out.println("[HOST] Already running, don't need to enable host");
                        } 
                        break;

                    case Protocol.RPCREQUEST:
                        TCP_Communicator tempTCP = new TCP_Communicator(hostName);
                        OnewayCommunicationPackage onewayCommunicationPackage = new OnewayCommunicationPackage(aSocket);
                        MessageHandler receivedMSG = tempTCP.receiveFromOne(onewayCommunicationPackage);
                        String RPC = receivedMSG.getMessageType();
                        HostAddress requestHost = hostManager.getHostAddress(aSocket.getInetAddress().getHostName());
                        String planText = receivedMSG.getMessageContent();
                        String[] aurgments = new String[4];
                        if (planText != null) {
                            if (DEBUG) System.out.println("[HOST] planText is " + planText);
                            aurgments = planText.split(",");
                        }
                        if (DEBUG) System.out.println("[HOST] RPC: " + RPC); // tkuo
                        switch (RPC) {
                            case RPCs.REQUESTVOTE:
                                follower.receivedHeartBeat();
                                int candidateTerm = Integer.parseInt(aurgments[0]);
                                int lastLogIndex = Integer.parseInt(aurgments[2]);
                                int lastLogTerm = Integer.parseInt(aurgments[3]);
                                synchronized (votedTerm) {
                                    if (currentTerm < candidateTerm && charactor != RoleType.FOLLOWER) {
                                        if (leader != null) {
                                            leader.leave();
                                        }
                                        candidate.leave();
                                        charactor = RoleType.FOLLOWER;
                                        System.out.println("3");
                                        followerThread = new Thread( follower );
                                        followerThread.setDaemon(true);
                                        followerThread.start();
                                        _isLeader = false;
                                    }
                                    if (votedTerm < candidateTerm && currentTerm <= candidateTerm
                                        && stateManager.getLastIndex() <= lastLogIndex
                                        && stateManager.getLastLog().getTerm() <= lastLogTerm) {
                                        votedTerm = candidateTerm;
                                        currentTerm = candidateTerm;
                                        tempTCP.replyToOne(onewayCommunicationPackage, new MessageHandler(RPCs.REQUESTVOTE, "Yes"));
                                    }
                                }
                                break;

                            case RPCs.APPENDENTRY:
                                if (planText == null) {
                                    System.out.println("invalid key!");
                                    break;
                                }

                                if (!aurgments[4].equals("")) { //if not heartbeat, forward
                                    receivedMSG.setMessageType(RPCs.FORWARD);
                                    TCP_Communicator newTCP = new TCP_Communicator(hostName);
                                    newTCP.initSendToAll(hostManager, new TCP_ReplyMsg_All(), receivedMSG, -1);
                                    Thread.sleep(100);
                                    planText = fCollector.getResult();
                                    if (DEBUG) System.out.println("[HOST] planText = " + planText); 
                                    if (planText == null) {
                                        tempTCP.replyToOne(onewayCommunicationPackage, new MessageHandler(RPCs.APPENDENTRY, RPCs.BFAIL));
                                        break;
                                    }
                                    aurgments = planText.split(",");

                                }

                                follower.receivedHeartBeat();
                                int leaderTerm = Integer.parseInt(aurgments[0]);
                                String leaderName = aurgments[1];
                                int preLogIndex = Integer.parseInt(aurgments[2]);
                                int preLogTerm = Integer.parseInt(aurgments[3]);
                                String newState = aurgments[4];
                                int leaderCommit = Integer.parseInt(aurgments[5]);

                                if (DEBUG) System.out.println("[HOST] currentTerm = " + currentTerm + " vs leaderTerm = " + leaderTerm);
                                if (currentTerm <= leaderTerm) {
                                    if (charactor != RoleType.FOLLOWER) {
                                        charactor = RoleType.FOLLOWER;
                                        candidate.leave();
                                        System.out.println("4");
                                        followerThread = new Thread( follower );
                                        followerThread.setDaemon(true);
                                        followerThread.start();
                                        _isLeader = false;
                                    }
                                    hostManager.setLeaderAddress(leaderName);
                                    currentTerm = leaderTerm;
                                    follower.receivedHeartBeat();

                                    if (stateManager.getLastIndex() >= preLogIndex
                                        && stateManager.getLog(preLogIndex).getTerm() == preLogTerm) {
                                        if (!newState.equals("")) {
                                            String[] stateParameter = newState.split("->");
                                            host.State tempState = new host.State(stateParameter[0], Integer.parseInt(stateParameter[1]));
                                            follower.appendAnEntry(tempState, currentTerm);
                                        }
                                        while (commitIndex < leaderCommit) {
                                            stateManager.commitEntry(++commitIndex);
                                        }
                                        tempTCP.replyToOne(onewayCommunicationPackage, new MessageHandler(RPCs.APPENDENTRY, RPCs.SUCCESS));
                                        if (DEBUG) System.out.println("[HOST]: " + stateManager.toString()); // tkuo
                                        follower.receivedHeartBeat();
                                    } else {
                                        tempTCP.replyToOne(onewayCommunicationPackage, new MessageHandler(RPCs.APPENDENTRY, RPCs.FAIL));
                                        follower.receivedHeartBeat();
                                    }
                                } else {
                                    if (DEBUG) System.out.println("[HOST] Doesn't append entry");
                                }

                                break;

                            case RPCs.FORWARD:
                                if (charactor != RoleType.LEADER) { //leader ignore forward msg
                                    fCollector.putIntoCollection(receivedMSG);
                                    if (DEBUG) System.out.println("[Host->forward] msg = " + receivedMSG.getMessageContent());
                                }
                                tempTCP.replyToOne(onewayCommunicationPackage, new MessageHandler(RPCs.FORWARD, "Yes"));
                                break;
                        } // end of case (RPC)
                        break;
                }

                if (pOut != null) {
                    pOut.close();
                }
                if (oOut != null) {
                    oOut.close();
                }
                if (oIn != null) {
                    oIn.close();
                }
                if (!aSocket.isClosed()) {
                    aSocket.close();
                }
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e1) {
                e1.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    static public void main(String args[]) {
        int forceLeader = 0;
        // turn off logMessage
        LogManager.getLogManager().reset();
        try {
            if (args.length > 0) {
               forceLeader = Integer.parseInt(args[0]);
               System.out.println("[Test] force the host to be special role");
            }
            Host aHost = new Host(forceLeader);
            aHost.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void readConfig() throws IOException {
        // created by wen-chieh
        ArrayList<HostAddress> result = new ArrayList<>();

        // The name of the file to open.
        String fileName = "./config.txt";
        // This will reference one line at a time
        String line = null;

        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = new FileReader(fileName);
            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            while((line = bufferedReader.readLine()) != null) {
                if (DEBUG) System.out.println(line);
                // parse line here
                String[] nameNipNport = line.split(",");
                result.add(new HostAddress(nameNipNport[0], nameNipNport[1], Integer.parseInt(nameNipNport[2])));
            }
            // Always close files.
            bufferedReader.close();
        } catch(FileNotFoundException ex) {
            System.out.println("Unable to open file '" + fileName + "'");
        } catch(IOException ex) {
            System.out.println("Error reading file '" + fileName + "'");
            // Or we could just do this:
            // ex.printStackTrace();
        }

        for (HostAddress ha : result) {
            if (ha.getHostIp().equals(InetAddress.getLocalHost().getHostAddress())) {
                aServer = new ServerSocket(ha.getHostPort());
                System.out.println(ha.getHostIp() + " at port number: " + aServer.getLocalPort());
                myAddress = new HostAddress(ha.getHostName(), ha.getHostIp(), ha.getHostPort());
                hostManager = new HostManager(myAddress);
                fCollector = new ForwardCollector(hostManager);
            } else {
                // for leaderelection usage
                // in order to avoid conflict, leaderlection port number = original port number + 1
                InetSocketAddress nodeAddress = new InetSocketAddress(ha.getHostIp(), ha.getHostPort()+1);
                nodeMap.put(ha.getHostName(), nodeAddress);
                continue;
            }
        }

        for (HostAddress ha : result) {
            if (ha.getHostIp().equals(myAddress.getHostIp())) {
                continue;
            } else {
                hostManager.addHostToList(ha);
            }
        }
    }

    private boolean isFirstUsage() {
        // The name of the file to open.
        String fileName = String.format("./%s/logFile.txt", myAddress.getHostIp());
        File f = new File(fileName);
        if(f.exists()) {
           System.out.println("[HOST] Warning: Hots is not first time to be executed");
           return false;
        } else {
           return true;
        }
    }

    private String getValueFromLog(String stateName) throws IOException {
        // created by wen-chieh
        // stateName is the Variable we are looking for
        String value = "";
        // The name of the file to open.
        String fileName = String.format("./%s/logFile.txt", myAddress.getHostIp());
        // This will reference one line at a time
        String line = null;

        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = new FileReader(fileName);
            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            while((line = bufferedReader.readLine()) != null) {
                if (DEBUG) System.out.println(line);
                // Parse line here
                String[] indexTermVariableValue = line.split("\t");
                // update value
                if (indexTermVariableValue[2].equals(stateName)) {
                    value = indexTermVariableValue[3];
                }
            }
            // Always close files.
            bufferedReader.close();
        } catch(FileNotFoundException ex) {
            System.out.println("Unable to open file '" + fileName + "'");
        } catch(IOException ex) {
            System.out.println("Error reading file '" + fileName + "'");
            // Or we could just do this:
            // ex.printStackTrace();
        }
        return value;
    }

    private ArrayList<String> readHostList() throws IOException {
        // created by wen-chieh
        ArrayList<String> result = new ArrayList<>();

        // The name of the file to open.
        String fileName = "./config.txt";
        // This will reference one line at a time
        String line = null;

        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = new FileReader(fileName);
            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            while((line = bufferedReader.readLine()) != null) {
                if (DEBUG) System.out.println(line);
                // parse line here
                String[] nameNipNport = line.split(",");
                result.add(nameNipNport[1]);
            }
            // Always close files.
            bufferedReader.close();
        } catch(FileNotFoundException ex) {
            System.out.println("Unable to open file '" + fileName + "'");
        } catch(IOException ex) {
            System.out.println("Error reading file '" + fileName + "'");
            // Or we could just do this:
            // ex.printStackTrace();
        }

        return result;
    }
}
