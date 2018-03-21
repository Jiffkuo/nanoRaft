package host;

import definedtype.LeaderJobs;
import definedtype.RPCs;
import Communicator.TCP_Communicator;
import Communicator.TCP_ReplyMsg_One;
import message.MessageHandler;

import javax.annotation.processing.SupportedSourceVersion;
import java.security.interfaces.RSAPublicKey;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class Leader_Worker implements Runnable {

    private String _hostName;
    private int _leaderJob;
    private Leader _leader;
    private Host _host;
    private boolean DEBUG = false;

    public Leader_Worker(Leader leader, int leaderJob, String hostName, Host host){
        _leader = leader;
        _leaderJob = leaderJob;
        _hostName = hostName;
        _host = host;
    }

    @Override
    public void run() {
        MessageHandler signedMessage = null;
        TCP_Communicator tcp_communicator = new TCP_Communicator(_hostName);
        TCP_ReplyMsg_One tcp_replyMsg_one = new TCP_ReplyMsg_One();
        int index;
        LogEntry prelogEntry;
        LogEntry curlogEntry;
        String appendEntry;
        boolean result;
        String msg;

        String[] appendEntryArray = new String[]{
            _host.getCurrentTerm().toString(),
            _host.getHostName(),
            String.valueOf(_host.getStateManager().getLastLog().getIndex()),
            String.valueOf(_host.getStateManager().getLastLog().getTerm()),
            "",
            String.valueOf(_host.getCommitIndex())
        };

        switch (_leaderJob){
            case LeaderJobs.FINDINDEX:
                if (DEBUG){
                    System.out.println("LeaderJobs.FINDINDEX");
                }
                index = _leader.get_nextIndex().get(_hostName);
                prelogEntry = _leader.get_host().getStateManager().getLog(index-1);
                appendEntryArray[2] = String.valueOf(prelogEntry.getIndex());
                appendEntryArray[3] = String.valueOf(prelogEntry.getTerm());
                appendEntry = String.join(",", appendEntryArray);
                signedMessage = new MessageHandler(RPCs.APPENDENTRY, appendEntry);
                result = tcp_communicator.initSendToOne(_leader.get_host().getHostManager().getHostAddress(_hostName),
                                                        tcp_replyMsg_one, signedMessage);
                if(result) {
                    msg = tcp_replyMsg_one.getMessage().getMessageContent();
                    if (msg.equals(RPCs.SUCCESS)) {
                        if (DEBUG){
                            System.out.println("RPCs.SUCCESS 1");
                        }
                        _leader.get_isFindNextIndex().add(_hostName);
                    } else if(msg.equals(RPCs.FAIL)) {
                        if (DEBUG){
                            System.out.println("RPCs.FAIL 1");
                        }
                        int nextIndex = _leader.get_nextIndex().get(_hostName);
                        _leader.get_nextIndex().put(_hostName, nextIndex - 1);
                    } else {
                        if (DEBUG){
                            System.out.println("Some error 1");
                        }
                    }
                }else {
                    if (DEBUG){
                        System.out.println("Result False 1");
                    }
                }
                break;
            case LeaderJobs.KEEPUPLOG:
                if (DEBUG){
                    System.out.println("LeaderJobs.KEEPUPLOG");
                }
                index = _leader.get_nextIndex().get(_hostName);
                prelogEntry = _leader.get_host().getStateManager().getLog(index-1);
                curlogEntry = _leader.get_host().getStateManager().getLog(index);
                appendEntryArray[2] = String.valueOf(prelogEntry.getIndex());
                appendEntryArray[3] = String.valueOf(prelogEntry.getTerm());
                appendEntryArray[4] = curlogEntry.getState().toString();
                appendEntry = String.join(",", appendEntryArray);
                signedMessage = new MessageHandler(RPCs.APPENDENTRY, appendEntry);
                result = tcp_communicator.initSendToOne(_leader.get_host().getHostManager().getHostAddress(_hostName),
                                                        tcp_replyMsg_one, signedMessage);
                if(result){
                    msg = tcp_replyMsg_one.getMessage().getMessageContent();
                    if (msg.equals(RPCs.SUCCESS)) {
                        if (DEBUG){
                            System.out.println("RPCs.SUCCESS 2");
                        }
                        _leader.get_nextIndex().put(_hostName, index+1);
                    } else if(msg.equals(RPCs.FAIL)) {
                        if (DEBUG){
                            System.out.println("RPCs.FAIL 2");
                        }
                        _leader.get_isFindNextIndex().remove(_hostName);
                    } else {
                        if (DEBUG) {
                            System.out.println("Some error 2");
                        }
                    }
                } else {
                    if (DEBUG) {
                        System.out.println("Result False 2");
                    }
                }
                break;
            case LeaderJobs.APPENDLOG:
                if (DEBUG){
                    System.out.println("LeaderJobs.APPENDLOG");
                }
                int size = 1000;
                synchronized (_leader._queue){
                    if(!_leader._queue.isEmpty()){
                        Leader.BState bState = _leader.getState();
                        _host.getStateManager().appendAnEntry(bState._state, _host.getCurrentTerm());
                        appendEntryArray[4] = bState._state.toString();
                        if (DEBUG){
                            System.out.println("message 1");
                        }
                    }else{
                        State state = _host.getStateManager().getLastLog().getState();
                        appendEntryArray[2] = String.valueOf(_host.getStateManager().getLog(_host.getStateManager().getLastIndex()-1).getIndex());
                        appendEntryArray[3] = String.valueOf(_host.getStateManager().getLog(_host.getStateManager().getLastIndex()-1).getTerm());
                        appendEntryArray[4] = state.toString();
                        if (DEBUG){
                            System.out.println("message 2");
                        }
                    }
                }
                appendEntry = String.join(",", appendEntryArray);
                signedMessage = new MessageHandler(RPCs.APPENDENTRY, appendEntry);
                result = tcp_communicator.initSendToOne(_leader.get_host().getHostManager().getHostAddress(_hostName),
                                                        tcp_replyMsg_one, signedMessage);
                if(result){
                    msg = tcp_replyMsg_one.getMessage().getMessageContent();
                    if (msg.equals(RPCs.SUCCESS)) {
                        if (DEBUG){
                            System.out.println("RPCs.SUCCESS 3");
                        }
                        int currentIndex = _leader.get_nextIndex().get(_hostName);
                        _leader.get_nextIndex().put(_hostName, currentIndex+1);
                        _leader.set_votes();
                    } else if(msg.equals(RPCs.FAIL)) {
                        if (DEBUG){
                            System.out.println("RPCs.FAIL 3");
                        }
                        _leader.get_isFindNextIndex().remove(_hostName);
                    } else if(msg.equals(RPCs.BFAIL)) {
                        if (DEBUG){
                            System.out.println("RPCs.BFAIL 3");
                        }
                    } else{
                        if (DEBUG){
                            System.out.println("Some error 3");
                        }
                    }
                } else {
                    if (DEBUG){
                        System.out.println("Result False 3: ");
                    }
                }
                break;
            case LeaderJobs.HEARTBEAT:
                if (DEBUG){
                //    System.out.println("LeaderJobs.HEARTBEAT");
                }
                appendEntry = String.join(",", appendEntryArray);
                // TODO: out of memoery issue
                if (signedMessage == null) signedMessage = new MessageHandler(RPCs.APPENDENTRY, appendEntry);
                result = tcp_communicator.initSendToOne(_leader.get_host().getHostManager().getHostAddress(_hostName), tcp_replyMsg_one, signedMessage);
                if(result){
                    msg = tcp_replyMsg_one.getMessage().getMessageContent();
                    if (msg.equals(RPCs.SUCCESS)) {
                        if (DEBUG){
                //            System.out.println("RPCs.SUCCESS 4");
                        }
                    } else if(msg.equals(RPCs.FAIL)){
                        if (DEBUG){
                //            System.out.println("RPCs.FAIL 4");
                        }
                        _leader.get_isFindNextIndex().remove(_hostName);
                    } else {
                        if (DEBUG){
                //            System.out.println("Some error 4:" + msg);
                        }
                    }
                } else {
                    if (DEBUG){
                //        System.out.println("Result False 4");
                    }
                }
                break;
        }
    }
}
