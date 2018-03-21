package host;

import definedtype.RPCs;
import definedtype.RoleType;
import message.MessageHandler;
import Communicator.TCP_Communicator;
import Communicator.TCP_ReplyMsg_All;

import java.util.Observable;

public class Candidate extends Observable implements Runnable {
    private boolean _closed;
    private Host _host;
    private String _hostName;
    private TCP_ReplyMsg_All _tcp_ReplyMsg_All;

    public Candidate(Host host){
        _host = host;
        _hostName = host.getHostName();
    }

    @Override
    public void run() {
        System.out.println("[" + _hostName +  "] is Candidate!");
        boolean result = false;
        MessageHandler signedMessage;
        TCP_Communicator tcp_communicator = new TCP_Communicator(_host.getHostName());

        _closed = false;
        while (!result && !_closed){
            _tcp_ReplyMsg_All = new TCP_ReplyMsg_All();
            String[] requestVoteArray = new String[] {
                _host.getCurrentTerm().toString(),
                _host.getHostName(),
                String.valueOf(_host.getStateManager().getLastLog().getIndex()),
                String.valueOf(_host.getStateManager().getLastLog().getTerm())
            };
            String requestVote = String.join(",", requestVoteArray);
            signedMessage = new MessageHandler(RPCs.REQUESTVOTE, requestVote);
            result = tcp_communicator.initSendToAll(_host.getHostManager(), _tcp_ReplyMsg_All, signedMessage);
            System.out.println("["+ _hostName + "] tcp_communicator.initSendToAll: " + result);
        }
        if(result){
            setChanged();
            notifyObservers(RoleType.C2L);
        }
    }

    public void leave(){
        _closed = true;
    }

    public TCP_ReplyMsg_All get_tcp_ReplyMsg_All(){
        return _tcp_ReplyMsg_All;
    }
}
