package host;

import definedtype.LeaderJobs;
import Communicator.TCP_Communicator;
import Communicator.TCP_ReplyMsg_All;

import java.util.*;

public class Leader extends Observable implements Runnable {
    private boolean _closed;
    private HashMap<String , Integer> _nextIndex;
    private HashSet<String> _isFindNextIndex;
    private Host _host;
    private String _hostName;
    private TCP_ReplyMsg_All _tcp_replyMsg_all;
    private Set<String> _hostnames;
    private int _votes;
    private boolean DEBUG = false;
    public Queue<BState> _queue;
    public Lock _Lock;

    public Leader(Host host, TCP_ReplyMsg_All tcp_replyMsg_all){
        _Lock = new Lock();
        _queue = new LinkedList<>();
        _host = host;
        _hostName = host.getHostName();
        _tcp_replyMsg_all = tcp_replyMsg_all;
        _nextIndex = new HashMap<>();
        _isFindNextIndex = new HashSet<>();
    }

    @Override
    public void run() {
        System.out.println("I am Leader!");
        _host.getHostManager().setLeaderAddress(_host.getHostName());
        // (1)  get the latest log index for each host
        //      update the log for non-up-to-date hosts
        //      1. initial each log index to the latest index (done in constructor)
        //      2. find the matching index and append the remaining logs
        // (2)  append the user request
        // (3)  send the heartbeat
        // (1), (2), (3) will happen concurrently.

        TCP_Communicator tcp_communicator = new TCP_Communicator(_hostName);
        _hostnames = _host.getHostManager().getHostNames();
        int lastIndex = _host.getCommitIndex();
        for(String hostname : _hostnames){
            _nextIndex.put(hostname, lastIndex + 1);
        }

        _closed = false;
        while (!_closed){
            _votes = 1;
            HashMap<String, Thread> threads = new HashMap<>();
            int queueSize = _queue.size();
            boolean appendFlag = false;
            for(String hostname : _hostnames){
                if(!hostname.equals(_host.getHostManager().getMyHostName())){
                    /*if(!_isFindNextIndex.contains(hostname)){
                        if (DEBUG) System.out.println("[Leader] Phase1 to hostName : " + hostname);
                        threads.put(hostname, new Thread(new Leader_Worker(this, LeaderJobs.FINDINDEX, hostname, _host)));
                    } else if(_nextIndex.get(hostname) <= _host.getCommitIndex()){
                        if (DEBUG) System.out.println("[Leader] Phase2 to hostName : " + hostname);
                        threads.put(hostname, new Thread(new Leader_Worker(this, LeaderJobs.KEEPUPLOG, hostname, _host)));
                    } else*/ if(queueSize > 0){
                        if (DEBUG) System.out.println("[Leader] Phase3 to hostName : " + hostname);
                        appendFlag = true;
                        threads.put(hostname, new Thread(new Leader_Worker(this, LeaderJobs.APPENDLOG, hostname, _host)));
                    } else{
                        if (DEBUG) System.out.println("[Leader] Phase4 to hostName : " + hostname);
                        threads.put(hostname, new Thread(new Leader_Worker(this, LeaderJobs.HEARTBEAT, hostname, _host)));
                    }
                    threads.get(hostname).setDaemon(true);
                    threads.get(hostname).start();
                }
            }
            for(String hostname : _hostnames){
                if(!hostname.equals(_host.getHostManager().getMyHostName())){
                    try{
                        threads.get(hostname).join();
                    } catch (InterruptedException e){
                        e.printStackTrace();
                    }
                }
            }
            if(appendFlag){
                // TODO: tkuo
                //boolean result = _votes > _hostnames.size() / 2;
                boolean result = _votes >= _hostnames.size() / 2;
                if(result){
                    _host.setCommitIndex(_host.getCommitIndex()+1);
                    _host.getStateManager().commitEntry(_host.getCommitIndex());
                    System.out.println(_host.getStateManager().toString());
                } else{
                    _host.getStateManager().deleteLastEntry();
                }
                synchronized (_Lock){
                    _Lock._result = result;
                    _Lock.notifyAll();
                }
            }
        }
    }

    public void set_votes(){
        _votes++;
    }
    public Host get_host(){
        return _host;
    }
    public HashSet<String> get_isFindNextIndex(){
        return _isFindNextIndex;
    }
    public HashMap<String , Integer> get_nextIndex(){
        return _nextIndex;
    }

    public boolean addState(State state){
        BState bState = new BState(state);
        _queue.offer(bState);
        synchronized (_Lock){
            try {
                _Lock.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("[Leader]: addState is " + _Lock._result);
            return _Lock._result;
        }
    }

    public BState getState(){
        return _queue.poll();
    }

    class Lock{
        public boolean _result;

        public Lock(){
            _result = false;
        }
    }

    public void leave(){
        _closed = true;
    }

    class BState{
        public State _state;
        public BState(State state){
            _state = state;
        }
    }
}
