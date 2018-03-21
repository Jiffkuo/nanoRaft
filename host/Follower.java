package host;

import definedtype.RoleType;

import java.util.Observable;
import java.util.Timer;
import java.util.TimerTask;

public class Follower extends Observable implements Runnable {
    // Receive heart beat, appendRPC, voteRPC, forwardRPC
    // Send forward, AppendRPCReply, VoteReply, voteRPC
    // follower will random time(300ms - 500ms) out and send the notify to the observer
    private StateManager _stateManager;
    private double _time;
    // TODO: need to figure out suitable timeout
    private final int BASELATENCY = 1000;
    private final int DURATION = 1000;
    private boolean _closed;

    public Follower(StateManager stateManager){
        resetTimer();
        _stateManager = stateManager;
    }

    @Override
    public void run() {
        System.out.println("I am Follower!");
        _closed = false;

        Timer _timer = new Timer();
        int duration = 10; // schedule period 10 ms
        _timer.schedule(new TimerTask() {
            @Override
            public void run() {
                _time -= duration;
                if(_time < 0 && !_closed){
                    resetTimer();
                    _timer.cancel();
                    setChanged();
                    notifyObservers(RoleType.F2C); // notify Host.java:update(...)
                }
            }
        }, 0, duration);
    }

    public void leave(){
        _closed = true;
    }

    /**
     * If host receive heartbeat msg will call this function.
     */
    public void receivedHeartBeat(){
        resetTimer();
    }

    public void appendAnEntry(State newState, int term){
        resetTimer();
        _stateManager.appendAnEntry(newState, term);
    }

    public boolean deleteLastEntry() {
        resetTimer();
        return _stateManager.deleteLastEntry();
    }

    public boolean commitLastEntry(){
        resetTimer();
        return _stateManager.commitLastEntry();
    }

    private void resetTimer(){
        _time = randomTimeout();
    }

    private int randomTimeout(){
        int result = ( int ) ( Math.random() * DURATION ) + BASELATENCY;
        return result;
    }
}
