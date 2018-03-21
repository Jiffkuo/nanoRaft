package host;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import java.util.*;

public class StateManager {
    private ArrayList<LogEntry> stateLog;
    private HashMap<String, State> states;
    private Storage fileStoreHandler;
    private boolean commitFailEnable = false;


/*
    StateManager(String[] stateName, String hostName){
        stateLog = new ArrayList<LogEntry>(16);
        states = new HashMap<>();
        this.fileStoreHandler = new Storage(hostName);
        int i = 0;
        for (String name: stateName) {
            State initializedState = new State(name, 0);
            LogEntry initialLog = new LogEntry(initializedState, 0, i);
            states.put(name, initializedState);
            stateLog.add(initialLog);
            commitEntry(i);
            i++;
        }
    }
*/

    StateManager(String[] stateName, String hostName, ArrayList<String> hostList){
        stateLog = new ArrayList<LogEntry>(16);
        states = new HashMap<>();
        this.fileStoreHandler = new Storage(hostName);
        // if no previous log, initialize
        if (fileStoreHandler.getAllLine().size() <= 1) {
            int i = 0;
            for (String name : stateName) {
                State initializedState = new State(name, 0);
                LogEntry initialLog = new LogEntry(initializedState, 0, i);
                states.put(name, initializedState);
                stateLog.add(initialLog);
                commitEntry(i);
                i++;
            }
        } else {
            // if log exist, go through file and update stateLog, states
            // choose one logfile to read
            String fromIP = "";
            for (String s : hostList) {
                if (s.equals(hostName)) continue;
                fromIP = s;
                break;
            }
            // overwrite our logfile
            try {
                Path from = Paths.get("./" + fromIP + "/logFile.txt"); //convert from File to Path
                Path to = Paths.get("./" + hostName + "/logFile.txt"); //convert from String to Path
                Files.copy(from, to, REPLACE_EXISTING);
                //System.out.println("[Warning] fromIP + " + fromIP + " and hostName is " + hostName);
            } catch (IOException e) {
                e.printStackTrace();
            }
            fileStoreHandler.reOpenFile();
            List<String> allLine = fileStoreHandler.getAllLine();
            for (int i = 1; i < allLine.size(); i++) {
                String line = allLine.get(i);
                String[] indexTermNameValue = line.split("\t");
                State st = new State(indexTermNameValue[2], Integer.parseInt(indexTermNameValue[3]));
                LogEntry le = new LogEntry(st, Integer.parseInt(indexTermNameValue[1]), Integer.parseInt(indexTermNameValue[0]));
                states.put(indexTermNameValue[2], st);
                stateLog.add(le);
            }
        }
    }


    /**
     * This method commits the last entry in the entryLog
     * can set to not commit, representing hardware failure, demonstrating Raft
     * @return commit is success
     */
    public boolean commitLastEntry(){
        //if some node recover from crash, do they have a lot of un-commit entry?
        //assuming only newest log entry needs to be commit. older entry are committed.
        return commitEntry(stateLog.size() - 1);
    }

    public synchronized boolean commitEntry(int at) {
        if (stateLog == null || stateLog.size() == 0) {
            System.out.println("Warning: Cannot commit entry");
            return false;
        }
        LogEntry logToCommit = null;
        if (at < stateLog.size()) {
            logToCommit = stateLog.get(at);
        }
        if (logToCommit == null) {
            System.out.println("no found");
            return false;
        }
        if (commitFailEnable) {
            return false;
        }else if (fileStoreHandler.storeNewValue(logToCommit)){
            LogEntry temp = stateLog.get(at);
            states.get(stateLog.get(at).getState().getStateName()).changeState(stateLog.get(at).getState().getStateValue());
            states.get(temp.getState().getStateName()).changeState(temp.getState().getStateValue());
            stateLog.get(at).commitEntry();
            return true;
        }else {
            return false;
        }
    }

    public void appendAnEntry(State newState, int term){
        stateLog.add(new LogEntry(newState, term, stateLog.size()));
    }

    /**
     * This method would delete the last log entry
     * used by the newly leader to sync its log entries
     */
    public boolean deleteLastEntry() {
        if (stateLog.isEmpty()) {
            return true;
        } //fileStoreHandler.deleteLatestCommitedValue()
        if (true) {
            stateLog.remove(stateLog.size()-1);
            return true;
        }else {
            return false;
        }
    }


    /**
     * Let the follower not to commit the entry
     */
    public void enableCommitFail() {
        this.commitFailEnable = true;
    }

    /**
     * Let the follower act normal
     */
    public void disableCommitFail() {
        this.commitFailEnable = false;
    }

    public LogEntry getLastLog(){
        return stateLog.get(stateLog.size()-1);
    }

    public int getLastIndex(){
        return stateLog.size()-1;
    }
    public LogEntry getLog(int index){

        return stateLog.get(index);
    }

    public String toString(){
        StringBuffer result = new StringBuffer();
        for (Map.Entry<String, State> a: states.entrySet()) {
            result.append(a.getValue().toString() + ";");
        }
        return result.toString().substring(0, result.length()-1);
    }
}
