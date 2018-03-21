package host;

import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HostManager {
    private HostAddress myAddress;
    private HostAddress leaderAddress;
    private HashMap<String ,HostAddress> hostList;

    HostManager(String hostName, String ip, int port){
        myAddress = new HostAddress(hostName, ip, port);
        leaderAddress = null;
        hostList = new HashMap<String, HostAddress>();
        hostList.put(hostName, myAddress);
    }

    HostManager(HostAddress myAddress){
        this.myAddress = myAddress;
        leaderAddress = null;
        hostList = new HashMap<String, HostAddress>();
        hostList.put(myAddress.getHostName(), myAddress);
    }

    public boolean addHostToList(HostAddress aHost){
        //return boolean for possible future use
        if (!hostList.containsKey(aHost.getHostName())){
            hostList.put(aHost.getHostName(), aHost);
            return true;
        } else {
            hostList.put(aHost.getHostName(), aHost);
            return false;
        }
    }

    public boolean addHostToList(String hostName, String ip, int port){
        //return boolean for possible future use
        if (!hostList.containsKey(hostName)){
            hostList.put(hostName, new HostAddress(hostName, ip, port));
            return true;
        } else {
            hostList.put(hostName, new HostAddress(hostName, ip, port));
            return false;
        }
    }

    public boolean addHostToList(String hostName, Socket aSocket){
        String ip = aSocket.getInetAddress().toString().substring(1);
        if (!hostList.containsKey(hostName)){
            hostList.put(hostName, new HostAddress(hostName, ip, aSocket.getPort()));
            return true;
        } else {
            hostList.put(hostName, new HostAddress(hostName, ip, aSocket.getPort()));
            return false;
        }
    }

    public boolean removeHostFroList(HostAddress aHost){
        //return boolean for possible future use
        if (hostList.containsKey(aHost.getHostName())){
            hostList.remove(aHost.getHostName());
            return true;
        } else {
            return false;
        }
    }

    public HashMap<String, HostAddress> getHostList(){
        return hostList;
    }

    public Set<String> getHostNames(){
        return hostList.keySet();
    }

    public String getMyHostName(){
        return myAddress.getHostName();
    }

    public HostAddress getMyAddress(){
        return myAddress;
    }

    public void setLeaderAddress(HostAddress leader){
        leaderAddress = leader;
    }

    public void setLeaderAddress(String leaderName){
        leaderAddress = hostList.get(leaderName);
    }

    public HostAddress getLeaderAddress(){
        return leaderAddress;
    }

    public String getLeaderName(){
        return leaderAddress.getHostName();
    }

    public HostAddress getHostAddress(String hostName){
        return hostList.get(hostName);
    }

    public void replaceHostList(HashMap<String ,HostAddress> newList){
        hostList = newList;
    }

    public String toString() {
        StringBuffer result = new StringBuffer();
        for (Map.Entry<String, HostAddress> a: hostList.entrySet()) {
            result.append("Host Name: " + a.getValue().getHostName() + " IP:" + a.getValue().getHostIp() + "\n");
        }
        return result.toString();
    }

    public boolean isInHostList(String ip){
        for (Map.Entry<String, HostAddress> a: hostList.entrySet()) {
            if (a.getValue().getHostIp().equals(ip)) {
                return true;
            }
        }
        return false;
    }
}
