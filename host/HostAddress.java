package host;

import java.io.Serializable;
import java.security.interfaces.RSAPublicKey;

public class HostAddress implements Serializable{
    private String hostName;
    private String hostIp;
    private int hostPort;

    public HostAddress(String hostName, String hostIp, int hostPort) {
        this.hostName = hostName;
        this.hostIp = hostIp;
        this.hostPort = hostPort;
    }

    public HostAddress(String hostName) {
        this.hostName = hostName;
    }

    public HostAddress(String hostIp, int hostPort) {
        this.hostIp = hostIp;
        this.hostPort = hostPort;
    }

    public String getHostName() {
        return hostName;
    }

    public String getHostIp() {
        return hostIp;
    }

    public int getHostPort() {
        return hostPort;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public void setHostIp(String hostIp) {
        this.hostIp = hostIp;
    }

    public void setHostPort(int hostPort) {
        this.hostPort = hostPort;
    }

    public boolean equals(HostAddress hostAddress){
        return hostAddress.getHostIp().equals(hostIp) && hostAddress.getHostPort() == hostPort;
    }
}
