package host;

import message.MessageHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ForwardCollector {
    HostManager hostManager;
    HashMap<String, Integer> forwardMsg;

    ForwardCollector(HostManager hostManager) {
        this.hostManager = hostManager;
        forwardMsg = new HashMap<>();
    }

    public synchronized void putIntoCollection(MessageHandler messages) {
        String plantext = messages.getMessageContent();
        if (forwardMsg.containsKey(plantext)) {
            forwardMsg.put(plantext, forwardMsg.get(plantext) + 1);
        }
        else {
            forwardMsg.put(plantext, 1);
        }
    }

    public synchronized String getResult() {
        // majority of followers, leader is not included
        // TODO
        //int majoritySize = (hostManager.getHostList().size() - 1) / 2 + 1; 
        int majoritySize = (hostManager.getHostList().size() - 1) / 2; 
        for (Map.Entry<String, Integer> a: forwardMsg.entrySet()) {
            if (a.getValue() >= majoritySize) {
                forwardMsg = new HashMap<>();
                return a.getKey();
            }
        }
        forwardMsg = new HashMap<>(); //clean old data;
        return null;
    }
}
