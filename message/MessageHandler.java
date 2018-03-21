package message;
import java.io.Serializable;

public class MessageHandler implements Serializable {
    private String messageType;
    private String messageContent;

    public MessageHandler(String messageType, String message) {
        this.messageType = messageType;
        this.messageContent = message;
    }

    public String getMessageType() {
        return messageType;
    }

    public String getMessageContent(){
        return messageContent;
    }

    public void setMessageType(String messageType){
        this.messageType = messageType;
    }
}
