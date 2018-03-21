package host;

public class State {
    private String stateName;
    private int stateValue;

    State(String stateName, int stateValue){
        this.stateName = stateName;
        this.stateValue = stateValue;
    }

    public void changeState(int newValue){
        stateValue = newValue;
    }

    public String getStateName(){
        return stateName;
    }

    public int getStateValue(){
        return stateValue;
    }

    /**
     * This method returns string:"stateName -> stateValue"
     * @return toString
     */
    public String toString(){
        return stateName + "->" + stateValue;
    }

    public State clone(){
        State temp = new State(stateName, stateValue);
        return temp;
    }

    /**
     * This method compares two states, returns true if they have same stateName and stateValue.
     * @param aState another state
     * @return is equal
     */
    public boolean equals(State aState){
        if (this.stateName.equals(aState.getStateName()) && this.stateValue == aState.getStateValue()){
            return true;
        }
        return false;
    }
}
