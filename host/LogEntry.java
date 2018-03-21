package host;

public class LogEntry {
    private State state;
    private int term;
    private int index; // index maybe not needed
    private boolean isCommitted;

    LogEntry(State aState, int term, int index){
        //the input should be the host's current state, we need a copy of it.
        //otherwise, state in this class will change will state in host.
        this.state = aState.clone();
        this.term = term;
        this.index = index;
        //this.index = index;
        isCommitted = false;
    }

    public State getState(){
        return state;
    }

    public int getTerm(){
        return term;
    }

    public int getIndex(){
        return index;
    }

    public boolean isCommitted(){
        return isCommitted;
    }

    public void commitEntry(){
        isCommitted = true;
    }

    /**
     * This method compares two LogEntry, returns true if state, term and index are all same. Ignore isCommitted
     * @param anEntry anEntry
     * @return boolean
     */
    public boolean equals(LogEntry anEntry){
        return (state.equals(anEntry.getState()) && term == anEntry.getTerm() && index == anEntry.getIndex());
    }

    public String getString(){
        String[] array = new String[] {
                String.valueOf(term),
                String.valueOf(index),
                String.valueOf(isCommitted)
        };
        String result = String.join(",", array);
        return result;
    }
}
