package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.util.LinkedList;

public class DHTNode {
    private static final String TAG = DHTNode.class.getSimpleName();

    private String nodeNumber;

    private CommandResult cmdResult = new CommandResult();

    public CommandResult getCmdResult() {
        return cmdResult;
    }

    private BufferedReader mReader = null;
    private PrintWriter mWriter = null;

    private boolean isReaderConnectionStable = false;
    private boolean isWriteConnectionStable = false;
    private boolean isNodeJoinCompleted = false;
    private boolean isNodeWriteBusy = false;
    private boolean isInsertCompleted = false;

    public boolean isInsertCompleted() {
        return isInsertCompleted;
    }

    synchronized public void setInsertCompleted(boolean insertCompleted) {
        isInsertCompleted = insertCompleted;
    }

    public boolean isNodeWriteBusy() { return isNodeWriteBusy; }
    public boolean isReaderConnectionStable() {
        return isReaderConnectionStable;
    }
    public boolean isWriteConnectionStable() {
        return isWriteConnectionStable;
    }
    public boolean isNodeJoinCompleted() {
        return this.isNodeJoinCompleted;
    }

    synchronized public void setNodeWriteBusy(boolean isNodeWriteBusy) {
        this.isNodeWriteBusy = isNodeWriteBusy;
    }
    synchronized public void setReaderConnectionStable(boolean connectionStable) {
        this.isReaderConnectionStable = connectionStable;
    }

    synchronized public void setWriterConnectionStable(boolean connectionStable) {
        this.isWriteConnectionStable = connectionStable;
    }

    synchronized public void setNodeJoinCompleted(boolean isNodeJoinCompleted) {
        this.isNodeJoinCompleted = isNodeJoinCompleted;
    }

    DHTNode(String nodeNumber) {
        this.nodeNumber = nodeNumber;
    }

    public String getNodeNumber() {
        return nodeNumber;
    }

    public BufferedReader getReader() {
        while(!isReaderConnectionStable) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return mReader;
    }

    public PrintWriter getWriter() {
        while(!isWriteConnectionStable) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return mWriter;
    }

    synchronized public void setReader(BufferedReader reader) {
        this.mReader = reader;
    }

    synchronized public void setWriter(PrintWriter writer) {
        this.mWriter = writer;
    }
}
