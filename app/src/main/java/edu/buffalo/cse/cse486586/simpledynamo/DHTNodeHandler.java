package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.ListIterator;

public class DHTNodeHandler {
    private static final String TAG = DHTNodeHandler.class.getSimpleName();
    private static final String[] AVD_NUMBER_LIST = {"5554", "5556", "5558", "5560", "5562"};

    private static final DHTNodeHandler singleInstance = new DHTNodeHandler();
    public static DHTNodeHandler getInstance() {
        return singleInstance;
    }

    private LinkedList<String> mDHTNodeKeyList = new LinkedList<String>();
    private HashMap<String, DHTNode> mDHTNodeMap= new HashMap<String, DHTNode>();

    private DHTNodeHandler() {
        createDHTNodeStructure();
    }

    public void close() {
        ListIterator<String> dhtKeyIter = mDHTNodeKeyList.listIterator();
        while(dhtKeyIter.hasNext()) {
            String nodeKeyAtDHT = dhtKeyIter.next();
            if(mDHTNodeMap.get(nodeKeyAtDHT).isReaderConnectionStable()) {
                try {
                    mDHTNodeMap.get(nodeKeyAtDHT).getReader().close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                mDHTNodeMap.get(nodeKeyAtDHT).setReaderConnectionStable(false);
            }
            if(mDHTNodeMap.get(nodeKeyAtDHT).isWriteConnectionStable()) {
                mDHTNodeMap.get(nodeKeyAtDHT).getWriter().close();
                mDHTNodeMap.get(nodeKeyAtDHT).setWriterConnectionStable(false);
            }
        }
    }

    public Collection<DHTNode> getNodeCollection() {
        return mDHTNodeMap.values();
    }

    public DHTNode getNodeForKey(String key) {
        String nodeContainingKey = mDHTNodeKeyList.getFirst();
        ListIterator<String> dhtKeyIter = mDHTNodeKeyList.listIterator();
        while(dhtKeyIter.hasNext()) {
            String nodeKeyAtDHT = dhtKeyIter.next();

            if(genHash(key).compareTo(genHash(nodeKeyAtDHT)) <= 0) {
                nodeContainingKey = nodeKeyAtDHT;
                break;
            }
        }
        return mDHTNodeMap.get(nodeContainingKey);
    }

    public DHTNode getNode(String node) {
        return mDHTNodeMap.get(node);
    }

    public DHTNode getPredecessorNode(String node) {
        String predecessor = mDHTNodeKeyList.getLast();
        String successor = mDHTNodeKeyList.getFirst();

        ListIterator<String> dhtKeyIter = mDHTNodeKeyList.listIterator();
        while(dhtKeyIter.hasNext()) {
            String nodeKeyAtDHT = dhtKeyIter.next();
            if(genHash(nodeKeyAtDHT).compareTo(genHash(node)) == 0) {
                dhtKeyIter.previous();
                break;
            }
        }
        if(dhtKeyIter.hasPrevious())
            predecessor = dhtKeyIter.previous();

        return mDHTNodeMap.get(predecessor);
    }

    public DHTNode getSuccessorNode(String node) {
        String successor = mDHTNodeKeyList.getFirst();
        ListIterator<String> dhtKeyIter = mDHTNodeKeyList.listIterator();
        while(dhtKeyIter.hasNext()) {
            String nodeKeyAtDHT = dhtKeyIter.next();
            if(genHash(nodeKeyAtDHT).compareTo(genHash(node)) == 0)
                break;
        }
        if(dhtKeyIter.hasNext())
            successor = dhtKeyIter.next();

        return mDHTNodeMap.get(successor);
    }

    public void nodeKeyJoin(String node) {
        int insertPos = 0;
        for(String nodeAtDHT : mDHTNodeKeyList) {
            //If the node to be inserted hash is smaller than nodeAtDHT
            //then break and insert
            if(genHash(node).compareTo(genHash(nodeAtDHT)) < 0)
                break;

            insertPos++;
        }
        mDHTNodeKeyList.add(insertPos, node);
    }

    private void createDHTNodeStructure() {
        for (String AVDNumber : AVD_NUMBER_LIST) {
            nodeKeyJoin(AVDNumber);
            mDHTNodeMap.put(AVDNumber, new DHTNode(AVDNumber));
        }
    }

    public String genHash(String input) {
        String hashCode = input;
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] sha1Hash = sha1.digest(input.getBytes());
            Formatter formatter = new Formatter();
            for (byte b : sha1Hash) {
                formatter.format("%02x", b);
            }
            hashCode = formatter.toString();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            Log.v(TAG, e.getMessage(), e);
        }

        return hashCode;
    }


}
