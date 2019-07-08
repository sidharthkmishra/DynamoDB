package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.util.Log;
import android.util.Pair;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class CommandResult {
    private static final String TAG = CommandResult.class.getSimpleName();

    CommandResult() {
    }

    public ConcurrentHashMap<String, Pair<String, Integer>> getResultMap() {
        return resultMap;
    }
    
    void clearQueryResultMap() {
        resultMap = new ConcurrentHashMap<String, Pair<String, Integer>>();
    }

    void addRowToQueryResultMap(String key, String value, String version) {
        if(key == null) {
            Log.e(TAG, "Trying to add empty key to result map.");
            return;
        }

        try {
            Integer versionInt = 1;
            if (!resultMap.containsKey(key)) {
                if(version != null) {
                    versionInt = Integer.parseInt(version);
                }
                resultMap.put(key, new Pair<String, Integer>(value, versionInt));
            } else {
                Pair<String, Integer> presentValue = resultMap.get(key);
                if(version != null) {
                    versionInt = Integer.parseInt(version);
                    if(presentValue.second < versionInt) {
                        resultMap.put(key, new Pair<String, Integer>(value, versionInt));
                    }
                } else {
                    resultMap.put(key, new Pair<String, Integer>(value, versionInt));
                }
            }
        } catch (NumberFormatException e) {
            Log.e(TAG, "version is not integer: " + key + " " + value + " " + version);
        }
    }

    boolean isQueryResultReady() {
        return this.isQueryResultReady;
    }

    synchronized void setQueryResultReady(boolean isQueryResultReady) {
        this.isQueryResultReady = isQueryResultReady;
    }

    private boolean isQueryResultReady = false;
    private ConcurrentHashMap<String, Pair<String, Integer>> resultMap = new ConcurrentHashMap<String, Pair<String, Integer>>();
}
