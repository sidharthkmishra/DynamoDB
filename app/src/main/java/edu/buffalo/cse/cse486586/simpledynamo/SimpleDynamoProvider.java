package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.StrictMode;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.util.Pair;

public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final String NODE_LIST[] = {"5554", "5556", "5558", "5560", "5562"};
	static final int SERVER_PORT = 10000;

	static final String msgChannelReady = "ChannelReady";
	static final String msgJoinCmd = "Join";
	static final String msgInsertCmd = "Insert";
	static final String msgInsertBackCmd = "InsertBack";
	static final String msgQueryCmd = "Query";
	static final String msgQueryResultCmd = "QueryResult";
	static final String msgQueryResultEndCmd = "QueryResultEnd";
	static final String msgDeleteCmd = "Delete";

	static String currentProcessNumber = null;

	//DB File
	private static final String DATABASE_NAME = "SimpleDynamo.db";
	//For optimization
	CommandResult mDbMap;

	//Wed on't need the version for this. But just keeping 1 for future use
	private static final int VERSION = 1;
	private SQLiteDatabase mDatabase;

	private Uri mUri;

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	@Override
	public boolean onCreate() {
		// If you need to perform any one-time initialization task, please do it here.
		Context context = getContext();
		mDbMap = new CommandResult();

		if(mDbMap == null)
			return false;

		StrictMode.ThreadPolicy tp = StrictMode.ThreadPolicy.LAX;
		StrictMode.setThreadPolicy(tp);
		//StrictMode.ThreadPolicy policy = new StrictMode.ThreadPolicy.Builder().permitAll().build();
		//StrictMode.setThreadPolicy(policy);

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		currentProcessNumber = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

		mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

		//Clear the DB as anyway we are going to reover from replicated nodes
		deleteInternal(mUri, null, null);

		//Start the server thread to allow the AVD to listen all incoming connections
		new HandleClientConnectionTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, SERVER_PORT);
		connectToAllActiveNodes();
		new CommandHandlerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, Integer.parseInt(currentProcessNumber));

		//try {
		//	Thread.sleep(10);
		//} catch (InterruptedException e) {
		//	Log.i(TAG, e.getMessage());
		//}

		return true;
	}

	private void connectToAllActiveNodes() {
		Log.i(TAG, "Start connecting to all other node");
		Socket socket = null;
		PrintWriter socketWriter;
		BufferedReader socketReader;

		for (String avdToConnect : NODE_LIST) {
			int retryCount = 0;
			while (true) {
				try {
					Thread.sleep(100);
					//Create a socket writer and reader to send/receive data
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdToConnect) * 2);
					//TODO
					socket.setSoTimeout(1700);
					socketWriter = new PrintWriter(socket.getOutputStream(), true);
					socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

					if (msgChannelReady.equals(socketReader.readLine())) {
						Log.i(TAG, "connected to server: " + avdToConnect);
						DHTNodeHandler.getInstance().getNode(avdToConnect).setWriter(socketWriter);
						DHTNodeHandler.getInstance().getNode(avdToConnect).setWriterConnectionStable(true);

						//If its the same AVD then don't set the reader as its been set at server socket
						if(!avdToConnect.equals(currentProcessNumber)) {
							DHTNodeHandler.getInstance().getNode(avdToConnect).setReader(socketReader);
							DHTNodeHandler.getInstance().getNode(avdToConnect).setReaderConnectionStable(true);
						}

						Log.i(TAG, "Sending command request: " + msgJoinCmd + " from " + currentProcessNumber);
						//Initiate the join command
						socketWriter.println(msgJoinCmd);
						socketWriter.println(currentProcessNumber);

						Thread.sleep(50);
						//If either the successor or predecessor then need to get the key values for recovery
						if(avdToConnect.equals(DHTNodeHandler.getInstance().getSuccessorNode(currentProcessNumber).getNodeNumber()) ||
								avdToConnect.equals(DHTNodeHandler.getInstance().getPredecessorNode(currentProcessNumber).getNodeNumber())) {
							socketWriter.println(msgQueryCmd);
							socketWriter.println("@");
							DHTNodeHandler.getInstance().getNode(avdToConnect).setNodeWriteBusy(true);
						} else if(!avdToConnect.equals(currentProcessNumber)){
							DHTNodeHandler.getInstance().getNode(avdToConnect).setNodeJoinCompleted(true);
						}

						break;
					} else {
						//if(socket != null)
						//	socket.close();
						socket = null;
						if (retryCount++ == 3) {
							//Try with other AVD as eventually the other node will join later
							break;
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
					socket = null;
				}
			}
		}
	}

	private class HandleClientConnectionTask extends AsyncTask<Integer, String, Void> {

		@Override
		protected Void doInBackground(Integer... sockets) {
			Integer serverPort = sockets[0];

			//Creating a server socket and a thread (AsyncTask) that listens on the server port
			ServerSocket serverSocket = null;
			//Connect to 5554 Server to get the successor and predecessor Info
			while(serverSocket == null) {
				try {
					//TODO
					serverSocket = new ServerSocket();
					serverSocket.setReuseAddress(true);
					serverSocket.bind(new InetSocketAddress(SERVER_PORT));
					//serverSocket = new ServerSocket(SERVER_PORT);
					serverSocket.setSoTimeout(10);
				} catch (Exception e) {
					Log.i(TAG, e.getMessage());
					serverSocket = null;
				}
			}

			while(!Thread.interrupted()) {
				try {
					//When a client connects to server, a client socket will be returned
					Socket clientSocket = serverSocket.accept();

					//Create a socket reader and writer to get/send msg
					//TODO
					clientSocket.setSoTimeout(1700);
					BufferedReader clientSocketReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
					PrintWriter clientSocketWriter = new PrintWriter(clientSocket.getOutputStream(), true);

					Log.i(TAG, "connection accepted from client");

					//Notify the AVD that the channel is ready.
					clientSocketWriter.println(msgChannelReady);

					//Start accepting the input from the connected AVD
					String cmdMsg = clientSocketReader.readLine();
					String avdNodeNumber = clientSocketReader.readLine();
					Log.i(TAG, "Received command request: " + cmdMsg + " from: " + avdNodeNumber);

					if(cmdMsg != null && cmdMsg.equals(msgJoinCmd)) {
						//Set the reader as the current connection
						DHTNodeHandler.getInstance().getNode(avdNodeNumber).setReader(clientSocketReader);
						DHTNodeHandler.getInstance().getNode(avdNodeNumber).setReaderConnectionStable(true);

						//If its the same AVD then don't set the writer as its been set from client socket
						if(!avdNodeNumber.equals(currentProcessNumber)) {
							DHTNodeHandler.getInstance().getNode(avdNodeNumber).setWriter(clientSocketWriter);
							DHTNodeHandler.getInstance().getNode(avdNodeNumber).setWriterConnectionStable(true);
						}
						DHTNodeHandler.getInstance().getNode(avdNodeNumber).setNodeJoinCompleted(true);
					}
				} catch (SocketTimeoutException st) {
					//Do nothing
				} catch (IOException e) {
					Log.e(TAG, e.getMessage(), e);
				}
			}
			try {
				serverSocket.close();
				DHTNodeHandler.getInstance().close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}
	}

	private class CommandHandlerTask extends AsyncTask<Integer, String, Void> {

		private Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
		private int mSeqNum = 0;

		@Override
		protected Void doInBackground(Integer... processNumber) {
			int idealCount = 5;
			while(!Thread.interrupted()) {
				String cmdToBeProcessed;
				idealCount = (idealCount + 5) % 50;
				Iterator<DHTNode> itr = DHTNodeHandler.getInstance().getNodeCollection().iterator();
				DHTNode node = null;
				while(itr.hasNext()) {
					try {
						node = itr.next();
						if(node.isReaderConnectionStable() && node.getReader() != null && node.getReader().ready()) {
							idealCount = 5;
							cmdToBeProcessed = node.getReader().readLine();
							Log.i(TAG, "Command received from " + node.getNodeNumber() +
									": " + cmdToBeProcessed);

							if (msgQueryResultCmd.equals(cmdToBeProcessed)) {
								node.getCmdResult().clearQueryResultMap();
								while (!(cmdToBeProcessed = node.getReader().readLine()).equals(msgQueryResultEndCmd)) {
									String valueFromNode = node.getReader().readLine();
									String versionFromNode = node.getReader().readLine();
									Log.i(TAG, "result: " + cmdToBeProcessed + " " + valueFromNode + " " + versionFromNode);
									node.getCmdResult().addRowToQueryResultMap(cmdToBeProcessed, valueFromNode, versionFromNode);
								}
								node.getCmdResult().setQueryResultReady(true);
								if(!node.isNodeJoinCompleted()) {
									Log.i(TAG, "Iterate through all the keys and insert at this node");
									String predecessor = DHTNodeHandler.getInstance().getPredecessorNode(currentProcessNumber).getNodeNumber();
									String predecessorOfPredecessor = DHTNodeHandler.getInstance().getPredecessorNode(predecessor).getNodeNumber();

									for(HashMap.Entry<String, Pair<String, Integer>> entry : node.getCmdResult().getResultMap().entrySet()) {
										String theNodeForKey = DHTNodeHandler.getInstance().getNodeForKey(entry.getKey()).getNodeNumber();
										if(theNodeForKey.equals(predecessor) ||
											theNodeForKey.equals(predecessorOfPredecessor) ||
											theNodeForKey.equals(currentProcessNumber)) {
											ContentValues cv = new ContentValues();
											cv.put(DynamoDbSchema.MessageTable.Cols.KEY, entry.getKey());
											cv.put(DynamoDbSchema.MessageTable.Cols.VALUE, entry.getValue().first);
											cv.put(DynamoDbSchema.MessageTable.Cols.VERSION, entry.getValue().second.toString());
											insertInternal(mUri, cv);
										}
									}

									node.getCmdResult().clearQueryResultMap();
									node.setNodeJoinCompleted(true);
									node.setNodeWriteBusy(false);
								}
							} /*else if(msgInsertBackCmd.equals(cmdToBeProcessed)) {
								node.setInsertCompleted(true);
							} */else if (msgInsertCmd.equals(cmdToBeProcessed)) {
								ContentValues cv = new ContentValues();
								String key = node.getReader().readLine();
								String value = node.getReader().readLine();
								Log.i(TAG, "Insert key:" + key + " value: " + value);
								cv.put(DynamoDbSchema.MessageTable.Cols.KEY, key);
								cv.put(DynamoDbSchema.MessageTable.Cols.VALUE, value);
								insertInternal(mUri, cv);
								//node.getWriter().println(msgInsertBackCmd);
							} else if(msgQueryCmd.equals(cmdToBeProcessed)) {
								String selection = node.getReader().readLine();
								Log.i(TAG, "query cmd from:" + currentProcessNumber);
								node.getWriter().println(msgQueryResultCmd);
								for(HashMap.Entry<String, Pair<String, Integer>> entry : queryInternal(mUri, null, selection, null, null).entrySet()) {
									Log.i(TAG, "Need to send the result from local query to cmd originator:" + node.getNodeNumber());
									node.getWriter().println(entry.getKey());
									node.getWriter().println(entry.getValue().first);
									node.getWriter().println(entry.getValue().second.toString());
								}
								node.getWriter().println(msgQueryResultEndCmd);
							} else if(msgDeleteCmd.equals(cmdToBeProcessed)) {
								String selection = node.getReader().readLine();
								deleteInternal(mUri, selection, null);
							}
						}

						Thread.sleep(idealCount);
					} catch (SocketTimeoutException e) {
						if(!node.isNodeJoinCompleted())
							node.setNodeWriteBusy(false);
						node.setReaderConnectionStable(false);
						Log.i(TAG, e.getMessage(), e);
					} catch (IOException e) {
						if(!node.isNodeJoinCompleted())
							node.setNodeWriteBusy(false);
						node.setReaderConnectionStable(false);
						Log.i(TAG, e.getMessage(), e);
					} catch (Exception e) {
						Log.e(TAG, e.getMessage(), e);
					}
				}
			}

			return null;
		}
		private Uri buildUri(String scheme, String authority) {
			Uri.Builder uriBuilder = new Uri.Builder();
			uriBuilder.authority(authority);
			uriBuilder.scheme(scheme);
			return uriBuilder.build();
		}
	}

	//Android provides the SQLiteOpenHelper class to check if the db is already exists,
	//if it doesn't it creates and create the initial db etc.
	//We need have a derived class from the abstract class
	public class DynamoDBHelper extends SQLiteOpenHelper {
		private static final int VERSION = 1;
		private static final String DATABASE_NAME = "crimeBase.db";

		public DynamoDBHelper(Context context) {
			super(context, DATABASE_NAME, null, VERSION);
		}

		@Override
		public void onCreate(SQLiteDatabase db) {
			db.execSQL("create table " + DynamoDbSchema.MessageTable.NAME + "(" +
					DynamoDbSchema.MessageTable.Cols.KEY + ", " +
					DynamoDbSchema.MessageTable.Cols.VALUE + ", " +
					DynamoDbSchema.MessageTable.Cols.VERSION +
					")"
			);
		}

		@Override
		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
		}
	}

	class DynamoDbSchema {
		final class MessageTable {
			static final String NAME = "message";

			final class Cols {
				static final String KEY = "key";
				static final String VALUE = "value";
				static final String VERSION = "version";
			}
		}
	}

	@Override
	public void shutdown() {
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		String overrideSelection = null;
		String [] overrideSelectionArg = null;

		Log.i(TAG, "delete with selection: " + selection);

		ArrayList<DHTNode> nodeListToSendCmd = new ArrayList<DHTNode>();
		if(selection != null) {
			if(selection.equals("*")) {
				Iterator<DHTNode> itr = DHTNodeHandler.getInstance().getNodeCollection().iterator();
				while(itr.hasNext()) {
					nodeListToSendCmd.add(itr.next());
				}
			} else {
				DHTNode nodeForKey;
				if(selection.equals("@")) {
					nodeForKey = DHTNodeHandler.getInstance().getNode(currentProcessNumber);
				} else {
					nodeForKey = DHTNodeHandler.getInstance().getNodeForKey(selection);
                    DHTNode successorNode = DHTNodeHandler.getInstance().getSuccessorNode(nodeForKey.getNodeNumber());
                    nodeListToSendCmd.add(successorNode);
                    nodeListToSendCmd.add(DHTNodeHandler.getInstance().getSuccessorNode(successorNode.getNodeNumber()));
				}
				nodeListToSendCmd.add(nodeForKey);
			}
		}

		int numberOfSuccessfullInsert = 0;
		for(DHTNode node : nodeListToSendCmd) {
			if(node.isNodeJoinCompleted() && node.isWriteConnectionStable() && node.getWriter() != null) {
				try {
					node.getWriter().println(msgDeleteCmd);
					node.getWriter().println(selection);
				} catch (Exception e) {
					node.setWriterConnectionStable(false);
					Log.i(TAG, "Exception occurred while deleting to node: "
							+ node.getNodeNumber() + ". Exception: " + e.getMessage());
					continue;
				}
			}
		}

		return 0;
	}

	public int deleteInternal(Uri uri, String selection, String[] selectionArgs) {
		String overrideSelection = null;
		String [] overrideSelectionArg = null;

		Log.i(TAG, "delete with selection: " + selection);
		if(selection != null && !selection.equals("@") && !selection.equals("*")) {
			overrideSelection = DynamoDbSchema.MessageTable.Cols.KEY + "= ?";
			overrideSelectionArg = new String[] { selection };
			mDbMap.getResultMap().remove(selection);
		} else {
			mDbMap = new CommandResult();
		}

		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		String key = (String) values.get(DynamoDbSchema.MessageTable.Cols.KEY);
		String value = (String) values.get(DynamoDbSchema.MessageTable.Cols.VALUE);

		ArrayList<DHTNode> nodeListToSendCmd = new ArrayList<DHTNode>();
		DHTNode nodeForKey = DHTNodeHandler.getInstance().getNodeForKey(key);
		nodeListToSendCmd.add(nodeForKey);
		DHTNode successorNode = DHTNodeHandler.getInstance().getSuccessorNode(nodeForKey.getNodeNumber());
		nodeListToSendCmd.add(successorNode);
		nodeListToSendCmd.add(DHTNodeHandler.getInstance().getSuccessorNode(successorNode.getNodeNumber()));

		int numberOfSuccessfullInsert = 0;
		for(DHTNode node : nodeListToSendCmd) {
			Log.i(TAG, "Need to insert key: " + key + " at node: " + node.getNodeNumber());

			if(node.isNodeJoinCompleted() && node.isWriteConnectionStable() && node.getWriter() != null) {
				boolean isErrorOccurred = false;
				try {
					node.getWriter().println(msgInsertCmd);
					node.getWriter().println(key);
					node.getWriter().println(value);
				} catch (Exception e) {
					node.setWriterConnectionStable(false);
					Log.i(TAG, "Exception occurred while inserting to node: "
							+ node.getNodeNumber() + ". Exception: " + e.getMessage());
					continue;
				}
				/*node.setInsertCompleted(false);

				int retryCount = 0;
				while (!node.isInsertCompleted()) {
					if(retryCount++ == 300) {
						//May be the node is down
						isErrorOccurred = true;
						break;
					}
					try {
						Thread.sleep(5);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				if(isErrorOccurred) {
					//node.setWriterConnectionStable(false);
					continue;
				} else {
					numberOfSuccessfullInsert++;
				}*/
			}

		}
		if(numberOfSuccessfullInsert < 2) {
			Log.i(TAG, "Insertion is not completed for at least two nodes.");
		}
		try {
			Thread.sleep(200);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return uri;
	}

	public Uri insertInternal(Uri uri, ContentValues values) {
		String key = (String) values.get(DynamoDbSchema.MessageTable.Cols.KEY);
		String value = (String) values.get(DynamoDbSchema.MessageTable.Cols.VALUE);
		String version = (String) values.get(DynamoDbSchema.MessageTable.Cols.VERSION);

		Log.i(TAG, "insert or update at node: " + currentProcessNumber);
		mDbMap.addRowToQueryResultMap(key, value, version);
		return uri;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		String overrideSelection = null;
		String [] overrideSelectionArg = null;

		Log.i(TAG, "query with selection: " + selection +
				" at node: " + currentProcessNumber);

		ArrayList<DHTNode> nodeListToSendCmd = new ArrayList<DHTNode>();
		if(selection != null) {
			if(selection.equals("*")) {
				Iterator<DHTNode> itr = DHTNodeHandler.getInstance().getNodeCollection().iterator();
				while(itr.hasNext()) {
					nodeListToSendCmd.add(itr.next());
				}
			} else {
				DHTNode nodeForKey;
				DHTNode successorNode;
				if(selection.equals("@")) {
					nodeForKey = DHTNodeHandler.getInstance().getNode(currentProcessNumber);
				} else {
					nodeForKey = DHTNodeHandler.getInstance().getNodeForKey(selection);
					successorNode = DHTNodeHandler.getInstance().getSuccessorNode(nodeForKey.getNodeNumber());
					nodeListToSendCmd.add(successorNode);
					nodeListToSendCmd.add(DHTNodeHandler.getInstance().getSuccessorNode(successorNode.getNodeNumber()));
				}
				nodeListToSendCmd.add(nodeForKey);
			}
		}

		ConcurrentHashMap<String, Pair<String, Integer>> previousMap = null;
		DHTNode finalNode = null;
		for(DHTNode node : nodeListToSendCmd) {
			try {
				int retryCount = 0;
				boolean isErrorOccurred = false;
				while (node.isNodeWriteBusy()) {
					if (retryCount++ == 400) {
						//May be something wrong
						isErrorOccurred = true;
						Log.e(TAG, "Stopping query without sucess");
						break;
					}
					//wait if node write is busy serving
					try {
						Thread.sleep(5);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				if(isErrorOccurred) {
					break;
				}
				node.setNodeWriteBusy(true);

				if (node.isNodeJoinCompleted() && node.isWriteConnectionStable() && node.getWriter() != null) {
					Log.i(TAG, "query need to be executed for node: "
							+ node.getNodeNumber() + " with Key: " + selection);

					try {
						node.getWriter().println(msgQueryCmd);
						node.getWriter().println(selection);
					} catch (Exception e) {
						node.setNodeWriteBusy(false);
						node.setWriterConnectionStable(false);
						Log.i(TAG, "Exception occurred while witting to node: "
								+ node.getNodeNumber() + ". Exception: " + e.getMessage());
						continue;
					}
					node.getCmdResult().setQueryResultReady(false);

					retryCount = 0;
					while (!node.getCmdResult().isQueryResultReady()) {
						//TODO
						if (retryCount++ == 300) {
							//May be the node is down
							isErrorOccurred = true;
							break;
						}
						try {
							Thread.sleep(5);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}

				if (isErrorOccurred) {
					node.setNodeWriteBusy(false);
					node.setWriterConnectionStable(false);
					continue;
				}
				if (previousMap != null) {
					for (HashMap.Entry<String, Pair<String, Integer>> entry : previousMap.entrySet()) {
						node.getCmdResult().addRowToQueryResultMap(entry.getKey(), entry.getValue().first, entry.getValue().second.toString());
						Log.i(TAG, "result at query: " + entry.getKey() + " " + entry.getValue().first + " " + entry.getValue().second.toString());
					}
				}
				finalNode = node;
				previousMap = node.getCmdResult().getResultMap();
				node.getCmdResult().clearQueryResultMap();
				node.setNodeWriteBusy(false);
			} catch (Exception e) {
				Log.i(TAG, "Exception occurred : " + e.getMessage());
			}
		}

		MatrixCursor resultCursor = getQueryResultCursor(previousMap);
		Log.i(TAG, "result count: " + resultCursor.getCount());

		return resultCursor;
	}

	private MatrixCursor getQueryResultCursor(ConcurrentHashMap<String, Pair<String, Integer>> resultMap) {
		MatrixCursor matrixCursor = new MatrixCursor(new String[] {
				SimpleDynamoProvider.DynamoDbSchema.MessageTable.Cols.KEY,
				SimpleDynamoProvider.DynamoDbSchema.MessageTable.Cols.VALUE});
		if(resultMap != null && resultMap.size() > 0) {
			for (ConcurrentHashMap.Entry<String, Pair<String, Integer>> entry : resultMap.entrySet()) {
				matrixCursor.addRow(new Object[]{entry.getKey(), entry.getValue().first});
			}
		}
		return matrixCursor;
	}

	public ConcurrentHashMap<String, Pair<String, Integer>> queryInternal(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		Log.i(TAG, "query with selection: " + selection + " at node: " + currentProcessNumber);

		ConcurrentHashMap<String, Pair<String, Integer>> resultMap;
		if(selection != null && !selection.equals("@") && !selection.equals("*")) {
			resultMap = new ConcurrentHashMap<String, Pair<String, Integer>>();
			if(mDbMap.getResultMap().containsKey(selection)) {
				resultMap.put(selection,
						new Pair<String, Integer>(mDbMap.getResultMap().get(selection).first, mDbMap.getResultMap().get(selection).second));
			}

		} else {
			resultMap = mDbMap.getResultMap();
		}
		return resultMap;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		return 0;
	}
}