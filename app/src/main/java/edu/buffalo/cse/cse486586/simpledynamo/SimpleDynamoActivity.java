package edu.buffalo.cse.cse486586.simpledynamo;

import android.database.Cursor;
import android.os.Bundle;
import android.app.Activity;
import android.os.StrictMode;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;
import android.net.Uri;

public class SimpleDynamoActivity extends Activity {

	private Button mLButton;
	private Button mRButton;
	private TextView mShowMsg;

	static final String TAG = SimpleDynamoActivity.class.getSimpleName();

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);

		//StrictMode.ThreadPolicy policy = new StrictMode.ThreadPolicy.Builder().permitAll().build();
		//StrictMode.setThreadPolicy(policy);

		mShowMsg = (TextView) findViewById(R.id.textView1);
		mShowMsg.setMovementMethod(new ScrollingMovementMethod());

		findViewById(R.id.button3).setOnClickListener(
				new OnTestClickListener(mShowMsg, getContentResolver()));

		mLButton = (Button) findViewById(R.id.button1);
		//Setting Listener for LDump
		mLButton.setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View v) {
				mShowMsg.setText("");
				Uri.Builder uriBuilder = new Uri.Builder();
				uriBuilder.authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
				uriBuilder.scheme("content");

				Log.v(TAG, "calling the query command...");
				Cursor resultCursor = getContentResolver().query(uriBuilder.build(), null, "@", null, null);
				Log.v(TAG, "Returned the result and now will write to UI...");
				if (resultCursor != null) {
					Log.v(TAG, "result curson is not null");
					if (resultCursor.moveToFirst()){
						do{
							String returnKey = resultCursor.getString(resultCursor.getColumnIndex(SimpleDynamoProvider.DynamoDbSchema.MessageTable.Cols.KEY));
							String returnValue = resultCursor.getString(resultCursor.getColumnIndex(SimpleDynamoProvider.DynamoDbSchema.MessageTable.Cols.VALUE));
							Log.v(TAG, "key , value: " + returnKey + ", " + returnValue);
							mShowMsg.append(returnKey + " : " + returnValue + "\n");
						}while(resultCursor.moveToNext());
					}
					resultCursor.close();
				}
				Log.v(TAG, "It should have printed the values.");
			}
		});

		mRButton = (Button) findViewById(R.id.button2);
		mRButton.setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View v) {
				mShowMsg.setText("");
				Uri.Builder uriBuilder = new Uri.Builder();
				uriBuilder.authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
				uriBuilder.scheme("content");

				Cursor resultCursor = getContentResolver().query(uriBuilder.build(), null, "*", null, null);
				if (resultCursor != null) {
					if (resultCursor.moveToFirst()){
						do{
							String returnKey = resultCursor.getString(resultCursor.getColumnIndex(SimpleDynamoProvider.DynamoDbSchema.MessageTable.Cols.KEY));
							String returnValue = resultCursor.getString(resultCursor.getColumnIndex(SimpleDynamoProvider.DynamoDbSchema.MessageTable.Cols.VALUE));
							mShowMsg.append(returnKey + " : " + returnValue + "\n");
						}while(resultCursor.moveToNext());
					}
					resultCursor.close();
				}
			}
		});
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");
	}

}
