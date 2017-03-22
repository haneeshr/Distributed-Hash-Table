package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.view.Menu;
import android.view.View;
import android.widget.TextView;


public class SimpleDhtActivity extends Activity {

    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    ContentResolver contentResolver;
    Uri mUri;

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);

        contentResolver = getContentResolver();
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        
        final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));

        findViewById(R.id.button2).setOnClickListener(
                new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {

                        Cursor cursor = contentResolver.query(mUri, null, "*", null, null);
                        cursor.moveToFirst();
                        int keyindex = cursor.getColumnIndex(KEY_FIELD);
                        int valueindex = cursor.getColumnIndex(VALUE_FIELD);
                        while(!cursor.isAfterLast()){
                            tv.append(cursor.getString(keyindex) + "=====" + cursor.getString(valueindex)+ "\n");
                            cursor.moveToNext();
                        }
                    }
                }
        );

        findViewById(R.id.button1).setOnClickListener(
                new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {
                        Cursor cursor = contentResolver.query(mUri, null, "@", null, null);
                        tv.append(String.valueOf(cursor.getCount())+"\n");
                    }
                }
        );
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

}
