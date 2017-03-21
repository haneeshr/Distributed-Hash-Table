package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;

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
import android.telephony.TelephonyManager;
import android.util.Log;


public class SimpleDhtProvider extends ContentProvider {
    static final String[] REMOTE_PORTS = {"11108","11112","11116","11120","11124"};
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static final String INSERT = "insert";
    static final String QUERY = "query";
    static final String DELETE = "delete";
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";



    SQLiteDatabase sqLiteDatabase;
    SQLiteOpenHelper sqLiteOpenHelper;
    String myport;
    String myhash;
    String succesor;
    String predecessor;

    LinkedList<String> hashvals;
    HashMap<String, String> hash_ports;

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub


        TelephonyManager tel = (TelephonyManager)getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length()-4);
        myport = String.valueOf(Integer.parseInt(portStr)*2);

        hash_ports = new HashMap<String, String>();
        hashvals = new LinkedList<String>();


        new init_ring().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);

        try{
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            serverSocket.setReuseAddress(true);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }catch (Exception e){
            Log.e(TAG, "onCreate: "+e.toString() );
        }


        try {
            myhash = genHash(myport);
        }catch (NoSuchAlgorithmException e){
            Log.e("onCreate: ", e.toString());
        }

        final String statement = "CREATE TABLE myDB ( key TEXT PRIMARY KEY, value TEXT)";;
        sqLiteOpenHelper = new SQLiteOpenHelper(getContext(),"myDB",null,1) {
            @Override
            public void onCreate(SQLiteDatabase db) {
                db.execSQL(statement);
            }

            @Override
            public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

            }
        };

        return true;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        String key = (String) values.get("key");
        String value = (String) values.get("value");
        String toPort = toPort(key);
        if(toPort.equals(myport)){
            sqLiteDatabase = sqLiteOpenHelper.getWritableDatabase();
            sqLiteDatabase.insertWithOnConflict("myDB", null, values, SQLiteDatabase.CONFLICT_REPLACE);
            Log.v("insert", values.toString());
        }else{
            String msg = INSERT + ":" + key + ":" + value;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, toPort, INSERT);
        }


        return null;
    }

    private String toPort(String key) {
        String port = hash_ports.get(hashvals.get(0));
        String hash = null;
        try{
            hash = genHash(key);
            //find the port to which this key is supposed to be inserted
            for(int i=0; i<hashvals.size(); i++){
                if(hash.compareTo(hashvals.get(i)) < 0){

                    return hash_ports.get(hashvals.get(i));
                }
            }
        }catch (Exception e){
            e.printStackTrace();
            Log.e(TAG, "toPort: error" );
        }

        return port;
    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub

        Cursor c=null;
        try{
            String query = "select * from myDB";
            String clause = " where key=\"" + selection + "\"";


            if(selection.equals("*")){
                //return all key-value pairs in entire DHT

                Log.i(TAG, "query: GDump");
                String msg = "query"+":"+"@";
                String res = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, null, QUERY).get();
                String split[] = res.split(":");
                Log.i(TAG, "query: split"+split.length);

                MatrixCursor matrixCursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});

                for(int i=0; i<split.length; i+=2){
                    matrixCursor.addRow(new String[] { split[i], split[i+1]});
                }

                return matrixCursor;
            }else if(selection.equals("@")){
                //return all in local table...so the query is not changed

                c = sqLiteDatabase.rawQuery(query, null);
                return c;

            }else{
                // Key in any one of the tables in the ring
                String inport = toPort(selection);
                if(myport.equals(inport)){
                    //Key in my table...return from here

                    query += clause;
                    c = sqLiteDatabase.rawQuery(query, null);
                    c.moveToNext();
                    return c;

                }else{
                    //key in other table...request other table
                    String msg = QUERY + ":" + selection;
                    String res = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, inport, QUERY).get();

                    String split[] = res.split(":");

                    MatrixCursor matrixCursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});

                    for(int i=0; i<split.length; i+=2){
                        matrixCursor.addRow(new String[] { split[i], split[i+1]});
                    }
                    return matrixCursor;
                }

            }
        }catch (Exception e){
            e.printStackTrace();
        }
        Log.v("query", selection);
        return c;

    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class init_ring extends AsyncTask<Void, Void, Void>{

        @Override
        protected Void doInBackground(Void... params) {

            int start = 5554;
            for(int i=0; i<5; i++, start+=2){
                try{
                    String hashval = genHash(String.valueOf(start));
//                    Log.i(TAG, "doInBackground: "+port_map.get(String.valueOf(start)));
                    hash_ports.put(hashval, String.valueOf(start*2));
                    hashvals.add(hashval);
                }catch (NoSuchAlgorithmException e){
                    Log.e("doInBackground: ", e.toString());
                }
            }

            Collections.sort(hashvals);
            succesor = hashvals.get((hashvals.indexOf(myhash)+1)%5);
            succesor = hash_ports.get(succesor);
            predecessor = hashvals.get((hashvals.indexOf(myhash)+4)%5);
            predecessor = hash_ports.get(predecessor);

//            Log.i("doInBackground: pred",predecessor);

            return null;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, Void, Void> {

        private Uri buildUri(String content, String s){
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(s);
            uriBuilder.scheme(content);
            return uriBuilder.build();
        }

        @Override
        protected Void doInBackground(ServerSocket... params) {
            ServerSocket serverSocket = params[0];
            Socket client;
            InputStream inputStream;
            OutputStream outputStream;
            BufferedReader bufferedReader;
            BufferedWriter bufferedWriter;

            try{
                while(true){
                    client = serverSocket.accept();
                    inputStream = client.getInputStream();
                    bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

                    String msgin = "";
//                    Log.i(TAG, "doInBackground server: waiting for msg");
                    msgin = bufferedReader.readLine();

//                    Log.i(TAG, "doInBackground server: received:"+msgin);
                    String op = msgin.split(":")[0];
                    if(op.equals(INSERT)){

                        String key = msgin.split(":")[1];
                        String value = msgin.split(":")[2];
                        ContentValues contentValues = new ContentValues();
                        contentValues.put("key", key);
                        contentValues.put("value", value);
                        insert(null, contentValues);
                    }else if(op.equals(QUERY)){
                        Cursor c = null;
                        String key = msgin.split(":")[1];
                        Log.i(TAG, "doInBackground: received==="+msgin);
                        c = query(null, null, key, null, null);
                        outputStream = client.getOutputStream();
                        bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));

                        int keyindex = c.getColumnIndex(KEY_FIELD);
                        int valueindex = c.getColumnIndex(VALUE_FIELD);
                        String msgout = "";
                        Log.i(TAG, "doInBackground: seding msg" + c.getCount());
                        c.moveToFirst();
                        while(!c.isAfterLast()){
                            msgout += c.getString(keyindex)+":";
                            if(c.isLast()){
                                msgout += c.getString(valueindex);
                            }else{
                                msgout += c.getString(valueindex)+":";
                            }
                            c.moveToNext();
                        }

                        bufferedWriter.write(msgout+"\n");
                        Log.i(TAG, "doInBackground: cursor sent");
                        bufferedWriter.flush();
                        bufferedWriter.close();
                        outputStream.flush();
                        outputStream.close();

                    }
                    bufferedReader.close();
                    inputStream.close();
                    client.close();
                }
            }catch (Exception e){

            }

            return null;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, String> {

        String res = "";

        @Override
        protected String doInBackground(String... params) {
            String msg = params[0];
            String remote_port = params[1];
            String op = params[2];

            Socket client;
            OutputStream outputStream;
            InputStream inputStream;
            BufferedReader bufferedReader;
            BufferedWriter bufferedWriter;


            if(op.equals(INSERT)){
                //insert to other avd
                //            Log.i(TAG, "doInBackground: sending msg");
                try {
                    client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remote_port));
//                Log.i(TAG, "doInBackground: connected to remote port");
                    outputStream = client.getOutputStream();
                    bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));

                    bufferedWriter.write(msg + "\n");
                    bufferedWriter.close();
                    outputStream.close();



                    client.close();
//                Log.i(TAG, "doInBackground: message sent"+msg);
                } catch (Exception e) {
                    Log.e(TAG, "doInBackground: "+e.toString() );
                }
            }else if(op.equals(QUERY)){
                //Query to other avd

                if(remote_port == null){
                    //get cursors from all remote avds

                    for(String remotePort: REMOTE_PORTS){
                            try{
                                client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
                                outputStream = client.getOutputStream();
                                bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));
                                inputStream = client.getInputStream();
                                bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

                                bufferedWriter.write(msg+"\n");
                                bufferedWriter.flush();

                                if(res.equals("")){
                                    res = bufferedReader.readLine();
                                }else{
                                    res += ":" + bufferedReader.readLine();
                                }

                                Log.i(TAG, "doInBackground: received gdump res"+res);
                                bufferedWriter.close();
                                outputStream.close();
                                bufferedReader.close();
                                inputStream.close();
                            }catch (Exception e){
                                Log.e(TAG, "doInBackground: client"+ e.toString() );
                            }
                        }
                    }
                else {
                    // get cursor from single remote avd

                    try {

                        client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2,}), Integer.parseInt(remote_port));
                        outputStream = client.getOutputStream();
                        bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));
                        inputStream = client.getInputStream();
                        bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

                        bufferedWriter.write(msg + "\n");
                        bufferedWriter.flush();


                        res = bufferedReader.readLine();

                        bufferedWriter.close();
                        outputStream.flush();
                        outputStream.close();

                    } catch (Exception e) {
                        Log.e(TAG, "doInBackground: client" + e.toString());
                    }
                }
            }else if(op.equals(DELETE)){
                //Delete from other avd

            }

            return res;
        }
    }
}
