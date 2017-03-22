package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
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
    static final int INSERT = 0;
    static final int QUERY = 1;
    static final int DELETE = 2;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private static final String MY_DB = "myDB";




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

        final String statement = "CREATE TABLE " + MY_DB + " ( key TEXT PRIMARY KEY, value TEXT)";;
        sqLiteOpenHelper = new SQLiteOpenHelper(getContext(),MY_DB,null,1) {
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

        Log.i(TAG, "insert");
        String key = (String) values.get("key");
        String value = (String) values.get("value");
        String msg = INSERT + ":" + key + ":" + value;


        try {
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, String.valueOf(INSERT)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
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
        String query = "select * from " + MY_DB ;
        String clause = " where key=\"" + selection + "\"";

        if(selection.equals("@")){
            c = sqLiteDatabase.rawQuery(query, null);
        }else if(selection.equals("*")){
            String msg = QUERY + ":@";
            try {
                String res = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, String.valueOf(QUERY)).get();
                String split[] = res.split(":");
                MatrixCursor matrixCursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});

                for(int i=0; i<split.length; i+=2){
                    matrixCursor.addRow(new String[] { split[i], split[i+1]});
                }
                return matrixCursor;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }else{
            String msg = QUERY + ":" + selection;
            try {
                String res = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, String.valueOf(QUERY)).get();
                if(res == null){
                    query += clause;
                    c = sqLiteDatabase.rawQuery(query, null);
                }else {
                    Log.i(TAG, "query: res:"+res);
                    MatrixCursor matrixCursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
                    matrixCursor.addRow(new String[] {res.split(":")[0], res.split(":")[1]});
                    return matrixCursor;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
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
                    outputStream = client.getOutputStream();
                    bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));

                    String msgin = "";
//                    Log.i(TAG, "doInBackground server: waiting for msg");
                    msgin = bufferedReader.readLine();

//                    Log.i(TAG, "doInBackground server: received:"+msgin);
                    int op = Integer.parseInt(msgin.split(":")[0]);
                    if(op == INSERT){

                        String key = msgin.split(":")[1];
                        String value = msgin.split(":")[2];
                        ContentValues contentValues = new ContentValues();
                        contentValues.put("key", key);
                        contentValues.put("value", value);
                        sqLiteDatabase = sqLiteOpenHelper.getWritableDatabase();
                        sqLiteDatabase.insertWithOnConflict(MY_DB, null, contentValues, SQLiteDatabase.CONFLICT_REPLACE);
                        outputStream = client.getOutputStream();
                        bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));

                        bufferedWriter.write("inserted\n");
                        bufferedWriter.flush();
                        bufferedWriter.close();
                        outputStream.close();
                    }else if(op == QUERY){


                        Cursor c = null;
                        String key = msgin.split(":")[1];
                        String query = "select * from " + MY_DB ;
                        String clause = " where key=\"" + key + "\"";
                        Log.i(TAG, "doInBackground: received==="+msgin);
                        if(!key.equals("@")){
                            query += clause;
                        }
                        c = sqLiteDatabase.rawQuery(query, null);


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

                        Log.i(TAG, "doInBackground: "+msgout);
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
            int op = Integer.parseInt(params[1]);

            String key;
            String value;
            String hash = "";


            switch (op){
                case INSERT:
                    boolean inserted = false;
                    key = msg.split(":")[1];
                    value = msg.split(":")[2];

                    try {
                        hash = genHash(key);
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }

                    for(String hashval : hashvals){
                        if(hash.compareTo(hashval) < 0){
                            Log.i(TAG, "doInBackground: trying");
                            if(tryinsert(msg, hash_ports.get(hashval))){
                                inserted = true;
                                break;
                            }
                        }
                    }

                    if(!inserted){
                        Log.i(TAG, "doInBackground: not yet inserted");
                        for(String hashval : hashvals){
                            if(tryinsert(msg, hash_ports.get(hashval))){
                                break;
                            }
                        }
                    }

                    Log.i(TAG, "doInBackground: inserted");
                    break;

                case QUERY:

                    boolean queried = false;
                    key = msg.split(":")[1];

                    try {
                        hash = genHash(key);
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }

                    if(key.equals("@")){
                        res = "";
                        for(String port : REMOTE_PORTS){
                            String resp = tryquery(msg, port);
                            if(resp != null){
                                if(res.equals("")){
                                    res += resp;
                                }else{
                                    res += ":" + resp;
                                }
                            }
                        }

                    }else{
                        for(String hashval : hashvals){
                            res = null;
                            if(hash.compareTo(hashval)<0){
                                res = tryquery(msg, hash_ports.get(hashval));
                                if(res != null){
                                    return res;
                                }
                            }
                        }

                        for(String hashval : hashvals){
                            res = tryquery(msg, hash_ports.get(hashval));
                            if(res != null){
                                break;
                            }
                        }
                    }
                    return res;

                case DELETE:

                    break;
            }

            return res;
        }

        private String tryquery(String msg, String port) {
            String res = null;
            InputStream inputStream;
            OutputStream outputStream;
            BufferedReader bufferedReader;
            BufferedWriter bufferedWriter;
            Socket socket = null;
            try {
                 socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));

                inputStream = socket.getInputStream();
                outputStream = socket.getOutputStream();
                bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));

                bufferedWriter.write(msg + "\n");
                bufferedWriter.flush();

                res = bufferedReader.readLine();
                bufferedWriter.close();
                bufferedReader.close();
                outputStream.close();
                inputStream.close();
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }


            return res;
        }

        private boolean tryinsert(String msg, String port) {

            InputStream inputStream;
            OutputStream outputStream;
            BufferedReader bufferedReader;
            BufferedWriter bufferedWriter;
            try {
                Log.i(TAG, "tryinsert: connecting");

//                Socket socket = new Socket();
//                socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port)), 100);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0 ,2, 2}), Integer.parseInt(port));
                Log.i(TAG, "tryinsert: connected");

                inputStream = socket.getInputStream();
                outputStream = socket.getOutputStream();

                bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));

                Log.i(TAG, "tryinsert: sending insert msg");
                bufferedWriter.write(msg+"\n");
                bufferedWriter.flush();

                String rep = bufferedReader.readLine();

                bufferedReader.close();
                bufferedWriter.close();
                inputStream.close();
                outputStream.close();
                socket.close();
                
                
                if (!(rep == null)) {
                    Log.i(TAG, "tryinsert: alive");
                    return true;
                }
                Log.i(TAG, "tryinsert: dead");
                return false;

            } catch (Exception e) {
//                e.printStackTrace();
            }

            return false;
        }
    }
}
