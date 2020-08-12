package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

import android.annotation.SuppressLint;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class SimpleDynamoProvider extends ContentProvider {

	MatrixCursor cursor;
	MatrixCursor cursor1;
    MatrixCursor comebackcursor = new MatrixCursor(new String[] { "key","value"});;
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	ReentrantLock read1 = new ReentrantLock();
	String read = "0";
	String msgHash;

	static final int SERVER_PORT = 10000;
	//static final String TAG = SimpleDhtProvider.class.getSimpleName();
	HashMap<String, String> mssg = new HashMap<String, String>();
	HashMap<String, Integer> portsmap = new HashMap<String, Integer>();
	HashMap<String, Integer> avdmap = new HashMap<String, Integer>();
	HashMap<String, Integer> avdhashmap = new HashMap<String, Integer>();
	HashMap<String, String> currNodes = new HashMap<String, String>();
	HashMap<String, String> Nodes = new HashMap<String, String>();
	HashMap<String, String> querstarreply = new HashMap<String, String>();
	ArrayList<String> seq = new ArrayList<String>();
	ArrayList<String> seq2 = new ArrayList<String>();
	ArrayList<String> avdseq = new ArrayList<String>();
	ConcurrentHashMap<String,String> chm = new ConcurrentHashMap<String, String>();
	HashMap<String,String> nhm = new HashMap<String, String>();
	String deleteflag ="0";
	String queryvalue="null";
	int currentavdseq;

	String myportclient = null;
	String hash=null;
	String[] remote_ports;

	private final Uri mUri;
	int i1=0;
	String joining = "no";
	DatabaseHandler db;


	Comparator<HashMap<String, String>> cmp = new Comparator<HashMap<String, String>>() {
		@Override
		public int compare(HashMap<String, String> t1, HashMap<String, String> t2) {

			return t1.get("hash").compareTo(t2.get("hash"));
		}
	};

	// priority queue for storing all the mssg(hashmap) in agree seq wise (lowest agree seq will be on th top)
	PriorityQueue<HashMap<String, String> > pq = new
			PriorityQueue<HashMap<String, String>>(25, cmp);
	PriorityQueue<HashMap<String, String> > pqnew = new
			PriorityQueue<HashMap<String, String>>(25, cmp);
	Socket[] socketarray1 = new Socket[5];
	Socket[] socketarray2 = new Socket[5];

	public SimpleDynamoProvider() {
		//mTextView = _tv;
		//mContentResolver = _cr;
		mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.SimpleDhtProvider");
		//   mContentValues = initTestValues();
		portsmap.put("11108",0);
		portsmap.put("11112",1);
		portsmap.put("11116",2);
		portsmap.put("11120",3);
		portsmap.put("11124",4);


		remote_ports  = new String[]{"11108", "11112", "11116", "11120","11124"};
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		Log.v(TAG,"Inside Delete"+uri +" "+selection+" "+selectionArgs);
		deleteflag="1";
		db.deletetable();
		cursor = new MatrixCursor(new String[] { "key","value"});
		
		String pre;
		String suc;
		String myself = String.valueOf(portsmap.get(myportclient));
		int i;
		Log.v(TAG,"Finding the pre n suc in avdseq size="+avdseq.size());
		for(i=0;i<avdseq.size();i++){
			if(avdseq.get(i).equals(myself)){
				break;
			}
		}
		currentavdseq = i;
		if(i==0){
			pre = avdseq.get(4);
			suc = avdseq.get(i+1);}
		else if(i==4){
			pre = avdseq.get(i-1);
			suc = avdseq.get(0);
		}else {
			pre = avdseq.get(i-1);
			suc = avdseq.get(i+1);
		}



		String preport = remote_ports[Integer.valueOf(pre)];
		String sucport = remote_ports[Integer.valueOf(suc)];
		Log.v(TAG,"Order of all the avds = "+avdseq+" pre n port = "+pre + " "+preport+" suc n port = "+suc+" "+sucport);
		String requesttype = "delete";
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, requesttype, preport,sucport,selection);

		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	//HashMap<String,String> phasesixhm = new HashMap<String, String>();

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		Log.v(TAG,"insert query = " +values.toString()+" myportclient = "+myportclient);

		//phasesixhm.put(String.valueOf(values.get("key")),String.valueOf(values.get("value")));

		/*try{*/
		if(myportclient.equals(values.get("port")) && values.get("requesttype").equals("selfinsert")){

			Log.v(TAG,"received the insert request from Server to insert in its own cursor = "+values.toString());

			read1.lock();

			read1.unlock();
				try {

					int ver=0;
					//String ver;
					try {
						try {
							Cursor cr = db.getMaxVersion(values);
							ver = Integer.valueOf(cr.getString(cr.getColumnIndex("ver")));
							cr.moveToFirst();
							String value = cr.getString(cr.getColumnIndex("value"));

							Log.v(TAG, "OLD  value in db for key = " + values.get("key") + " " + value+ " "+ ver);
							if (cr == null) {
								ver = 0;
							}
						}catch(Exception ex){
							ex.printStackTrace();
							ver=0;
						}
						ver = ver+1;
						values.put("ver",String.valueOf(ver));
						Log.v(TAG, "the version of the key = " + values.get("key") + " going to be = " + ver);
					}catch(Exception ex){
						ex.printStackTrace();
					}
					//read1.lock();
					try {
						Log.v(TAG,"content values = "+values);
						db.addKeyPair(values);
					}catch(Exception ex){
						ex.printStackTrace();
					}
					//read1.unlock();
					try {
						Cursor cr1 = db.getMaxVersion(values);
						cr1.moveToFirst();
						String value1 = cr1.getString(cr1.getColumnIndex("value"));
						String version = cr1.getString(cr1.getColumnIndex("ver"));
						Log.v(TAG, "updated value in db for key = " + values.get("key") + " " + value1+" "+version);
					}catch(Exception ex){
						ex.printStackTrace();
					}

						if(chm.containsKey(values.get("key"))) {
							Log.v(TAG,"Contains the old value = "+chm.get(values.get("key"))+" new value = "+values.get("value"));
							String removeresult = chm.remove(values.get("key"));
							Log.v(TAG,"removeresult = "+removeresult);
							chm.put(String.valueOf(values.get("key")), String.valueOf(values.get("value")));
							Log.v(TAG,"updated value in db for key = "+values.get("key")+ " "+db.getValue(String.valueOf(values.get("key"))));
						}
						if(nhm.containsKey(values.get("key"))) {
							Log.v(TAG,"Contains the old value = "+chm.get(values.get("key"))+" new value = "+values.get("value"));
							String removeresult1 = nhm.remove(values.get("key"));
							Log.v(TAG,"removeresult1 = "+removeresult1);
							nhm.put(String.valueOf(values.get("key")), String.valueOf(values.get("value")));
						}
					} catch (Exception ex) {
						ex.printStackTrace();
					} finally {
					Log.v(TAG, "In Finally block ...inserted for key=" + values.get("key"));
						//write.unlock();
					}



/*
					try {
*/

						/*MatrixCursor cursortmp = new MatrixCursor(new String[]{"key", "value", "avd1", "avd2", "avd3"});
						//MatrixCursor.RowBuilder rowBuildertmp1 = cursor.newRow();
						//rowBuildertmp1.add("key",values.get("key")).add("value",values.get("value")).add("avd1",values.get("avd1")).add("avd2",values.get("avd2")).add("avd3",values.get("avd3"));
						String found = "no";
						if (cursor != null) {

							if (cursor.getCount() > 0) {
								Log.v(TAG, "Going to copy from cursor to cursortmp");
								cursor.moveToFirst();


								do {
									int columnIndex = cursor.getColumnIndex("key");
									String word = cursor.getString(columnIndex);
									int columnIndex1 = cursor.getColumnIndex("value");
									String value = cursor.getString(columnIndex1);
									int columnIndexavd1 = cursor.getColumnIndex("avd1");
									String avd1 = cursor.getString(columnIndexavd1);
									int columnIndexavd2 = cursor.getColumnIndex("avd2");
									String avd2 = cursor.getString(columnIndexavd2);
									int columnIndexavd3 = cursor.getColumnIndex("avd3");
									String avd3 = cursor.getString(columnIndexavd3);

									if (word.equals(values.get("key"))) {
										found = "yes";
										Log.v(TAG, "key is found" + word + " old value = " + value + " new value = " + values.get("value"));
										//cursor1 = new MatrixCursor(new String[]{"key", "value","avd1","avd2","avd3"});
										MatrixCursor.RowBuilder rowBuildertmp = cursortmp.newRow();
										rowBuildertmp.add("key", values.get("key")).add("value", values.get("value")).add("avd1", values.get("avd1")).add("avd2", values.get("avd2")).add("avd3", values.get("avd3"));
									} else {
										MatrixCursor.RowBuilder rowBuilder1 = cursortmp.newRow();
										rowBuilder1.add("key", word).add("value", value).add("avd1", avd1).add("avd2", avd2).add("avd3", avd3);
									}
								} while (cursor.moveToNext());
							} else {
								Log.v(TAG, "Inserting the 1st value in original cursor");
								//cursor.
								MatrixCursor.RowBuilder rowBuildertmp1 = cursor.newRow();
								rowBuildertmp1.add("key", values.get("key")).add("value", values.get("value")).add("avd1", values.get("avd1")).add("avd2", values.get("avd2")).add("avd3", values.get("avd3"));
							}
							//cursor.close();
						}

						//Log.v(TAG,"cursortmp values In insert for key = "+values.get("key")+ " = "+DatabaseUtils.dumpCursorToString(cursortmp));
						//Log.v(TAG,"cursor values In insert for key = "+values.get("key")+ " = "+DatabaseUtils.dumpCursorToString(cursor));

						if (found.equals("no")) {
							MatrixCursor.RowBuilder rowBuildertmp1 = cursor.newRow();
							rowBuildertmp1.add("key", values.get("key")).add("value", values.get("value")).add("avd1", values.get("avd1")).add("avd2", values.get("avd2")).add("avd3", values.get("avd3"));

						} else {
							if (cursortmp != null) {
								if (cursortmp.getCount() > 0) {
									cursor = new MatrixCursor(new String[]{"key", "value", "avd1", "avd2", "avd3"});
									Log.v(TAG, "Going to copy from cursortmp to cursor");

									cursortmp.moveToFirst();


									do {
										int columnIndex = cursortmp.getColumnIndex("key");
										String word = cursortmp.getString(columnIndex);
										int columnIndex1 = cursortmp.getColumnIndex("value");
										String value = cursortmp.getString(columnIndex1);
										int columnIndexavd1 = cursortmp.getColumnIndex("avd1");
										String avd1 = cursortmp.getString(columnIndexavd1);
										int columnIndexavd2 = cursortmp.getColumnIndex("avd2");
										String avd2 = cursortmp.getString(columnIndexavd2);
										int columnIndexavd3 = cursortmp.getColumnIndex("avd3");
										String avd3 = cursortmp.getString(columnIndexavd3);
										MatrixCursor.RowBuilder rowBuilder = cursor.newRow();
										rowBuilder.add("key", word).add("value", value).add("avd1", avd1).add("avd2", avd2).add("avd3", avd3);


										//Log.v(TAG,"Key and Value in Cursor ="+word+" "+value+" "+" "+avd1+ " "+avd2+ " "+avd3);
									} while (cursortmp.moveToNext());
								}
								//cursortmp.close();
							}
						}


					return uri;*/


				/*	}catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			Log.v(TAG, "In Finally block ...inserted for key=" + values.get("key"));

		}*/
		}else{
			Log.v(TAG,"insert Finding the correct AVD");
			try {
				msgHash = genHash((String) values.get("key"));
				Log.v(TAG, "hash value of key in insert = " + msgHash);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}

			ArrayList<String> arr = new ArrayList<String>();

			int count =0;
			for(String s : seq2){
				arr.add(s);
				count++;
			}
			arr.add(msgHash);
			count++;

			Collections.sort(arr);

			/*for(String a: arr){
				Log.v(TAG,a);
			}*/
			int position=0;
			for(String a : arr){

				if(a.equals(msgHash))
					break;

				position++;
			}
			Log.v(TAG,"position="+position);
			Log.v(TAG,"count of avds+msgHash ="+count);

			if(position == count-1){
				position =0;
			}

			int avd1;
			int avd2;

			if(position == count-2){
				avd1=0;
				avd2=(avd1+1);
			}else if(position == count-3){
				avd1 = (position+1);
				avd2 =0;
			}else{
				avd1 = (position+1);
				avd2 = (avd1+1);
			}

			String hash = seq2.get(position);
			String avd = currNodes.get(hash);

			String hash1 = seq2.get(avd1);
			String savd1 = currNodes.get(hash1);

			String hash2 = seq2.get(avd2);
			String savd2 = currNodes.get(hash2);


			String requesttype = "insert";
			String msg = (String) values.get("value");
			String key = (String) values.get("key");
			String msgHash1 = msgHash;
			String myPort1 = remote_ports[Integer.parseInt(avd)];
			String savd01 = remote_ports[Integer.parseInt(savd1)];
			String savd02 = remote_ports[Integer.parseInt(savd2)];

			//Log.v(TAG,"seq of all the nodes="+seq2);
			Log.v(TAG," key= "+values.get("key")+" value= "+values.get("value")+" hash of key= "+msgHash+ "sending to AVD = "+avd+" "+myPort1+" "+savd1+" "+savd01+" "+savd2+" "+savd02);


			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, requesttype,msg, key,msgHash1,myPort1,savd01,savd02);


		}
		// return uri;
		return uri;
	}

	@Override
	public boolean onCreate() {
		Log.v(TAG,"Going to acquire a lock");
		//read1.lock();
		// TODO Auto-generated method stub
		portsmap.put("11108", 0);
		portsmap.put("11112", 1);
		portsmap.put("11116", 2);
		portsmap.put("11120", 3);
		portsmap.put("11124", 4);


		avdmap.put("5554",0);
		avdmap.put("5556",1);
		avdmap.put("5558",2);
		avdmap.put("5560",3);
		avdmap.put("5562",4);
		db = new DatabaseHandler(getContext());
		remote_ports = new String[]{"11108", "11112", "11116", "11120", "11124"};


		// TODO Auto-generated method stub
		cursor = new MatrixCursor(new String[]{"key", "value","avd1","avd2","avd3"});

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));


		myportclient = myPort;
		try {
			hash = genHash(portStr);
			Log.v(TAG, "my port =" + myportclient + " " + portStr + " my hash value is = " + hash);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		for (HashMap.Entry<String, Integer> entry : avdmap.entrySet()) {
			try{
				String hash = genHash(entry.getKey());
				avdhashmap.put(hash,entry.getValue());
				currNodes.put(hash, String.valueOf(entry.getValue()));
				seq2.add(hash);
			}catch(Exception e){
				e.printStackTrace();
			}

		}



		Collections.sort(seq2);
		joining="yes";

		for (String h: seq2) {
			avdseq.add(currNodes.get(h));
		}

		String pre;
		String suc;
		String myself = String.valueOf(portsmap.get(myportclient));
		int i;
		Log.v(TAG,"Finding the pre n suc in avdseq size="+avdseq.size());
		for(i=0;i<avdseq.size();i++){
			if(avdseq.get(i).equals(myself)){
				break;
			}
		}
		currentavdseq = i;
		if(i==0){
		pre = avdseq.get(4);
		suc = avdseq.get(i+1);}
		else if(i==4){
			pre = avdseq.get(i-1);
			suc = avdseq.get(0);
		}else {
			pre = avdseq.get(i-1);
			suc = avdseq.get(i+1);
		}



		String preport = remote_ports[Integer.valueOf(pre)];
		String sucport = remote_ports[Integer.valueOf(suc)];
		Log.v(TAG,"Order of all the avds = "+avdseq+" pre n port = "+pre + " "+preport+" suc n port = "+suc+" "+sucport);

		String msg = hash;

		try {
			/*
			 * Create a server socket as well as a thread (AsyncTask) that listens on the server
			 * port.
			 *
			 * AsyncTask is a simplified thread construct that Android provides. Please make sure
			 * you know how it works by reading
			 * http://developer.android.com/reference/android/os/AsyncTask.html
			 */
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

		} catch (IOException e) {
			/*
			 * Log is a good way to debug your code. LogCat prints out all the messages that
			 * Log class writes.
			 *
			 * Please read http://developer.android.com/tools/debugging/debugging-projects.html
			 * and http://developer.android.com/tools/debugging/debugging-log.html
			 * for more information on debugging.
			 */
			Log.e(TAG, "Can't create a ServerSocket");

			//return;
		}

		//for joining client will send the request to server
		//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);

		String requesttype = "iamback";
		//if(deleteflag.equals("0")) {
		Log.v(TAG,"Sending a comeback request from Oncreate ");
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, requesttype, preport, sucport);

		//}
		//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, requesttype, preport,sucport);


		return true;
	}

ConcurrentHashMap<String,String> queryvaluehm = new ConcurrentHashMap<String, String>();
	public ConcurrentHashMap<String,String> queryFun(String requesttype,String msgHash1,String myPort1,String key,String avdpre,String avdsuc,String avdpre2,String avdsuc2){
		queryvaluehm = new ConcurrentHashMap<String, String>();
		try {
			queryvalue = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, requesttype,msgHash1,myPort1,key,avdpre,avdsuc,avdpre2,avdsuc2).get();
			queryvaluehm.put(key,queryvalue);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}


		while(true){
			if(!queryvaluehm.isEmpty() && !queryvaluehm.get(key).equals("null")){
				Log.v(TAG,"Got the value = "+queryvalue+" for key = "+key);
				return queryvaluehm;
			}
		}

	}


	String ans = "null";
	public HashMap queryFun2(String requesttype){
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, requesttype);
		querstarreply = new HashMap<String, String>();
		ans="null";
		while(true){
			if(!ans.equals("null")){
				Log.v(TAG,"Got all the key n Value in HashMap");
				return querstarreply;
			}
		}
	}

	HashMap<String,String> allavdskeys = new HashMap<String, String>();

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		Log.v(TAG,"Inside Query "+uri+" "+projection+" "+selection+" "+selectionArgs+" "+sortOrder);

		if(!selection.equals("null") && !selection.equals("@") && !selection.equals("*")){
			try {
				String ans="no";
				if(!selection.equals("null") ) {

					read1.lock();
					read1.unlock();


                    /*try{
						if(phasesixhm.containsKey(selection)){
							cursor1 = new MatrixCursor(new String[]{"key", "value"});
							MatrixCursor.RowBuilder rowBuilder1 = cursor1.newRow();
							rowBuilder1.add("key", selection).add("value", chm.get(selection));
							ans = "yes";
							phasesixhm.remove(selection);
							return cursor1;
						}
					}catch(Exception ex){
                    	ex.printStackTrace();
					}*/

					//Log.v(TAG,"status of lock = "+read.isLocked());
						try {
							if (chm.containsKey(selection)) {
								Log.v(TAG,"found the key in chm = "+ selection);
								cursor1 = new MatrixCursor(new String[]{"key", "value"});
								MatrixCursor.RowBuilder rowBuilder1 = cursor1.newRow();
								rowBuilder1.add("key", selection).add("value", chm.get(selection));
								ans = "yes";
								return cursor1;
							}
						} catch (Exception ex) {
							ex.printStackTrace();
						}

					try {
						if (nhm.containsKey(selection)) {
							Log.v(TAG,"found the key in chm = "+ selection);
							cursor1 = new MatrixCursor(new String[]{"key", "value"});
							MatrixCursor.RowBuilder rowBuilder1 = cursor1.newRow();
							rowBuilder1.add("key", selection).add("value", nhm.get(selection));
							ans = "yes";
							return cursor1;
						}
					} catch (Exception ex) {
						ex.printStackTrace();
					}

					//read1.lock();
					//read1.unlock();
					//read1.lock();
						try{

							Cursor cr = db.getValue(selection);

							if(cr!=null) {
								cr.moveToFirst();
								Log.v(TAG,"found the key in database = "+ selection +" = "+cr.getString(cr.getColumnIndex("value")));
								cursor1 = new MatrixCursor(new String[]{"key", "value"});
								MatrixCursor.RowBuilder rowBuilder1 = cursor1.newRow();
								rowBuilder1.add("key", selection).add("value", cr.getString(cr.getColumnIndex("value")));
								ans = "yes";
								return cursor1;
							}
						}catch (Exception ex){
							ex.printStackTrace();
						}
					//read1.unlock();

					/*try {
						String requesttype = "query*";

						if(seq2.isEmpty()){
							return cursor;
						}else {
							if(allavdskeys.isEmpty())
							{
								allavdskeys = queryFun2(requesttype);
							}
							//Log.v(TAG,"reply="+reply);
							MatrixCursor cursortmp1 = new MatrixCursor(new String[]{"key", "value"});
							if(allavdskeys.containsKey(selection)) {
								MatrixCursor.RowBuilder rowBuilder1 = cursor1.newRow();
								rowBuilder1.add("key", selection).add("value", allavdskeys.get(selection));
								ans = "yes";
								return cursortmp1;
							}
						}
					}catch(Exception e){
						e.printStackTrace();
					}*/
					String word1="";
						/*try {
							if (cursor != null) {
								if (cursor.getCount() > 0) {
									cursor.moveToFirst();
									int columnIndex = cursor.getColumnIndex("key");
									//String ans="no";
									do {
										String word = cursor.getString(columnIndex);
										//Log.v("key=", word);
										if (word.equals(selection)) {
											ans = "yes";

											int columnIndex1 = cursor.getColumnIndex("value");
											word1 = cursor.getString(columnIndex1);
											//Log.v("value=", word1);
											Log.v(TAG, "key is found" + word + " value = " + word1);
											cursor1 = new MatrixCursor(new String[]{"key", "value"});
											MatrixCursor.RowBuilder rowBuilder1 = cursor1.newRow();
											rowBuilder1.add("key", word).add("value", word1);

											break;
										}
									} while (cursor.moveToNext());
								}
								//cursor.close();
							}
						}catch(Exception ex){
							ex.printStackTrace();
						}*/
					//break;

					if(ans.equals("yes")) {
						Log.v(TAG,"Found the key here and returning now for key"+ selection +" value ="+ word1);
						return cursor1;
					}
				}
				if(ans.equals("no")){
					Log.v(TAG, "Finding the AVD for query key =" + selection);
					String hash = genHash(selection);

					ArrayList<String> arr = new ArrayList<String>();

					int count = 0;
					for (String s : seq2) {
						arr.add(s);
						count++;
					}

					arr.add(hash);
					count++;
					Log.v(TAG, "count of avds+msgHash =" + count);
					Collections.sort(arr);

					/*for (String a : arr) {
						Log.v(TAG, a);
					}*/
					int position = 0;
					for (String a : arr) {

						if (a.equals(hash))
							break;

						position++;
					}
					Log.v(TAG, "position=" + position);
					if (position == count - 1) {
						position = 0;
					}

					String hash1 = seq2.get(position);
					String avd = currNodes.get(hash1);
					int pre=-1;
					int pre2=-1;
					int suc=-1;
					int suc2=-1;
					if(avd.equals("0")){
						pre=1;
						pre2=4;
						suc =2;
						suc2 =3;
					}else if(avd.equals("1")){
						pre=4;
						pre2=3;
						suc =0;
						suc2 =2;
					}else if(avd.equals("2")){
						pre=0;
						pre2=1;
						suc =3;
						suc2 =4;
					}else if(avd.equals("3")){
						pre=2;
						pre2=0;
						suc =4;
						suc2 =1;
					}else if(avd.equals("4")) {
						pre = 3;
						pre2 = 2;
						suc = 1;
						suc2 = 0;
					}
					//String hashpre = seq2.get(pre);
					//String avdpre = currNodes.get(hashpre);
					String avdpre = String.valueOf(pre);
					String avdpre2 = String.valueOf(pre2);
					String avdsuc = String.valueOf(suc);
					String avdsuc2 = String.valueOf(suc2);
					Log.v(TAG, "key = "+selection+" hash of key= " + hash + "sending to AVD = " + avd + " " + remote_ports[Integer.parseInt(avd)]);

					String requesttype = "query";
					String key = selection;
					String msgHash1 = hash;
					String myPort1 = remote_ports[Integer.parseInt(avd)];

					ConcurrentHashMap<String,String> value = queryFun(requesttype, msgHash1, myPort1, key,avdpre,avdsuc,avdpre2,avdsuc2);

					Log.v(TAG, "got the query reply for key ="+  value);
					MatrixCursor cursortmp = new MatrixCursor(new String[]{"key", "value"});
					MatrixCursor.RowBuilder rowBuilder1 = cursortmp.newRow();
					rowBuilder1.add("key", selection).add("value", value.get(selection));
					return cursortmp;
				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		if(selection.equals("*")){
			Log.v(TAG,"going to return all the values (*)");
			try {
				String requesttype = "query*";

				if(seq2.isEmpty()){
					return cursor;
				}else {
					 allavdskeys = queryFun2(requesttype);
					//Log.v(TAG,"reply="+reply);
					MatrixCursor cursortmp1 = new MatrixCursor(new String[]{"key", "value"});
					for (HashMap.Entry<String, String> entry : allavdskeys.entrySet()) {
						//querstarreply.put(entry.getKey(),entry.getValue());
						MatrixCursor.RowBuilder rowBuilder1 = cursortmp1.newRow();
						rowBuilder1.add("key", entry.getKey()).add("value", entry.getValue());
					}

					return cursortmp1;
				}
			}catch(Exception e){
				e.printStackTrace();
			}

		}
		else if(selection.equals("@")){
			Log.v(TAG,"going to return all the values (@)");
			//Log.v(TAG,"cursor="+cursor);
			cursor1 = new MatrixCursor(new String[]{"key", "value"});
			HashMap<String,String> hmat = new HashMap<String, String>();
			HashMap<String,String> hmat2 = new HashMap<String, String>();
			HashMap<String,String> hmat3 = new HashMap<String, String>();
			Cursor dbcursor = db.getAllloaclKeys();
			Log.v(TAG,"dbcursor = "+dbcursor.getCount()+" "+dbcursor.getColumnName(0));
			MatrixCursor localcursor = new MatrixCursor(new String[]{"key", "value"});
			try {
                if (dbcursor != null) {
                    if (dbcursor.getCount() > 0) {
                        dbcursor.moveToFirst();
                        do {
                            int columnIndex = dbcursor.getColumnIndex("key1");
                            String word = dbcursor.getString(columnIndex);
                            //Log.v("key=", word);

                            int columnIndex1 = dbcursor.getColumnIndex("value");
                            String word1 = dbcursor.getString(columnIndex1);
                            MatrixCursor.RowBuilder rowBuilder1 = localcursor.newRow();
                            rowBuilder1.add("key", word).add("value", word1);
                            hmat2.put(word, word1);
                            //Log.v(TAG,"key value rowise = "+hmat);

                            //Log.v(TAG, " key= " + word + " value= " + word1);
                        } while (dbcursor.moveToNext());
                    }
                    dbcursor.close();
                }

               // Log.v(TAG,"localcursor from db @= "+hmat2);
            }catch(Exception ex){
			    ex.printStackTrace();
            }

			if (cursor != null) {
				if (cursor.getCount() > 0) {
					cursor.moveToFirst();
					do {
						int columnIndex = cursor.getColumnIndex("key");
						String word = cursor.getString(columnIndex);
						//Log.v("key=", word);

						int columnIndex1 = cursor.getColumnIndex("value");
						String word1 = cursor.getString(columnIndex1);
						MatrixCursor.RowBuilder rowBuilder1 = cursor1.newRow();
						rowBuilder1.add("key", word).add("value", word1);
						hmat.put(word,word1);
						//Log.v(TAG,"key value rowise = "+hmat);

						//Log.v(TAG, " key= " + word + " value= " + word1);
					}while(cursor.moveToNext());
				}
				cursor.close();
			}

            /*if (comebackcursor != null) {
                if (comebackcursor.getCount() > 0) {
                    comebackcursor.moveToFirst();
                    do {
                        int columnIndex = comebackcursor.getColumnIndex("key");
                        String word = comebackcursor.getString(columnIndex);
                        //Log.v("key=", word);

                        int columnIndex1 = comebackcursor.getColumnIndex("value");
                        String word1 = comebackcursor.getString(columnIndex1);

						MatrixCursor.RowBuilder rowBuilder2 = localcursor.newRow();
						rowBuilder2.add("key", word).add("value", word1);

                        MatrixCursor.RowBuilder rowBuilder1 = cursor1.newRow();
                        rowBuilder1.add("key", word).add("value", word1);
                        hmat3.put(word,word1);
                        //Log.v(TAG,"key value rowise = "+hmat);

                        //Log.v(TAG, " key= " + word + " value= " + word1);
                    }while(comebackcursor.moveToNext());
                }

				Log.v(TAG,"localcursor from comeback  = "+hmat3);

                comebackcursor.close();
            }*/
			//Log.v(TAG,"cursor = "+cursor.toString());
            Log.v(TAG,"count of rows @ = "+db.getCount());
            Log.v(TAG,"size of comebackcursor in @ = "+comebackcursor.getCount());
            Log.v(TAG,"size of cursor = "+ cursor.getCount());
			Log.v(TAG,"size of cursor1 = "+ cursor1.getCount());
			Log.v(TAG,"size of dbcursor = "+ dbcursor.getCount());
			Log.v(TAG,"size of localcursor = "+ localcursor.getCount());
			Log.v(TAG,"@ reply hmat  = "+hmat.size());
			Log.v(TAG,"@ reply hmat2  = "+hmat2.size());
			Log.v(TAG,"@ reply hmat3  = "+hmat3.size());
			Log.v(TAG,"@ reply for chm  = "+chm.size());

			return localcursor;
			//return new MergeCursor(new Cursor[]{dbcursor, comebackcursor});
		}


		else

		{
			if (cursor != null) {
				if (cursor.getCount() > 0) {
					cursor.moveToFirst();
					int columnIndex = cursor.getColumnIndex("key");
					do {
						String word = cursor.getString(columnIndex);
						//Log.v("key=", word);
						if (word.equals(selection)) {
							//Log.v("key is found", "");
							int columnIndex1 = cursor.getColumnIndex("value");
							String word1 = cursor.getString(columnIndex1);
							//Log.v("value=", word1);
							cursor1 = new MatrixCursor(new String[]{"key", "value"});
							MatrixCursor.RowBuilder rowBuilder1 = cursor1.newRow();
							rowBuilder1.add("key", word).add("value", word1);
							break;
						}
					} while (cursor.moveToNext());
				}
				cursor.close();
			}
			return cursor1;
		}
		try {
			String requesttype = "query*";

			if(seq2.isEmpty()){
				return cursor;
			}else {
				if(allavdskeys.isEmpty())
				{
					allavdskeys = queryFun2(requesttype);
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return cursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
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

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


		@SuppressLint("LongLogTag")
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			System.out.println("serverSocket=" + serverSocket);

			while (true) {

				Socket socket = null;
				try {
					socket = serverSocket.accept();
					//socket.setSoTimeout(2500);
					Log.v(TAG, "Inside Server code");

					//http://www.jgyan.com/networking/how%20to%20send%20object%20over%20socket%20in%20java.php
					ObjectInputStream is = new ObjectInputStream(socket.getInputStream());
					HashMap<String, String> hm1 = (HashMap<String, String>) is.readObject();
					Log.v(TAG,"Inside Server hm1="+hm1);

					try{

						if(hm1.get("requesttype").equals("delete")) {
							Log.v(TAG,"Going to delete all the key and value pairs because of delete request");
							cursor =new MatrixCursor(new String[] { "key","value"});
							String key = hm1.get("key");
							//delete(mUri,key,null);
							db.deletetable();

						}else if(hm1.get("requesttype").equals("iamback")) {

							Log.v(TAG,"Inside comeback request in server");
							String avd = hm1.get("myoriginseq");
							ContentValues cv = new ContentValues();
							cv.put("avd",avd);
							HashMap<String, HashMap<String,String>> comebackreplyserver = new HashMap<String, HashMap<String,String>>();
                            //HashMap<String,String> hm = new HashMap<String,String>();
							Cursor cursorback = null;
							try {
								 Log.v(TAG,"going to fetch data for comeback");
								 cursorback = db.getComebackKeys(cv);

							}catch(Exception ex){
								ex.printStackTrace();
							}
							//read1.lock();
							try{
							if (cursorback != null) {
								if (cursorback.getCount() > 0) {
									//Log.v(TAG,"cursor values="+ DatabaseUtils.dumpCursorToString(cursor));
									cursorback.moveToFirst();

									do {
										int columnIndex = cursorback.getColumnIndex("key1");
										String key = cursorback.getString(columnIndex);
										int columnIndex1 = cursorback.getColumnIndex("value");
										String value = cursorback.getString(columnIndex1);
										int columnIndexavd1 = cursorback.getColumnIndex("avd1");
										int columnIndexavd2 = cursorback.getColumnIndex("avd2");
										int columnIndexavd3 = cursorback.getColumnIndex("avd3");
										int columnIndexavd4 = cursorback.getColumnIndex("version");
										String avd1 = cursorback.getString(columnIndexavd1);
										String avd2 = cursorback.getString(columnIndexavd2);
										String avd3 = cursorback.getString(columnIndexavd3);
										String version = cursorback.getString(columnIndexavd4);
										HashMap<String,String> hm = new HashMap<String,String>();


											hm.put("avd1",avd1);
											hm.put("avd2",avd2);
											hm.put("avd3",avd3);
											hm.put("value",value);
											hm.put("version",version);
											comebackreplyserver.put(key,hm);

									} while (cursorback.moveToNext());
								}
							}
							}catch(Exception ex){
								ex.printStackTrace();
							}
							//read1.unlock();
							/*read1.lock();
							if (cursor != null) {
								if (cursor.getCount() > 0) {
									//Log.v(TAG,"cursor values="+ DatabaseUtils.dumpCursorToString(cursor));
									cursor.moveToFirst();

									do {
										int columnIndex = cursor.getColumnIndex("key");
										String key = cursor.getString(columnIndex);
										int columnIndex1 = cursor.getColumnIndex("value");
										String value = cursor.getString(columnIndex1);
										int columnIndexavd1 = cursor.getColumnIndex("avd1");
										int columnIndexavd2 = cursor.getColumnIndex("avd2");
										int columnIndexavd3 = cursor.getColumnIndex("avd3");
										int columnIndexavd4 = cursor.getColumnIndex("ver");
										String avd1 = cursor.getString(columnIndexavd1);
										String avd2 = cursor.getString(columnIndexavd2);
										String avd3 = cursor.getString(columnIndexavd3);
										String version = cursor.getString(columnIndexavd4);
										HashMap<String,String> hm = new HashMap<String,String>();
										if(avd.equals(avd1) || avd.equals(avd2) || avd.equals(avd3)){

                                            hm.put("avd1",avd1);
                                            hm.put("avd2",avd2);
                                            hm.put("avd3",avd3);
                                            hm.put("value",value);
											hm.put("version",version);
											comebackreplyserver.put(key,hm);
										}
										//queryreply.put(key, value);

									}while(cursor.moveToNext());
								}
							}
							read1.unlock();
*/
							try {
								 //Log.v(TAG, "Sending the comeback reply now = "+comebackreplyserver);
								ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
								os.writeObject(comebackreplyserver);
								os.flush();
							} catch (Exception ex) {
								ex.printStackTrace();
							}

						}else if (hm1.get("requesttype").equals("query*")){
							Log.v(TAG,"Inside Query * in server");
							String origin = hm1.get("origin");
							HashMap<String, String> queryrstareply = new HashMap<String, String>();
							Cursor crquerystar = db.getAllloaclKeys();
							if(crquerystar!=null){
								if(crquerystar.getCount()>0){
									crquerystar.moveToFirst();
									do {
										int columnIndex = crquerystar.getColumnIndex("key1");
										String key = crquerystar.getString(columnIndex);
										int columnIndex1 = crquerystar.getColumnIndex("value");
										String value = crquerystar.getString(columnIndex1);
										queryrstareply.put(key, value);
									}while(crquerystar.moveToNext());

								}
							}
							/*if (cursor != null) {
								if (cursor.getCount() > 0) {
									cursor.moveToFirst();
									do {
										int columnIndex = cursor.getColumnIndex("key");
										String key = cursor.getString(columnIndex);
										int columnIndex1 = cursor.getColumnIndex("value");
										String value = cursor.getString(columnIndex1);
										queryreply.put(key, value);
									}while(cursor.moveToNext());
								}
							}*/
							try {
								// Log.v(TAG, "Sending the query star reply now = " + queryrstareply);
								ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
								os.writeObject(queryrstareply);
								os.flush();
							} catch (Exception ex) {
								ex.printStackTrace();
							}

						}
						else if (hm1.get("requesttype").equals("query")) {
							Log.v(TAG, "received query request=" + hm1);
							String key = hm1.get("key");
							String origin = hm1.get("origin");
							String value ="null";
							Cursor cr = db.getValue(key);
							ConcurrentHashMap<String,String> chmqueryreply = new ConcurrentHashMap<String, String>();

							if(cr!=null){
							    if(cr.getCount()>0){
							        cr.moveToFirst();
                                    chmqueryreply.put(key,cr.getString(cr.getColumnIndex("value")));
									chmqueryreply.put(key+"version",cr.getString(cr.getColumnIndex("ver")));
                                }
                            }

							/*if (cursor != null) {
								if (cursor.getCount() > 0) {
									cursor.moveToFirst();
									int columnIndex = cursor.getColumnIndex("key");
									do {
										String word = cursor.getString(columnIndex);
										//Log.v("key=", word);received the comeback reply
										if (word.equals(key)) {

											int columnIndex1 = cursor.getColumnIndex("value");
											value = cursor.getString(columnIndex1);
											Log.v(TAG, "key "+ word+" is found value = "+value);
											//Log.v(TAG,"value= "+value);
											break;
										}
									} while (cursor.moveToNext());
							*/		//socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									//        Integer.parseInt(origin));
									Log.v(TAG,"sending the value n key for query = "+chmqueryreply);
									try {
										// Log.v(TAG, "Sending the reply now==" + fhmp);
										ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
										os.writeObject(chmqueryreply);
										os.flush();
									} catch (Exception ex) {
										ex.printStackTrace();
									}
						}
					}catch(Exception e){
						e.printStackTrace();
					}
					try {
						if (hm1.get("requesttype").equals("insert")) {
							Log.v(TAG, "insert request received = " + hm1);
							ContentValues values = new ContentValues();
							values.put("key", hm1.get("key"));
							values.put("value", hm1.get("msg"));
							values.put("hash", hm1.get("msgHash"));
							values.put("port", hm1.get("port"));
							values.put("avd1", hm1.get("port1"));
							values.put("avd2", hm1.get("port2"));
							values.put("avd3", hm1.get("port3"));
							values.put("requesttype","selfinsert");
							insert(mUri, values);
							// break;
						}

					}catch(Exception e){
						e.printStackTrace();
					}
					try {//BQNIWFvQfvbc3GIratUf0OE0zd5mTd09
						if (hm1.get("requesttype").equals("remap")) {

							Log.v(TAG, "I got the remapping request");
							Log.v(TAG, "===================================================================================");
							Log.v(TAG, "final seq=" + hm1);

							for (HashMap.Entry<String,String> entry : hm1.entrySet())
							{
								if(!entry.getKey().equals("requesttype")) {
									seq.add(entry.getKey());
									currNodes.put(entry.getKey(), entry.getValue());
								}
							}

							Collections.sort(seq);
							Log.v(TAG,"size of seq ="+seq +" "+seq.size());
							Log.v(TAG,"size of curr node ="+currNodes +" "+currNodes.size());
							joining = "yes";

							/*for (String i : seq) {
								avdseq.add(currNodes.get(i));
							}*/
						}

					}catch(Exception e){
						e.printStackTrace();
					}


				} catch(Exception e){
					e.printStackTrace();
				}
			}
			// return null;
		}
	}


	private class ClientTask extends AsyncTask<String, Void, String> {

		String msgToSend;


		@SuppressLint("LongLogTag")
		@Override
		protected String doInBackground(String... msgs) {


			Log.v(TAG, "Inside Client ");

				if(msgs[0].equals("delete")) {

					Log.v(TAG,msgs[0]+" port = "+msgs[1]);
					Socket socket1 = null;
					Socket socket2 = null;

					HashMap<String, String> hmdelete = new HashMap<String, String>();
					hmdelete.put("requesttype", msgs[0]);
					hmdelete.put("preport", msgs[1]);
					hmdelete.put("sucport", msgs[2]);
					hmdelete.put("key", msgs[3]);
					hmdelete.put("origin",String.valueOf(myportclient));
					hmdelete.put("myoriginseq",String.valueOf(portsmap.get(myportclient)));

					try {
						try {
							socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[1]));

						} catch (IOException e) {
							e.printStackTrace();
						}

						try {
							ObjectOutputStream os;
							try {
								Log.v(TAG, "sending a delete  to pre port request = " + hmdelete);
								os = new ObjectOutputStream(socket1.getOutputStream());
								os.writeObject(hmdelete);
								os.flush();
								//os.close();
							} catch (Exception ex) {
								ex.printStackTrace();
							}
						} catch (Exception ex) {
							ex.printStackTrace();
						}

					}catch(Exception e){
						e.printStackTrace();
					}



					try {
						try {

							socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[2]));
						} catch (IOException e) {
							e.printStackTrace();
						}


						ObjectOutputStream os;
						try {
							Log.v(TAG, "sending a delete to suc port  request =" + hmdelete);
							os = new ObjectOutputStream(socket2.getOutputStream());
							os.writeObject(hmdelete);
							os.flush();
							//os.close();
						} catch (Exception ex) {
							ex.printStackTrace();
						}
					}catch(Exception e){
						e.printStackTrace();
					}

				}else if(msgs[0].equals("iamback")) {

					read1.lock();
					Log.v(TAG,"going to acquire a lock in iamback client");
				Log.v(TAG,msgs[0]+" port = "+msgs[1]);
				Socket socket1 = null;
				Socket socket2 = null;
				String avd1=null;
				String avd2=null;
				String avd3=null;
				String version=null;
				String value1 = null;
				HashMap<String,String> tmphm = new HashMap<String, String>();
				//tmphm = db.getAllloaclKeys();
				/*	Cursor dbcursor = db.getAllloaclKeys();
					Log.v(TAG,"dbcursor in server comeback= "+dbcursor.getCount()+" "+dbcursor.getColumnName(0));
					//MatrixCursor localcursor = new MatrixCursor(new String[]{"key", "value"});
					try {
						if (dbcursor != null) {
							if (dbcursor.getCount() > 0) {
								dbcursor.moveToFirst();
								do {
									int columnIndex = dbcursor.getColumnIndex("key1");
									String word = dbcursor.getString(columnIndex);
									//Log.v("key=", word);

									int columnIndex1 = dbcursor.getColumnIndex("ver");
									String word1 = dbcursor.getString(columnIndex1);
									//MatrixCursor.RowBuilder rowBuilder1 = localcursor.newRow();
									//rowBuilder1.add("key", word).add("value", word1);
									tmphm.put(word, word1);
									//Log.v(TAG,"key value rowise = "+hmat);

									//Log.v(TAG, " key= " + word + " value= " + word1);
								} while (dbcursor.moveToNext());
							}
							dbcursor.close();
						}

						//Log.v(TAG,"tmphm from db = "+tmphm);
					}catch(Exception ex){
						ex.printStackTrace();
					}*/

				String sql="";

				HashMap<String, String> hmback = new HashMap<String, String>();
				hmback.put("requesttype", msgs[0]);
				hmback.put("preport", msgs[1]);
				hmback.put("sucport", msgs[2]);
				hmback.put("origin",String.valueOf(myportclient));
				hmback.put("myoriginseq",String.valueOf(portsmap.get(myportclient)));

                HashMap<String, HashMap<String,String>> comebackreply = new HashMap<String, HashMap<String,String>>();
                HashMap<String, HashMap<String,String>> tmpcomeback = new HashMap<String, HashMap<String,String>>();
				try {
                    try {
                        socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[1]));

                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    try {
                        ObjectOutputStream os;
                        try {
                            Log.v(TAG, "sending a comeback pre request =" + hmback);
                            os = new ObjectOutputStream(socket1.getOutputStream());
                            os.writeObject(hmback);
                            os.flush();
                            //os.close();
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }

                    comebackreply = new HashMap<String, HashMap<String,String>>();
                    try {
                        ObjectInputStream is = new ObjectInputStream(socket1.getInputStream());
                        comebackreply = (HashMap<String, HashMap<String,String>>) is.readObject();
                        Log.v(TAG, "received the comeback reply ");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    tmpcomeback = new HashMap<String, HashMap<String,String>>();

                    for(HashMap.Entry<String, HashMap<String,String>> entry : comebackreply.entrySet()) {


							//if (!tmpcomeback.containsKey(entry.getKey())) {
									tmpcomeback.put(entry.getKey(), entry.getValue());
									HashMap<String, String> hm = entry.getValue();
									 avd1 = hm.get("avd1");
									 avd2 = hm.get("avd2");
									 avd3 = hm.get("avd3");
									 value1 = hm.get("value");
									 version = hm.get("version");
							ContentValues cv = new ContentValues();
							cv.put("key",entry.getKey());
							cv.put("value",value1);
							cv.put("avd1",avd1);
							cv.put("avd2",avd2);
							cv.put("avd3",avd3);
							cv.put("ver",version);
							//insert(mUri,cv);
									sql = "('" + entry.getKey() + "','" + value1 + "','" + avd1 + "','" + avd2 + "','" + avd3 + "','" + version + "')," + sql;
									//MatrixCursor.RowBuilder rowBuilder = comebackcursor.newRow();
									//rowBuilder.add("key", entry.getKey()).add("value", value1).add("avd1", avd1).add("avd2", avd2).add("avd3", avd3);

							}
						//}

                }catch(Exception e){
				    e.printStackTrace();
                }



				try {
                    try {

                        socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[2]));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }


                        ObjectOutputStream os;
                        try {
                            Log.v(TAG, "sending a comeback suc request =" + hmback);
                            os = new ObjectOutputStream(socket2.getOutputStream());
                            os.writeObject(hmback);
                            os.flush();
                            //os.close();
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    HashMap<String, HashMap<String,String>> comebackreply2 = new HashMap<String, HashMap<String,String>>();
                    try {
                        ObjectInputStream is = new ObjectInputStream(socket2.getInputStream());
                        comebackreply2 = (HashMap<String, HashMap<String,String>>) is.readObject();
                        //Log.v(TAG, "received the comeback reply " + comebackreply2);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    //Log.v(TAG,"received the comeback reply = "+comebackreply2);
                    //tmpcomeback = new HashMap<String, HashMap<String,String>>();

                    for(HashMap.Entry<String, HashMap<String,String>> entry1 : comebackreply2.entrySet()) {
								//tmpcomeback.put(entry1.getKey(), entry1.getValue());
								HashMap<String, String> hm = entry1.getValue();
								avd1 = hm.get("avd1");
								avd2 = hm.get("avd2");
								avd3 = hm.get("avd3");
								value1 = hm.get("value");
								version = hm.get("version");
						ContentValues cv = new ContentValues();
						cv.put("key",entry1.getKey());
						cv.put("value",value1);
						cv.put("avd1",avd1);
						cv.put("avd2",avd2);
						cv.put("avd3",avd3);
						cv.put("ver",version);
						//insert(mUri,cv);
								sql = "('" + entry1.getKey() + "','" + value1 + "','" + avd1 + "','" + avd2 + "','" + avd3 + "','" + version + "')," + sql;
								//MatrixCursor.RowBuilder rowBuilder = comebackcursor.newRow();
								//rowBuilder.add("key", entry.getKey()).add("value", value1).add("avd1", avd1).add("avd2", avd2).add("avd3", avd3);
							}



					if (sql.endsWith(",")) {

						sql = sql.substring(0, sql.length() - 1);
						//Log.v(TAG,"sql ends with coma = "+ sql );
					}
					//read1.lock();
					if(!sql.isEmpty()) {
						Log.v(TAG,"Going to add the comeback keyvalues in db , count before = "+db.getCount() );
						db.addKeyPair2(sql);
						Log.v(TAG,"Added the comeback keyvalues in db , count after = "+db.getCount() );
					}
					//read1.unlock();

                }catch(Exception e){
				    e.printStackTrace();
                }
					Log.v(TAG,"Going to release the comeback lock");
					read1.unlock();

			}else if(msgs[0].equals("query*")){
				querstarreply = new HashMap<String, String>();
					Log.v(TAG,msgs[0]);

					int count =0;
					for(String a:seq2){
						count++;
					}
					int preavd;
					int cavd;
					Log.v(TAG,"avdseq ="+avdseq);
					currentavdseq = portsmap.get(myportclient);

					if(currentavdseq == 0){
						preavd = 3;
						cavd = 4;
					}else if(currentavdseq == 1){
						preavd = 2;
						cavd = 3;
					}else if(currentavdseq == 2){
						preavd = 4;
						cavd = 1;
					}else if(currentavdseq == 3){
						preavd = 1;
						cavd = 0;
					}else{
						preavd = 0;
						cavd = 2;
					}
					Log.v(TAG,"curretn nodes = "+count);
					MatrixCursor cursortmp1 = new MatrixCursor(new String[]{"key", "value"});
					int i=0;
					//for(i=0;i<count;i++){

					Log.v(TAG,"sending the request for quer * to "+remote_ports[currentavdseq]);
					HashMap<String,String> hmquery = new HashMap<String, String>();
					hmquery.put("requesttype",msgs[0]);
					hmquery.put("origin",myportclient);
					try {
						Socket socket1 = null;
						try {
							//for (int i = 0; i < 5; i++) {
							socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(remote_ports[currentavdseq]));
						} catch (Exception ex) {
							ex.printStackTrace();
						}

						try {
							ObjectOutputStream os;
							try {
								os = new ObjectOutputStream(socket1.getOutputStream());
								os.writeObject(hmquery);
								os.flush();
							} catch (Exception ex) {
								ex.printStackTrace();
							}
						} catch (Exception ex) {
							ex.printStackTrace();
						}

						//receiving the propose sequence no. as a response from all avd including from himself
						Log.v(TAG, "received the reply for query * from " + remote_ports[currentavdseq]);
						HashMap<String, String> tmp = new HashMap<String, String>();
						ObjectInputStream is = null;
						try {
							is = new ObjectInputStream(socket1.getInputStream());
							try {

								tmp = (HashMap<String, String>) is.readObject();

								//is.close();
							} catch (Exception ex) {
								ex.printStackTrace();
							}
							for (HashMap.Entry<String, String> entry : tmp.entrySet()) {
								querstarreply.put(entry.getKey(), entry.getValue());
								//Log.v(TAG, "querstarreply=" + querstarreply);
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}catch(Exception e){
						e.printStackTrace();
					}
				String fail="0";
				Log.v(TAG,"sending the request for query * to "+remote_ports[cavd]);
				try {
					Socket socket2 = null;
					try {
						//for (int i = 0; i < 5; i++) {
						socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(remote_ports[cavd]));
					} catch (Exception ex) {
						fail="1";
						ex.printStackTrace();
					}

					try {
						ObjectOutputStream os;
						try {
							os = new ObjectOutputStream(socket2.getOutputStream());
							os.writeObject(hmquery);
							os.flush();
						} catch (Exception ex) {
							fail="1";
							ex.printStackTrace();
						}
					} catch (Exception ex) {
						fail="1";
						ex.printStackTrace();
					}

					//receiving the propose sequence no. as a response from all avd including from himself
					Log.v(TAG, "received the reply for query * from " + remote_ports[cavd]);
					HashMap<String, String> tmp = new HashMap<String, String>();
					ObjectInputStream is = null;
					try {
						is = new ObjectInputStream(socket2.getInputStream());
						try {

							tmp = (HashMap<String, String>) is.readObject();

							//is.close();
						} catch (Exception ex) {
							fail="1";
							ex.printStackTrace();
						}
						for (HashMap.Entry<String, String> entry : tmp.entrySet()) {
							querstarreply.put(entry.getKey(), entry.getValue());
							//Log.v(TAG, "querstarreply=" + querstarreply);
						}
					} catch (Exception e) {
						fail="1";
						e.printStackTrace();
					}
				}catch(Exception e){
					fail="1";
					e.printStackTrace();
				}

				if(fail.equals("1")) {
					Log.v(TAG,"sending the request for query * to "+remote_ports[preavd]);

					try {
						Socket socket3 = null;
						try {
							//for (int i = 0; i < 5; i++) {
							socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(remote_ports[preavd]));
						} catch (Exception ex) {
							ex.printStackTrace();
						}

						try {
							ObjectOutputStream os;
							try {
								os = new ObjectOutputStream(socket3.getOutputStream());
								os.writeObject(hmquery);
								os.flush();
							} catch (Exception ex) {
								ex.printStackTrace();
							}
						} catch (Exception ex) {
							ex.printStackTrace();
						}

						//receiving the propose sequence no. as a response from all avd including from himself
						Log.v(TAG, "received the reply for query * from " + remote_ports[preavd]);
						HashMap<String, String> tmp = new HashMap<String, String>();
						ObjectInputStream is = null;
						try {
							is = new ObjectInputStream(socket3.getInputStream());
							try {

								tmp = (HashMap<String, String>) is.readObject();

								//is.close();
							} catch (Exception ex) {
								ex.printStackTrace();
							}
							for (HashMap.Entry<String, String> entry : tmp.entrySet()) {
								querstarreply.put(entry.getKey(), entry.getValue());
								Log.v(TAG, "querstarreply=" + querstarreply);
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
						//Log.v(TAG,i+"~~~"+count);
				//	}
					//if(i==count){
						Log.v(TAG,"ans=yes");
						ans="yes";

					//}
				}
				else if(msgs[0].equals("insert")) {
					Log.v(TAG,msgs[0]+" "+msgs[1]+" "+msgs[2]+" "+msgs[3]+" "+msgs[4]+" "+msgs[5]+" "+msgs[6]);
					Socket socket = null;
					Socket socket1 = null;
					Socket socket2 = null;

					//String mssg1 = (String) values.get("value");
					HashMap<String, String> hminsert = new HashMap<String, String>();
					hminsert.put("requesttype", msgs[0]);
					hminsert.put("msg", msgs[1]);
					hminsert.put("key", msgs[2]);
					hminsert.put("msgHash", msgs[3]);
					hminsert.put("port", msgs[4]);
					hminsert.put("port1",String.valueOf(portsmap.get(msgs[4])));
					hminsert.put("port2",String.valueOf(portsmap.get(msgs[5])));
					hminsert.put("port3",String.valueOf(portsmap.get(msgs[6])));
					//hminsert.put("remap","-");
					//read1.lock();
					//read1.unlock();
                try {
                    try {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[4]));

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    try {
                        ObjectOutputStream os;
                        try {
                            //Log.v(TAG," inside outputstream block");
                            Log.v(TAG, "sending a insert  request to avd1 =" + hminsert);
                            os = new ObjectOutputStream(socket.getOutputStream());
                            os.writeObject(hminsert);
                            os.flush();
                            //os.close();
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                        //  socket.close();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }catch(Exception e){
                    e.printStackTrace();
                }
					try {
						hminsert.put("port", msgs[5]);
						try {
							socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[5]));
						} catch (Exception e) {
							e.printStackTrace();
						}
						try {
							ObjectOutputStream os;
							try {
								//Log.v(TAG," inside outputstream block");
								Log.v(TAG, "sending a insert  request to avd2 =" + hminsert);
								os = new ObjectOutputStream(socket1.getOutputStream());
								os.writeObject(hminsert);
								os.flush();
								//os.close();
							} catch (Exception ex) {
								ex.printStackTrace();
							}
							//  socket.close();
						} catch (Exception ex) {
							ex.printStackTrace();
						}
					}catch(Exception e){
						e.printStackTrace();
					}
					try {
						try {
							socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[6]));
						} catch (Exception e) {
							e.printStackTrace();
						}

						hminsert.put("port", msgs[6]);
						try {
							ObjectOutputStream os;
							try {
								//Log.v(TAG," inside outputstream block");
								Log.v(TAG, "sending a insert  request  to avd3 =" + hminsert);
								os = new ObjectOutputStream(socket2.getOutputStream());
								os.writeObject(hminsert);
								os.flush();
								//os.close();
							} catch (Exception ex) {
								ex.printStackTrace();
							}
							//  socket.close();
						} catch (Exception ex) {
							ex.printStackTrace();
						}
					}catch(Exception e){
						e.printStackTrace();
					}


				}else if(msgs[0].equals("query")) {

					Log.v(TAG,msgs[0]+" hash = "+msgs[1]+" port = "+msgs[2]+" key ="+msgs[3]+"avdpre="+msgs[4]+"avdsuc="+msgs[5]);

					queryvalue ="null";
                    queryvaluehm = new ConcurrentHashMap<String, String>();
					//String mssg1 = (String) values.get("value");
					HashMap<String, String> hmquery = new HashMap<String, String>();
					hmquery.put("requesttype", msgs[0]);
					hmquery.put("key", msgs[3]);
					hmquery.put("msgHash", msgs[1]);
					hmquery.put("port", msgs[2]);
					hmquery.put("origin",String.valueOf(myportclient));
					hmquery.put("avdpre",msgs[4]);
					hmquery.put("avdsuc",msgs[5]);
					hmquery.put("avdpre2",msgs[6]);
					hmquery.put("avdsuc2",msgs[7]);
					ConcurrentHashMap<String, String>	queryvaluehm1 = new ConcurrentHashMap<String, String>();
					ConcurrentHashMap<String, String>	queryvaluehm2 = new ConcurrentHashMap<String, String>();
					ConcurrentHashMap<String, String>	queryvaluehm0 = new ConcurrentHashMap<String, String>();
					ArrayList<String> freq = new ArrayList<String>();
					int version = -1;
					String fail ="0";

					Socket socket = null;
					try{
					try {
						Log.v(TAG,"creating pre socket");
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remote_ports[Integer.parseInt(msgs[4])]));
						//socket.setSoTimeout(10000);
					} catch (IOException e) {
						fail="1";
						Log.v(TAG,"pre avd also111 ?????");
						//e.printStackTrace();
					}
					try {
						ObjectOutputStream os;
						try {
							//Log.v(TAG," inside outputstream block");
							Log.v(TAG, "sending a pre query  request =" + hmquery);
							os = new ObjectOutputStream(socket.getOutputStream());
							os.writeObject(hmquery);
							os.flush();
							//os.close();
						} catch (Exception ex) {
							fail ="1";
							Log.v(TAG,"pre avd also222 ?????");
							//ex.printStackTrace();
						}
						//  socket.close();
					} catch (Exception ex) {
						fail ="1";
						Log.v(TAG,"pre avd also333 ?????");
						//ex.printStackTrace();
					}
						queryvaluehm0 = new ConcurrentHashMap<String, String>();

						try {
							ObjectInputStream is = new ObjectInputStream(socket.getInputStream());
							queryvaluehm0 = (ConcurrentHashMap<String, String>) is.readObject();
							Log.v(TAG,"received the pre query reply = "+queryvaluehm0);
							//if(queryvaluehm0!=null) {
								if (!queryvaluehm0.isEmpty() ) {
									if (!queryvaluehm0.get(msgs[3]).equals("null")) {
										String value = queryvaluehm0.get(msgs[3]);
										if(version < Integer.valueOf(queryvaluehm0.get(msgs[3] + "version"))) {
											Log.v(TAG,"for key new version is greater =  "+queryvaluehm0);
											version = Integer.valueOf(queryvaluehm0.get(msgs[3] + "version"));
											if (queryvaluehm.containsKey(msgs[3])) {
												Log.v(TAG,"removing the old value = "+queryvaluehm0.get(msgs[3]));
												queryvaluehm.remove(msgs[3]);
												queryvaluehm.put(msgs[3], queryvaluehm.get(msgs[3]));
												Log.v(TAG,"now the new  value = "+queryvaluehm.get(msgs[3]));
											}else{
												Log.v(TAG,"not present so now adding the value "+queryvaluehm0);
												queryvaluehm.put(msgs[3], queryvaluehm0.get(msgs[3]));
											}
										}
									}else {
										fail = "1";
										Log.v(TAG, "query pre value is empty");
									}
								}else{
										fail = "1";
										Log.v(TAG, "query pre value is empty");
									}

							//freq.add(queryvaluehm0.get(msgs[3]));
						}catch(Exception e){
							fail="1";
							//e.printStackTrace();
						}
					}catch (Exception e){
						fail="1";
						Log.v(TAG,"current avd also444 ?????");
					//	e.printStackTrace();
					}

					//if(fail.equals("1") || queryvaluehm.get(msgs[3]).equals("null") ){
						try {
							Socket socket2=null;
							try {
								Log.v(TAG, "sending to curr  avd....as current avd ???? ");
								 socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[2]));
							} catch (IOException e) {
								Log.v(TAG, " avd111 also?????");
								fail = "2";
								//e.printStackTrace();
							}
							try {
								ObjectOutputStream os;
								try {
									//Log.v(TAG," inside outputstream block");
									Log.v(TAG, "sending a curr  query  request =" + hmquery);
									os = new ObjectOutputStream(socket2.getOutputStream());
									os.writeObject(hmquery);
									os.flush();
									//os.close();
								} catch (Exception ex) {
									Log.v(TAG, " avd222 also?????");
									fail = "2";
									//ex.printStackTrace();
								}
								//  socket.close();
							} catch (Exception ex) {
								Log.v(TAG, " avd333 also?????");
								fail = "2";
								//ex.printStackTrace();
							}
								queryvaluehm1 = new ConcurrentHashMap<String, String>();

							try {

								ObjectInputStream is = new ObjectInputStream(socket2.getInputStream());
								queryvaluehm1 = (ConcurrentHashMap<String, String>) is.readObject();
								Log.v(TAG,"received the curr query reply = "+queryvaluehm1);
								Log.v(TAG,"queryvalehm1 = "+queryvaluehm1.get(msgs[3]).equals("null"));
								if (!queryvaluehm1.isEmpty() ) {
									if (!queryvaluehm1.get(msgs[3]).equals("null")) {
										String value = queryvaluehm1.get(msgs[3]);
										if(version < Integer.valueOf(queryvaluehm1.get(msgs[3] + "version"))) {
											Log.v(TAG,"for key new version is greater =  "+queryvaluehm1);
											version = Integer.valueOf(queryvaluehm1.get(msgs[3] + "version"));
											if (queryvaluehm.containsKey(msgs[3])) {
												Log.v(TAG,"removing the old value = "+queryvaluehm1.get(msgs[3]));
												queryvaluehm.remove(msgs[3]);
												queryvaluehm.put(msgs[3], queryvaluehm.get(msgs[3]));
												Log.v(TAG,"now the new  value = "+queryvaluehm.get(msgs[3]));
											}else{
												Log.v(TAG,"not present so now adding the value "+queryvaluehm1);
												queryvaluehm.put(msgs[3], queryvaluehm1.get(msgs[3]));
											}
										}
									} else {
										fail = "2";
										Log.v(TAG, "query pre value is empty");
									}
								}else{
									fail = "2";
									Log.v(TAG, "query pre value is empty");
								}
								//freq.add(queryvaluehm1.get(msgs[3]));
							}catch(Exception e){
								fail="2";
								//e.printStackTrace();
							}
						}catch(Exception e){
							fail="2";
							//e.printStackTrace();
						}
					//}
					//if(fail.equals("2") || queryvaluehm.get(msgs[3]).equals("null")){
						try {
							Socket socket3=null;
							try {
								Log.v(TAG, "sending to suc avd....as current avd ???? ");
								socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remote_ports[Integer.parseInt(msgs[5])]));
							} catch (IOException e) {
								Log.v(TAG, "suc avd111 also?????");
								fail = "3";
								//e.printStackTrace();
							}
							try {
								ObjectOutputStream os;
								try {
									//Log.v(TAG," inside outputstream block");
									Log.v(TAG, "sending a suc query  request =" + hmquery);
									os = new ObjectOutputStream(socket3.getOutputStream());
									os.writeObject(hmquery);
									os.flush();
									//os.close();
								} catch (Exception ex) {
									Log.v(TAG, "suc avd222 also?????");
									fail = "3";
									//ex.printStackTrace();
								}
								//  socket.close();
							} catch (Exception ex) {
								Log.v(TAG, "suc avd333 also?????");
								fail = "3";
								//ex.printStackTrace();
							}
							 queryvaluehm2 = new ConcurrentHashMap<String, String>();
							try {
								ObjectInputStream is = new ObjectInputStream(socket3.getInputStream());
                                queryvaluehm2 =  (ConcurrentHashMap<String, String>) is.readObject();
								Log.v(TAG,"received the suc query reply = "+queryvaluehm2);
								if (!queryvaluehm2.isEmpty() ) {
									if (!queryvaluehm2.get(msgs[3]).equals("null")) {
										String value = queryvaluehm2.get(msgs[3]);
										if(version < Integer.valueOf(queryvaluehm2.get(msgs[3] + "version"))) {
											Log.v(TAG,"for key new version is greater =  "+queryvaluehm2);
											version = Integer.valueOf(queryvaluehm2.get(msgs[3] + "version"));
											if (queryvaluehm.containsKey(msgs[3])) {
												Log.v(TAG,"removing the old value = "+queryvaluehm.get(msgs[3]));
												queryvaluehm.remove(msgs[3]);
												queryvaluehm.put(msgs[3], queryvaluehm2.get(msgs[3]));
												Log.v(TAG,"now the new  value = "+queryvaluehm.get(msgs[3]));
											}else{
												Log.v(TAG,"not present so now adding the value "+queryvaluehm2);
												queryvaluehm.put(msgs[3], queryvaluehm2.get(msgs[3]));
											}
										}
									} else {
										fail = "1";
										Log.v(TAG, "query pre value is empty");
									}
								}else{
									fail = "1";
									Log.v(TAG, "query pre value is empty");
								}
								//freq.add(queryvaluehm2.get(msgs[3]));
							}catch(Exception e){
								fail="3";
								//e.printStackTrace();
							}
						}catch(Exception e){
							fail="3";
							//e.printStackTrace();Got the value =
						}


						Log.v(TAG,"After all socket replies the queryvaluehm = "+queryvaluehm );

						if(queryvaluehm.isEmpty()) {
							try {
								if (queryvaluehm0.get(msgs[3]).equals(queryvaluehm1.get(msgs[3])))
									queryvaluehm.put(msgs[3], queryvaluehm0.get(msgs[3]));
								else if (!queryvaluehm0.get(msgs[3]).equals("null"))
									queryvaluehm.put(msgs[3], queryvaluehm1.get(msgs[3]));
							} catch (Exception ex) {
								//ex.printStackTrace();
							}
							try {
								if (queryvaluehm0.get(msgs[3]).equals(queryvaluehm2.get(msgs[3])))
									queryvaluehm.put(msgs[3], queryvaluehm0.get(msgs[3]));
								else if (!queryvaluehm1.get(msgs[3]).equals("null"))
									queryvaluehm.put(msgs[3], queryvaluehm1.get(msgs[3]));
							} catch (Exception ex) {
								//ex.printStackTrace();
							}
							try {
								if (queryvaluehm1.get(msgs[3]).equals(queryvaluehm2.get(msgs[3])))
									queryvaluehm.put(msgs[3], queryvaluehm1.get(msgs[3]));
								else if (!queryvaluehm2.get(msgs[3]).equals("null"))
									queryvaluehm.put(msgs[3], queryvaluehm1.get(msgs[3]));
							} catch (Exception ex) {
								//ex.printStackTrace();
							}
						}
					Log.v(TAG,"After all socket replies the queryvaluehm  part2 = "+queryvaluehm );
						if(queryvaluehm.isEmpty()) {
							if (queryvaluehm0.isEmpty() && queryvaluehm1.isEmpty()) {
								queryvaluehm.put(msgs[3], queryvaluehm2.get(msgs[3]));
							}
							if (queryvaluehm1.isEmpty() && queryvaluehm2.isEmpty()) {
								queryvaluehm.put(msgs[3], queryvaluehm0.get(msgs[3]));
							}
							if (queryvaluehm2.isEmpty() && queryvaluehm0.isEmpty()) {
								queryvaluehm.put(msgs[3], queryvaluehm1.get(msgs[3]));
							}
						}
					Log.v(TAG,"After all socket replies the queryvaluehm part3 = "+queryvaluehm );
				return queryvaluehm.get(msgs[3]);
				}/*else{
					Log.v(TAG, " msgs =" + msgs[0] + " " + msgs[1]);
				}*/

			return null;


		}
	}
}
/*
(1,2,3),
    (4,5,6),
    (7,8,9);
 */
//Final Code Submission