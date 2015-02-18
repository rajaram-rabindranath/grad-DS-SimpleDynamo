package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.NoSuchAlgorithmException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;






import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;

import android.content.Context;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.telephony.TelephonyManager;
import android.text.TextUtils;
import android.util.Log;


/*=========================================================================
 * Class name   : SimpleDHTProvider
 * Description  : Handles the content provider for the SimpleDhtActivity
 * 					application
 * Author		: RAJARAM RABINDRANATH
 *=========================================================================*/
public class SimpleDynamoProvider extends ContentProvider 
{

	final String uri = "content://edu.buffalo.cse.cse486586.simpledynamo.provider";
	final Uri simpleDHTURI = Uri.parse(uri);
	static final String TAG = SimpleDynamoProvider.class.getName();
	/**
	 * database details
	 */
	static final String dbName 	="dynamo";
	static final String tableName	= "data_dynamo";
	static int dbVersion = 1; // overkill -- for prj2
	private static SQLiteDatabase sqliteDB = null;	
	static dataAccess db_conduit = null; 
	static final String KEY_FIELD = "key";
	static final String VALUE_FIELD = "value";
	long timeout = 100;
	Context simpleDynamoContext = null;
	
	/**
	 * comm arrangments
	 */
	int SERVER_PORT = 10000;
	String myAVDnum = null;
	node myDetails = null;	
	//static String tester_query = null;	
	/**
	 * Chord 
	 */
	ChordMaster chord = null; 
	

	/**
	 * Query levels -- identifiers
	 */
	final String query_all = "*";
	final String query_mine = "@";
	final String query_particular = "-";
	
	final String delete_all =  query_all;
	final String delete_mine =  query_mine;
	final String delete_particular = query_particular;  
	
	/**
	 * Locks for distributed search/insert/query/delete
	 * for synchronized response to the app that requests
	 */
	dynamoLock _GDump_Lock = null;
	dynamoLock _insert_Lock = null; // this dynamoLock does not use the records_Requested Hashtable 
	dynamoLock _query_Lock = null;
	dynamoLock _GDel_Lock = null;
	dynamoLock _delObj_Lock = null;
	
	dynamoLock _DBWRITE_Lock = null;
	
	final Integer OBJECT_DOES_NOT_EXIST = 99999;
	
	
	/*=========================================================================
	 * Class name   : Lock
	 * Description  : inner class that has template for the many locks that
	 * 					shall be used by this application to handle user requests					
	 * Author		: RAJARAM RABINDRANATH
	 *=========================================================================*/
    class dynamoLock
	{
		//Boolean isGood = Boolean.valueOf(false);
		Hashtable<String, String>records =  null;
		Integer rows_affected =  0;
		
		public dynamoLock(){}
		/*
		public void setGood(Boolean goodness)
		{
			isGood = goodness;
		}*/
	}

    /*=========================================================================
     * Function   : onCreate()
     * Description: setsup the SimpleDhtProvider
     * Parameters : void
     * Return	  : boolean
     *=========================================================================*/
    public boolean onCreate() 
    {
		simpleDynamoContext = getContext();
		db_conduit = new dataAccess(simpleDynamoContext);
		
		// init all locks
		_GDump_Lock  = new dynamoLock();
		_insert_Lock = new dynamoLock();
		_query_Lock = new dynamoLock();
		_GDel_Lock = new dynamoLock();
		_delObj_Lock = new dynamoLock();
		_DBWRITE_Lock = new dynamoLock();
		// permissions to be writable
		sqliteDB = db_conduit.getWritableDatabase();
		
		if(sqliteDB == null)
		{
			Log.e("window_shopper","COULD NOT CREATE DATABASE!");
			return false;
		}
		
		
		/**
		 *  Who am i ? .. well
		 *  Become self aware
		 */
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        myAVDnum = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(myAVDnum) * 2));
        myDetails =  new node(myPort,myAVDnum);
        Log.d("window_shopper","I am <avdnum>` ::"+myAVDnum);
        
        /**
         * Create the ChordMaster -- Shall help
         * with all questions vis-a-vis the chord
         */
        chord = ChordMaster.makeChord(myDetails);
        
        
        
        
          Log.d("window_shopper","my pred node"+chord.predNode(Integer.valueOf(myDetails.portNum)).avdNum);
          Log.d("window_shopper","my succ node"+chord.succNode(Integer.valueOf(myDetails.portNum)).avdNum);
        
        
        /**
         * coming back to life -- Floyd-esque beacon
         * or Just an I am "Alive"<Pearl Jam-esque?> beacon
         */
        Message.joinMessage(myAVDnum);
        
        /**
		 * Start server thread
		 */
		try
		{
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerOps(new serverOps_params(serverSocket,this)).start();
        }
		catch(IOException ex)
		{
			ex.printStackTrace();
			Log.e("window_shopper","Cannot create serversocket");
		}
		return true;	
    }

	
	/*=========================================================================
	 * Class name   : dataAccess
	 * Description  : a inner classextends SQLiteOpenHelper 
	 * 					Sets up a database in sqlite					
	 * Author		: RAJARAM RABINDRANATH
	 *=========================================================================*/
    private class dataAccess extends SQLiteOpenHelper 
    {
    	
    		// make primary key --- please -- for bad retreivals in phase 6
    	//	final String sqlStatement_CreateTable = "create table "+tableName+"( key text primary key,"+" value text not null);";	
    	final String sqlStatement_CreateTable = "create table "+tableName+"( key text not null,"+" value text not null);";
    		final String TAG = dataAccess.class.getName();
    		
    		public dataAccess(Context context)
    		{
    			super(context,dbName, null,dbVersion);
    		}

    		/*
    		 *get called when getWriteable database is called
    		 */
    		public void onCreate(SQLiteDatabase sqliteDB) 
    		{
    			sqliteDB.execSQL(sqlStatement_CreateTable); // creates a table
    			Log.e("pigtail","CREATING TABLE");
    		}

    		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) 
    		{
    			Log.e("window_shopper","I have been asked to upgrade .. don't know what to do");
    		}    	
    }

    /*=========================================================================
     * Function   : getType()
     * Description: NONE
     * Parameters : Uri
     * Return	  : String
     *=========================================================================*/
    public String getType(Uri uri) 
    {
        return null;
    }

    
    /*=========================================================================
     * Function   : insert
     * Description: this is an overloaded method which insets the givn KV_pair
     * 				into the sqlite table-- shall be called programmatically
     * Parameters : String[] KV_pair
     * Return	  : boolean
     *=========================================================================*/
    public boolean insert(Object KV_pair,String avd) // making this also synchronized -- bad idea -- raja may 8
    {
    	/**
    	 *  lock the DB when inserting -- query also uses this -- 
    	 *  To keep Query section from querying 
    	 */
    	 
		synchronized (_DBWRITE_Lock)
		{
			long rowID = 0l;
			ContentValues newValues = null;
			if(KV_pair instanceof String[])
			{
				newValues = new ContentValues();
				String[] KV = (String[])KV_pair;
				newValues.put(KEY_FIELD,(String)KV[0]);
				newValues.put(VALUE_FIELD,(String)KV[1]);
				// insert or replace -- handle that
				rowID = sqliteDB.insert(tableName, "", newValues);
				/*if(rowID < 0)
				{
					rowID = sqliteDB.replace(tableName, "", newValues);
				}*/
				 Log.v("window_shopper", "ins:"+KV[0]+"::"+KV[1]+" ---  "+rowID); // notify user debug statement
			}
			else if(KV_pair instanceof Hashtable) // when recovery happens !! FIXME -- RAJA
			{
				Hashtable<String, String> values =  (Hashtable<String, String>)KV_pair;
				Set<String> keys =  values.keySet();
				for(String key : keys)
				{
					newValues = new ContentValues();
					newValues.put(KEY_FIELD,key);
					newValues.put(VALUE_FIELD,values.get(key));
					rowID = sqliteDB.insert(tableName, "", newValues);
					// replace only if from predecessor
					/*if(rowID < 0) 
					{
						rowID = sqliteDB.replace(tableName, "", newValues);
					}*/
					Log.v("window_shopper", "ins:"+key+"::"+values.get(key)+" ---  "+rowID); // notify user debug statement
				}
			}
			
			if(rowID < 0)
	        {
	        	Log.e("window_shopper"+" insert","INSERT FAIL");
	        	return false; //FIXME-- interpretation problems
	        }
	        return true;
		}
	}
    
    /*=========================================================================
     * Function   : insert
     * Description: Inserts given KV_pair into the sqlite database
     * Parameters : Uri uri, ContentValues values
     * Return	  : Uri 
     *=========================================================================*/
    synchronized public Uri insert(Uri uri, ContentValues values) // FIXME-- raja made it synchronized 
    {
    	/**
    	 * will synchronize the inserts -- going to others nodes --> replication --> replication
    	 * SYNCHRONIZED 
    	 * 	-- to order concurrent insert requests coming in from
    	 *  -- C1 C2 C3 C4 C5 ..............CN	
    	 * Gets insert_order from Clients()
    	 * {
    	 * 		-- find owner --> send to owner
    	 * 		-- waits <using _insert_lock> ------ to make sure that system of replicas are in consistent shape
    	 * 			-- owner+1 replicates
    	 * 			-- owner+2 replicates
    	 * 				-- owner+2 responds to me 
    	 * 		-- wait ends
    	 * } 
    	 */
    	
    	String[] KV_pair ={(String)values.get(KEY_FIELD),(String)values.get(VALUE_FIELD)};
    	String hashedKey = null;
    	node owner = null; 
    	
    	Log.e("window_shopper","Tester msg::insert::"+KV_pair[0]+"::"+KV_pair[1]);
    	
    	try
    	{
    		hashedKey =	ChordMaster.genHash(KV_pair[0]);
    		owner = chord.getOwner(hashedKey);
    	}
    	catch(NoSuchAlgorithmException nsa)
    	{
    		nsa.printStackTrace();
    	}
    	
    	if(owner == null)
    	{
    		Log.e("window_shopper","owner is null OH! GOD!");
    	}
    	else
    	{
    		Log.e("window_shopper","The owner is::"+owner.avdNum);
    	}
    	
    	// synchronize DB_WRITELOCK HERE
    	// problem I lock and i get request for replication
    	// what to do
    	
		if(!owner.avdNum.equals(myDetails.avdNum))
    	{
    		//send to owner
    		Message.sendMessage(MessageType.objectInsertMessage,null,myAVDnum,KV_pair,owner.portNum);
    	}
    	else // i am the owner
    	{
    			Log.e("window_shopper","I am the owner of this key");
        		this.insert(KV_pair,null);
        		Message.sendMessage(MessageType.objectReplicationMessage,null,myAVDnum,KV_pair,chord.mysucc.portNum);
		}
	
    	// waiting for chain replication to complete
    	// only then shall i respond
		synchronized (_insert_Lock)
		{
			try
			{
				/**
				 *  wait for 100 milli seconds
				 *  if i don't get response then do something
				 */
				//_insert_Lock.wait(); // means someone did not respond
				Log.e("window_shopper","----------- WAITING FOR OBJECT INSERT RETURN -------------------");
				_insert_Lock.wait(1500); // means someone did not respond
			}
			catch(InterruptedException iex)
			{
				Log.e("window_shopper","\n\n Insert wait has thrown this error have encountered an exception when waiting\n\n");
				iex.printStackTrace();
			}
		}
		
		getContext().getContentResolver().notifyChange(uri, null);
        return simpleDHTURI;
    }

    /*=========================================================================
     * Function   : delete
     * Description: handles delete requests generated programmatically, requests
     * 				coming from the predecessor
     * Parameters : String del_lvl,String _param
     * Return	  : Uri 
     *=========================================================================*/
    public Integer delete(String[] keys)
    {
    	Integer retVal = 0;
    	Log.e("recovery","I need to delete::"+keys.length+" rows");
    	String bull[] = new String[1];
    	for(int i=0;i<keys.length;i++)
    	{
    		bull[0] = keys[i];
    		retVal+=sqliteDB.delete(tableName, "key=?",bull);	
    	}
    	
    	Log.e("recovery","I have deleted::"+retVal+" rows");
    	return retVal;
    	//String _del_keys = TextUtils.join(", ",keys);
    	//sqliteDB.execSQL(String.format("DELETE FROM "+tableName+" WHERE key IN (%s);",_del_keys));
    }
    /*=========================================================================
     * Function   : delete
     * Description: handles delete requests generated programmatically, requests
     * 				coming from the predecessor
     * Parameters : String del_lvl,String _param
     * Return	  : Uri 
     *=========================================================================*/
    public Integer delete(String del_lvl,String _param)
    {
    	int retVal = 0;
    	
    	if(del_lvl.equals(delete_all)) 
    	{
    		retVal =  sqliteDB.delete(tableName,null,null);
    	}
    	else if(del_lvl.equals(delete_particular))
    	{
			String[] delArgs={_param};
			retVal =  sqliteDB.delete(tableName,"key=?",delArgs);
			if(retVal == 0) retVal = OBJECT_DOES_NOT_EXIST;
    	}
    	else
    	{
    		Log.e("window_shopper","wrong del_lvl sent"+del_lvl);
    	}
    	return retVal;
    }
    
    /*=========================================================================
     * Function   : query()
     * Description: Query for an object in the database -- overloaded method
     * Parameters : String queryLevel,Object _param
     * Return	  : Hashtable<String,String> 
     *=========================================================================*/
    public Hashtable<String,String> query(String queryLevel,Object _param) // making this sync can break the system
    {
    	
    	Hashtable<String, String> queryResult = null;
    	String query = null;
    	//String TAG = "query_shopper";
    	String TAG = "window_Shopper";
    	/**
    	 * query the whole DHT
    	 */
    	if(queryLevel.equals(query_all)) 
    	{
    		query ="select * from "+tableName;
    		Cursor cursor =  sqliteDB.rawQuery(query,null);
    		queryResult = unpack_cursor(cursor);
    		
    		
    		
    		// need to append only if both are not null
    		if(_param != null && queryResult != null)
    		{
	    		appendRecords(queryResult,(Hashtable<String, String>)_param);
	    		queryResult = (Hashtable<String, String>)_param;
    		}
    		else if(queryResult == null) queryResult = (Hashtable<String, String>)_param;
    	}
    	else if(queryLevel.equals(query_mine))
    	{
    		/*
    		 *  since recovery always get from successor
    		 *  this guy may not have what actually belongs
    		 *  to pred -- so having stuff written before 
    		 *  querying mine
    		 */
    		synchronized (_DBWRITE_Lock)
			{
    			Log.e(TAG,"Handling recovery!!");
	    		query ="select * from "+tableName;
	    		Cursor cursor =  sqliteDB.rawQuery(query,null);
	    		queryResult = unpack_cursor(cursor);
			}
    	}
    	else if(queryLevel.equals(query_particular))
    	{
    		String[] queryArgs={(String)_param};
			
    		// Cannot read when writing is happening
			synchronized (_DBWRITE_Lock)
			{
				Log.d(TAG,"Program Looking for::"+queryArgs[0]);
				query = "select * from "+tableName+" where key=?";
				Cursor cursor =  sqliteDB.rawQuery(query,queryArgs);
				if(cursor == null) Log.e(TAG,"I don't have the key");
				else
				{
					queryResult = unpack_cursor(cursor);
				}
			}
		}
    	else
    	{
    		Log.e(TAG,"wrong queryLevel send"+queryLevel);
    	}
		return queryResult;
    }
    
    /*=========================================================================
     * Function   : query
     * Description: Query the database to fetch the requested items
     * Parameters : Uri uri, String[] projection, String selection, 
     * 				String[] selectionArgs,String sortOrder
     * Return	  : Cursor 
     *=========================================================================*/
   synchronized public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,String sortOrder) 
    {
    	SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
		queryBuilder.setTables(tableName);
        sqliteDB = db_conduit.getReadableDatabase();
        Cursor queryResult = null;
        String query = null;
        String[] colnames={"key","value"};
        MatrixCursor matCursor =  null;
        //String TAG = "query_shopper";
        String TAG = "window_shopper";
        
        // debug mandi
       /* String countSize = "select * from "+tableName;
    	Cursor n = sqliteDB.rawQuery(countSize,null);
    	Log.d(TAG,"rowCount::"+n.getCount());
    	*/
    	if(selection.equals(query_all))
        {
    		//Log.d(TAG,"someone asked for GDUMP ? inside query method");
    		query = "select * from "+tableName;
        	queryResult = sqliteDB.rawQuery(query,null);
        	queryResult.moveToFirst();
        	
        	_GDump_Lock.records = unpack_cursor(queryResult);
        	
        	// send GDUMP request message to successor
        	Message.sendMessage(MessageType.GDumpQueryMessage,query_all,myDetails.avdNum,_GDump_Lock.records,chord.mysucc.portNum);
        	
    		synchronized (_GDump_Lock) 
    		{
    			try
        		{
					Log.d(TAG,"--- waiting for the GDUMP ----");
    				_GDump_Lock.wait();// wait for successor to respond
    			}
        		catch(InterruptedException iex)
        		{
        			iex.printStackTrace();
        			Log.e(TAG,"was waiting on GDUMP");
        		}
    			Log.d(TAG,"---- Have recvd GDUMP ----");
    			matCursor = construct_MatrixCursor(_GDump_Lock.records,colnames);
    		}
		}
        else if(selection.equals(query_mine))
        {
        	query = "select * from "+tableName;
        	queryResult = sqliteDB.rawQuery(query,null);
        	Hashtable<String,String> unpack= unpack_cursor(queryResult);
        	notDomain_delete(unpack);
        	matCursor = construct_MatrixCursor(unpack,colnames);
        	
        	
        }
        else // query for a particular object
        {	
        	
        	Log.d(TAG,"Tester looking for::"+selection);
        	String tester_query = selection;
        	query = "select * from "+tableName+" where key=?";
        	
        	try
        	{
        		String hashedKey  =  ChordMaster.genHash(selection);
        		node[] queryNodes = chord.getQueryNodes(hashedKey);
        		Hashtable<String,String> records = null;
        		
        		// list all my brothers
        		for(int i=0;i<queryNodes.length;i++)
        		{
        			Log.e(TAG,"The query nodes are "+i+"::"+queryNodes[i].avdNum);
        		}
        		
        		// ask brothers starting from the eldest one
        		for(int i=queryNodes.length-1;i>=0;i--)
        		{
        			Log.e(TAG,"The query node no "+i+"::"+queryNodes[i].avdNum);
        			
        			/**
        			 *  if we know that this node is down then why bother with query
        			 *  lets query the other query nodes
        			 */
        			if(queryNodes[i].avdNum.equals(myAVDnum))
        			{
        				records = this.query(query_particular,selection);
    	    			if(records != null)
    	    				break;
    	    			else // result null -- try brother nodes
    	    			{
    	    				Log.e(TAG,"Query Fetch failed");
    	    				continue;
	    				}
    	        		
        			}
        			else  
		        	{
		        		Message.sendMessage(MessageType.objectQueryMessage,query_particular,myAVDnum,selection,queryNodes[i].portNum);
		        		synchronized (_query_Lock)
		        		{
		        			try
		        			{
		        				Log.e(TAG,"----------- WAITING FOR OBJECT QUERY RETURN -------------------");
		        				_query_Lock.wait(500); // wait for the response
		        				records = _query_Lock.records;
		        				_query_Lock.records = null;
		        			}
		        			catch(InterruptedException iex)
		        			{
		        				iex.printStackTrace();
		        				Log.e("query_shopper","Interrupted Exception waiting on object query return");
		        			}
		        			
		        			if(records != null)
	        				{
		        				//if(records.keys().nextElement().equals(tester_query))
		        				{
		        					break;
		        				}
		        				/*else
		        				{
		        					if(i==0) i=2;
		    	    				continue;
		        				}*/
	        				}
		        			else
		        			{
		        				Log.e(TAG,"\n\n\nQuery Fetch failed\n\n\n");
		        				if(i==0) i=2;
	    	    				continue;
		    				}
		        		}// synchronized
		        	}
        		}//for loop
        		
        		if(records != null)
        		{
	        		Log.d(TAG,"Query Answered :: size of hash returned query"+records.size());
	    			Log.v(TAG,"Result is------------------------------------->::"+records.keys().nextElement()+"::"+records.get(records.keys().nextElement()));
        		}
        		else
        		{
        			Log.e(TAG,"\n\n\nQuery Fetch has failed\n\n\n");
        		}
        		// pack the result in records <hashtable> into matrixCursor
    			matCursor =  construct_MatrixCursor(records, colnames);
    		}
        	catch(NoSuchAlgorithmException nex)
        	{
        		nex.printStackTrace();
        	}
    	}
        
    	// debug code that tell us state
       if(matCursor == null) 
       {
	    	Log.d(TAG,"Query Failure ? -- matcursor null");
	    	return null;
       }   
       else
       {
    	   matCursor.moveToFirst();
           Log.v(TAG,"num rows returned_returned cursor::"+matCursor.getCount());
           Log.v(TAG,"=========== MAT CUSOR ========");
           printCursor(matCursor);
           matCursor.moveToFirst();
       }  
        // make sure that potential listeners are getting notified
        matCursor.setNotificationUri(getContext().getContentResolver(), uri);
        Log.v(TAG,"Take this tester in your face :: "+selection);
        return matCursor;
    }
    
    
    /*=========================================================================
     * Function   : printCursor
     * Description: Prints the cursor object's contents
     * Parameters : Cursor
     * Return	  : void
     *=========================================================================*/
    private void printCursor(Cursor cursor)
    {
    	if(cursor == null) return;
    	if(cursor.getCount() == 0) return;
    	
    	cursor.moveToFirst();
    	int index = 1;
    	int keyIndex = cursor.getColumnIndex(KEY_FIELD);
		int valueIndex = cursor.getColumnIndex(VALUE_FIELD);

		Log.d("window_shopper","row_"+index+"::"+cursor.getString(keyIndex)+"::"+cursor.getString(valueIndex));
		index++;
		while(cursor.moveToNext())
    	{
    		Log.d("window_shopper","row_"+index+"::"+cursor.getString(keyIndex)+":"+cursor.getString(valueIndex));
    		index++;
    	}
    	cursor.moveToFirst();
    	cursor.close();
    	return;
    }
    
    /*=========================================================================
     * Function   : appendRecords()
     * Description: Appends records from one hashtable to another
     * Parameters : Hashtables from <my LDUMP> & to <GDUMP from predecessor>
     * Return	  : void
     *=========================================================================*/
    void appendRecords(Hashtable<String,String> LDUMP,Hashtable<String,String> GDUMP)
    {	
    	/**
		 * GDUMP from pred and MY LDUMP are null -- do-nothing
		 * My LDUMP is null -- nothing to append
		 * GDUMP is null -- just make "to" <LDUMP> = "from"  <GDUMP>
     	*/	
    	Set<String> keys = LDUMP.keySet();
    	for(String key:keys)
    	{
    		GDUMP.put(key,LDUMP.get(key));
    	}
	
    	return;
    }
    
    /*=========================================================================
     * Function   : update
     * Description: 
     * Parameters : Uri uri, String[] projection, String selection, 
     * 				String[] selectionArgs,String sortOrder
     * Return	  : Cursor 
     *=========================================================================*/
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) 
    {
        return 0;
    }

    
       
    /*=========================================================================
	 * Function   : delete
	 * Description: deletes the KV_pair from the sqlite database
	 * Parameters : Uri uri, String selection,String[] selectionArgs
	 * Return	  : int
	 *=========================================================================*/
	public int delete(Uri uri, String selection, String[] selectionArgs) 
	{
		int retVal = 0;
		
		if(selection.equals(delete_all)) // delete all in DHT
		{
		 	retVal = sqliteDB.delete(tableName, null, null);
		 	
		 	Message.sendMessage(MessageType.GDelReqMessage,delete_all,myAVDnum,null,chord.mysucc.portNum);
		 	
		 	synchronized (_GDel_Lock) 
		 	{
		 		try
		 		{
		 			_GDel_Lock.wait();
		 		}
		 		catch(InterruptedException iex)
		 		{
		 			iex.printStackTrace();
		 		}
		 		retVal = _GDel_Lock.rows_affected;
		 		Log.d("window_shopper","All peers have deleted");
			}
		}
		else if(selection.equals(delete_mine)) // delete all of mine
		{
	    	retVal = sqliteDB.delete(tableName,null, null);
		}
		else // delete_particular
		{
			try
			{
				/*
				 * find owner of key
				 */
				Log.v("window_shopper","deleting::"+selection);
				String queryArgs[]={selection};
				String hashedKey = ChordMaster.genHash(selection);
				node owner = chord.getOwner(hashedKey);
				
				if(owner.avdNum.equals(myAVDnum)) // i am owner
				{
					retVal = sqliteDB.delete(tableName,"key=?",queryArgs);
					// need to delete replications as well
					Message.sendMessage(MessageType.objectDelReplicaMessage,delete_particular,myAVDnum,selection,chord.mysucc.portNum);
				}
				else // i am not the owner
					Message.sendMessage(MessageType.objectDelReqMessage,delete_particular,myAVDnum,selection,owner.portNum);
				
				
				synchronized (_delObj_Lock)
				{
					try
    				{
    					Log.v("window_shopper","Waiting for del operations to complete!");
    					_delObj_Lock.wait();
    				}
    				catch(InterruptedException iex)
    				{
    					iex.printStackTrace();
    				}
    				
    				retVal = _delObj_Lock.rows_affected;
    				if(retVal == OBJECT_DOES_NOT_EXIST)
    					Log.v("window_shopper","CANNOT DELETE OBJ DOES NOT EXIST"+retVal);        					
    				else
    					Log.v("window_shopper","The delete operation was successful"+retVal);
    			}
				
			}
			catch(NoSuchAlgorithmException nex)
			{
				nex.printStackTrace();
			}
		}
		return retVal;
	}



	/*=========================================================================
     * Function   : unpack_cursor
     * Description: copies the contents of a Cursor object into a Hashtable
     * Parameters : 
     * Return	  : Hashtable<String,String> 
     *=========================================================================*/
    Hashtable<String, String> unpack_cursor(Cursor cursor)
    {
    	Hashtable<String, String> result = null;
    	if(cursor == null) return null;
    	else if(cursor.getCount() == 0) return null;
    	else
    	{
    		result= new Hashtable<String, String>();
	    	cursor.moveToFirst(); 
	    	int keyIndex = cursor.getColumnIndex(KEY_FIELD);
			int valueIndex = cursor.getColumnIndex(VALUE_FIELD);
			result.put(cursor.getString(keyIndex), cursor.getString(valueIndex));
			while (cursor.moveToNext()) 
	    	{
	    		result.put(cursor.getString(keyIndex), cursor.getString(valueIndex));
	    	}
    	}
    	return result;
    }
    
    /*=========================================================================
     * Function   : unpack_cursor (overloaded method)
     * Description: copies the content of a cursor object into the given 
     * 				Hashtable<String,String>
     * Parameters : Cursor cursor, Hashtable<String,String> result
     * Return	  : Hashtable<String,String> 
     *=========================================================================*/
    Hashtable<String, String> unpack_cursor(Cursor cursor,Hashtable<String, String> result)
    {
    	if(cursor == null) return null;
    	cursor.moveToFirst(); 
    	if(result == null)
    	result = new Hashtable<String, String>();
    	
    	int keyIndex = cursor.getColumnIndex(KEY_FIELD);
		int valueIndex = cursor.getColumnIndex(VALUE_FIELD);
		while (cursor.moveToNext()) 
    	{
    		result.put(cursor.getString(keyIndex), cursor.getString(valueIndex));
    	}
    	return result;
    }
    
    
    /*=========================================================================
     * Function   : construct_MatrixCursor
     * Description: Given a cursor  constructs a MatrixCursor with contents of
     * 				the former
     * Parameters : Cursor records,String[] columnNames
     * Return	  : MatrixCursor
     *=========================================================================*/
    private MatrixCursor construct_MatrixCursor(Cursor records,String[] columnNames)
    {
    	if(records == null || records.getCount() == 0) return null;
    	String[] rowItem = new String[2];
    	MatrixCursor matCursor = new MatrixCursor(columnNames);
    	records.moveToFirst();
    	
    	int keyIndex = records.getColumnIndex(KEY_FIELD);
		int valueIndex = records.getColumnIndex(VALUE_FIELD);
		
		rowItem[0] = records.getString(keyIndex);
		rowItem[1] = records.getString(valueIndex);
		matCursor.addRow(rowItem);
		
		while(records.moveToNext())
    	{
    		rowItem[0] = records.getString(keyIndex);
    		rowItem[1] = records.getString(valueIndex);
    		matCursor.addRow(rowItem);
    	}
		matCursor.moveToFirst();
		records.moveToFirst();
		return matCursor;
    }
    
    /*=========================================================================
     * Function   : construct_MatrixCursor(overloaded method)
     * Description: Given a Hashtable construct a matrix cursor with content
     * 					of the former
     * Parameters : Hashtable<String, String> records,String[] columnNames
     * Return	  : Cursor 
     *=========================================================================*/
    private MatrixCursor construct_MatrixCursor(Hashtable<String, String> records,String[] columnNames)
    {
    	if(records == null) return null;
    	MatrixCursor matCursor = new MatrixCursor(columnNames);
    	String[] rowItem = new String[2];
    	Set<String> keys = records.keySet();
    	for(String key:keys)
    	{
    		rowItem[0] = key;
    		rowItem[1] = records.get(rowItem[0]);
    		matCursor.addRow(rowItem);
    	}
    	return matCursor;
    }
    
    private void notDomain_delete(Hashtable<String, String>unpack)
    {
    
    	Set<String> keys = unpack.keySet();
    	String[] _del = chord.get_nonDomain(keys);
    	
    	if(_del == null) return;
    	
    	String[] bull = new String[1];
    	for(int i =0; i<_del.length;i++)
    	{
    		bull[0] = _del[i];
    		sqliteDB.delete(tableName, "key=?",bull);
    		unpack.remove(_del[i]);
    	}
    }
}



/*
queryResult = sqliteDB.rawQuery(query,queryArgs);
if(queryResult == null)
{
	Log.e("window_shopper","Eerything is now lost\n");
	
}
matCursor =  construct_MatrixCursor(queryResult, colnames);
*/
