package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

import edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider.dynamoLock;
import android.util.Log;

/*=========================================================================
 * Class   	  : MessageProcessors
 * Description: A thread that is kick started in the ServerOps class to
 * 				handle incoming request/responses
 * Author	  : RAJARAM RABINDRANATH
 *=========================================================================*/
public class MessageProcessor extends Thread 
{
	Message msg = null;
	SimpleDynamoProvider dynamoDB = null;
	static String TAG = MessageProcessor.class.getName();
	// query string
	static String query_all = "*";
	static String query_particular = "-";
	Socket clientPipe = null;
	
	public MessageProcessor(Message msg,SimpleDynamoProvider dynamoDB,Socket clientPipe)
	{
		this.msg = msg;
		this.dynamoDB= dynamoDB;
		this.clientPipe =  clientPipe;
		if(msg.msgType != MessageType.joinMessage)
		{
			sendAck(clientPipe);
		}
	}

	/*=========================================================================
	 * Function   	: sendAck
	 * Description  : send acknowledgement of having received the message
	 * Author		: RAJARAM RABINDRANATH
	 *========================================================================*/
	private void sendAck(Socket socket)
	{
		try
		{
			Message pingAck = Message.ackMsg(dynamoDB.myAVDnum);
			ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
			oos.writeObject(pingAck);  
			oos.flush();
			oos.close();
			socket.close();
		}
		catch(IOException ioex)
		{
			Log.e("window_shopper","\n\nproblems with sending ack!!!\n\n");
			ioex.printStackTrace();
		}
	}

	/*=========================================================================
     * Function   : run
     * Description: handles a received message and takes the appropriate actions
     * Parameters : void
     * Return	  : void 
     *=========================================================================*/
	public void run()
	{
		Log.e("window_shopper","Message recvd is::"+msg.msgType+"::"+msg.msgOrigin);

		// send ack satisfy
		
		
		switch(msg.msgType)
		{
		
			case joinMessage:
				//dynamoDB.chord._setNodeStatus(msg.msgOrigin,nodeStatus.up);
				_reassignKeys();
				break;
			case recoverKeysMessage:
				_recoverKeys();
				break;
			case GDumpQueryMessage:
				_GDumpQuery();
				break;
			
			case objectInsertMessage:
				_ObjInsert();
				break;
			
			case objectQueryMessage:
				if(msg.msgOrigin.equals(dynamoDB.myAVDnum))
				{
					Log.e("window_shopper","I have got my own request -- nobody can be as stupid as me");
					synchronized (dynamoDB._query_Lock)
					{
						dynamoDB._query_Lock.records = null;
						dynamoDB._query_Lock.notify();
					}
				}
				else
				{
					_ObjQuery();
					
				}
				
				break;
			
			case GDumpQueryResponseMessage:
				synchronized (dynamoDB._GDump_Lock) 
				{
					dynamoDB._GDump_Lock.records = (Hashtable<String, String>)msg.payload;
					dynamoDB._GDump_Lock.notify();
				}
				break;
			
			case objectQueryResponseMessage:
				synchronized (dynamoDB._query_Lock)
				{
					dynamoDB._query_Lock.records = (Hashtable<String, String>)msg.payload;
					dynamoDB._query_Lock.notify();
				}
				break;
				
			case objectInsertResponseMessage:
				synchronized (dynamoDB._insert_Lock)
				{
					Log.e("window_shopper","the object have been inserted!");
					//dynamoDB._insert_Lock.setGood(true);
					dynamoDB._insert_Lock.notify();
				}
				break;
			case GDelReqMessage:
				_dhtDel();
				break;
			case GDelReqResponseMessage:
				synchronized (dynamoDB._GDel_Lock)
				{
					dynamoDB._GDel_Lock.rows_affected = (Integer)msg.payload;
					dynamoDB._GDel_Lock.notify();
				}
				break;
				
			case objectDelReqMessage:
				_ObjDel();
				break;
			
			case objectDelReqResponseMessage:
				synchronized (dynamoDB._delObj_Lock)
				{
					dynamoDB._delObj_Lock.rows_affected = (Integer)msg.payload;
					dynamoDB._delObj_Lock.notify();
				}
				break;
			case objectDelReplicaMessage:
				_ObjDelReplica();
				break;
			
			case objectReplicationMessage:
				_ObjReplicate();
				break;
			case objectReplicationRespMessage:
				synchronized (dynamoDB._insert_Lock)
				{
					String[] a = (String[]) msg.payload;
					Log.e("window_shopper","the object have been inserted::"+a[0]+"::"+a[1]+"-->"+msg.msgOrigin);
					dynamoDB._insert_Lock.notify();
				}
				break;
			default:
				Log.e("window_shopper","life can be cruel sometimes");
					break;
		}
	}
	
	/*=========================================================================
     * Function   : _ObjQuery()
     * Description: handles the object query request from client
     * Parameters : void
     * Return	  : void 
     *=========================================================================*/
	private void _reassignKeys()
	{
		/**
		 * Is the recovered node my predecessor ?
		 * -- send all keys that are not mine
		 * -- delete all keys that are not in my domain
		 */
		
		Log.e("window_shopper","Recovered Node::"+msg.msgOrigin);
		Log.v("window_shopper","\n\n recovery management got a message from :: "+msg.msgOrigin+"\n\n");
		if(msg.msgOrigin.equals(dynamoDB.chord.mypred.avdNum))
		{
			
			Hashtable<String, String> mine = dynamoDB.query(dynamoDB.query_mine,null);
			if(mine!=null)
			{
				/**
				 * 1. send keys to - N
				 */
				Hashtable<String,String> sendToNode = new Hashtable<String, String>();
				Set<String> keys = mine.keySet();
				
				Log.e("window_shopper","My pred is back::"+msg.msgOrigin);
				Log.e("window_shopper","Rows in my DB::"+keys.size()+"--"+mine.size());
				
				for(String key : keys)
				{
					// give everything but mine
					if(!dynamoDB.chord.isOwner(key,dynamoDB.myAVDnum))
					{
						sendToNode.put(key,mine.get(key));
						//mine.remove(key); // my preds key is mine too saving work for part 2.
					}
				}
				
				if(sendToNode.size()!=0)
				{
					Log.e("window_shopper","My Pred needs to recover::"+sendToNode.size()+" rows");
					Message.sendMessage(MessageType.recoverKeysMessage,null,dynamoDB.myAVDnum,sendToNode,msg.originPort);
				}
				
				/**
				 * 2. delete non-domain keys 
				 */
				String[] _delete = dynamoDB.chord.get_nonDomain(mine.keySet());
				if(_delete != null)dynamoDB.delete(_delete);
			}
		}
		/**
		 * Is the recovered node my successor ?
		 * -- do nothing -- you are ok
		 */
		else if(msg.msgOrigin.equals(dynamoDB.chord.mysucc.avdNum))
		{
			// get mine and preds keys and send it across
			Hashtable<String, String> mice = dynamoDB.query(dynamoDB.query_mine,null);
			if(mice!=null)
			{
				/**
				 * 1. send keys to - N
				 */
				Hashtable<String,String> send = new Hashtable<String, String>();
				Set<String> keys = mice.keySet();
				
				Log.e("window_shopper","My succ is back::"+msg.msgOrigin);
				Log.e("window_shopper","Rows in my DB::"+keys.size()+"--"+mice.size());
				
				for(String key : keys)
				{
					// give everything but mine
					String belongsTo = dynamoDB.chord.whoOwner(key); 
					
					if(belongsTo.equals(dynamoDB.myAVDnum)||belongsTo.equals(dynamoDB.chord.mypred.avdNum))
					{
						send.put(key,mice.get(key));
						//mine.remove(key); // my preds key is mine too saving work for part 2.
					}
				}
				
				if(send.size()!=0)
				{
					Log.e("window_shopper","My Pred needs to recover::"+send.size()+" rows");
					Message.sendMessage(MessageType.recoverKeysMessage,null,dynamoDB.myAVDnum,send,msg.originPort);
				}
			}
			Log.e("window_shopper","My succ Node::"+msg.msgOrigin+"  ::do something");
		}
		/**
		 * otherwise -- recovered node is -- pred -1 / succ + 1
		 * --- delete all keys not in my domain 
		 */
		else
		{
			Hashtable<String, String> mine = dynamoDB.query(dynamoDB.query_mine,null);

			
			/**
			 * 2. delete non-domain keys 
			 */
			if(mine!=null)
			{
				String[] _delete = dynamoDB.chord.get_nonDomain(mine.keySet());
				if(_delete!=null)dynamoDB.delete(_delete);
			}
		}
	}
	
	/*=========================================================================
     * Function   : _ObjQuery()
     * Description: handles the object query request from client
     * Parameters : void
     * Return	  : void 
     *=========================================================================*/
	private void _recoverKeys()
	{
		if(msg.payload instanceof Hashtable)
		{
			Hashtable<String, String> assign = (Hashtable<String, String>) msg.payload;
			if(assign!=null)
			{
				Log.e("window_shopper","Have to recover::"+assign.size()+" rows");
				dynamoDB.insert(assign,msg.msgOrigin);
			}
		}
		else
		{
			Log.e("window_shopper","\n\n what a looooser \n\n");
		}
	}
	
	/*=========================================================================
     * Function   : _ObjQuery()
     * Description: handles the object query request from client
     * Parameters : void
     * Return	  : void 
     *=========================================================================*/
	private void _ObjQuery()
	{
		Hashtable<String, String> queryResult = dynamoDB.query(msg.query,msg.payload);
		
		if(queryResult!=null) // end the request chain
		{
			Log.d("window_shopper","Query Successful :: The size of queryResult =::"+queryResult.keys().nextElement());
		}
		else // i don't have it sending to successor
		{
			//msg.forwardMessage(dynamoDB.chord.mysucc.portNum);
			Log.d("window_shopper","Query Failed to fetch::"+msg.payload);
		}
		Message.sendMessage(MessageType.objectQueryResponseMessage,null,dynamoDB.myAVDnum,queryResult,msg.originPort);
	}
	
	/*=========================================================================
     * Function   : _ObjDel()
     * Description: handles the object delete request from predecessor
     * Parameters : void
     * Return	  : void 
     *=========================================================================*/
	private void _ObjDel()
	{
		// just a debug check
		if(msg.msgOrigin.equals(dynamoDB.myAVDnum))
		{
			Log.e("window_shopper","nobody can be more stupid than me");
		}
		Integer retVal = dynamoDB.delete(msg.query, (String)msg.payload);

		if(retVal > 0) // delete was successful
			Log.v("window_shopper","Delete opertion on the owner was successful");
		else // did not find key -- forwarding request to sender
			Log.e("window_shopper","Delete at owner was not successful!");
		
		// need to delete replicas as well	
		msg.msgType = MessageType.objectDelReplicaMessage;
		msg.TTL = 2;
		msg.forwardMessage(dynamoDB.chord.mysucc.portNum);
	}
	
	/*=========================================================================
     * Function   : _ObjDelReplica()
     * Description: handles the object delete request from predecessor
     * Parameters : void
     * Return	  : void 
     *=========================================================================*/
	private void _ObjDelReplica()
	{
		Integer result = dynamoDB.delete(msg.query,(String)msg.payload);
		// one more replication required ? TTL == 1 means i am the last guy
		
		Log.e("window_shopper","the TTL"+msg.TTL);
		if(msg.TTL == 2) // do we need one more replication
		{
			msg.TTL--;
			msg.forwardMessage(dynamoDB.chord.mysucc.portNum);
		}
		else // send response message to allow the originator to proceed
 		{
			Message.sendMessage(MessageType.objectDelReqResponseMessage,null,dynamoDB.myAVDnum,result, msg.originPort);
		}
	}
	
	/*=========================================================================
     * Function   : dhtDel
     * Description: handles the delete all propagation request from the predecessor
     * Parameters : void
     * Return	  : void 
     *=========================================================================*/
	private void _dhtDel()
	{		
		// another bebug check
		node succ = dynamoDB.chord.mysucc;
		
		if(msg.msgOrigin.equals(dynamoDB.myAVDnum))
		{
			Log.e("window_shopper","nobody can be more stupid than me");
		}
		else // pred has asked me to get the job done
		{
			Integer retVal = dynamoDB.delete(msg.query,null);
			Integer newPayload =  retVal + (Integer)msg.payload;
			// put in the highest value
			if(succ.avdNum.equals(msg.msgOrigin))
			{
				Message.sendMessage(MessageType.GDelReqResponseMessage,null,dynamoDB.myAVDnum,(Object)newPayload, msg.originPort);
			}
			else
			{
				msg.forwardMessage(succ.portNum);
			}
		}
	}
	
	
	/*=========================================================================
     * Function   : _ObjInsert()
     * Description: handles object insert request
     * Parameters : void
     * Return	  : void 
     *=========================================================================*/
	private void _ObjInsert()
	{
		String[] payload = (String[]) msg.payload;
		Log.e("window_shopper","\n\nBEEN ASKED TO INSERT::"+payload[0]+"::"+payload[1]+"-->"+msg.msgOrigin);
		dynamoDB.insert((String[])msg.payload,null);
		// replication
		msg.msgType = MessageType.objectReplicationMessage;
		msg.TTL = 2;
		msg.forwardMessage(dynamoDB.chord.mysucc.portNum);
	}
	
	/*=========================================================================
     * Function   : _ObjReplicate()
     * Description: handles object insert request from the predecessor
     * Parameters : void
     * Return	  : void 
     *=========================================================================*/
	private void _ObjReplicate()
	{
		Boolean result = Boolean.valueOf("false");
		String[] payload = (String[]) msg.payload;
		Log.e("window_shopper","\n\nBEEN ASKED TO REPLICATE::"+payload[0]+"::"+payload[1]+"-->"+msg.msgOrigin);
		
		result = (Boolean)dynamoDB.insert((String[])msg.payload,null);
		// one more replication required ? TTL == 1 means i am the last guy
		
		Log.e("window_shopper","the TTL"+msg.TTL);
		if(msg.TTL == 2) // do we need one more replication
		{
			msg.TTL--;
			msg.forwardMessage(dynamoDB.chord.mysucc.portNum);
		}
		else // send response message to allow the originator to proceed
 		{
			if(msg.msgOrigin.equals(dynamoDB.myAVDnum))
			{
				synchronized (dynamoDB._insert_Lock)
				{
					String[] a = (String[]) msg.payload;
					Log.e("window_shopper","I know the object have been inserted::"+a[0]+"::"+a[1]+"-->"+msg.msgOrigin);
					dynamoDB._insert_Lock.notify();
				}
			}	
			else
			{
				Message.sendMessage(MessageType.objectReplicationRespMessage,null,dynamoDB.myAVDnum,msg.payload, msg.originPort);	
			}
		}
		return;
	}
	
	
	/*=========================================================================
     * Function   : _GDumpQuery()
     * Description: handles the Global Dump query request
     * Parameters : void
     * Return	  : void 
     *=========================================================================*/
	private void _GDumpQuery()
	{
		Hashtable<String, String> result = null;
		node succ = dynamoDB.chord.mysucc;
		
		// I have received my own message -- some failure must have caused this
		if(msg.msgOrigin.equals(dynamoDB.myAVDnum))
		{			
			// ihave got my own query
			synchronized (dynamoDB._GDump_Lock) 
			{
				dynamoDB._GDump_Lock.records = (Hashtable<String, String>)msg.payload;
				dynamoDB._GDump_Lock.notify();
			}
			
		}
		else
		{
			/**
			 * get my dump
			 * check if successor is the originator -- 
			 * if yes send queryResponse message 
			 */
			result  = dynamoDB.query(query_all,msg.payload);
			if(succ.avdNum.equals(msg.msgOrigin))
			{
				Message.sendMessage(MessageType.GDumpQueryResponseMessage,null,dynamoDB.myAVDnum,result,succ.portNum);
			}
			else // forward message to successor for GDump 
			{
				msg.payload = result;
				msg.forwardMessage(dynamoDB.chord.mysucc.portNum);
			}
		}
		return;
	}				
}



/*
try
{
		
	 * String owner_pred_hash = dynamoDB.chord.getPredNode_hash(msg.originPort);
	 * String owner_hash = dynamoDB.chord.genHash(msg.msgOrigin);
	
	{
		Hashtable<String, String> recover = dynamoDB.query(dynamoDB.query_mine,null);
		
		if(recover != null)
		{
			Iterator<String> iter =  recover.keySet().iterator();
			// fork it!!!
			while(iter.hasNext())
			{
				String hashedkey = ChordMaster.genHash(iter.next());
				*//**
				 * don't send keys whose 
				 * A. Rightful owner is not s
				 * B. not mine
				 *//*
				//if(dynamoDB.chord.getOwner(hashedkey).equals(msg.msgOrigin))//||!dynamoDB.chord.getOwner(hashedkey).equals(dynamoDB.myAVDnum))
				if(dynamoDB.chord.getOwner(hashedkey).equals(dynamoDB.myAVDnum))
				recover.remove(hashedkey);
			}
			Message.sendMessage(MessageType.recoverKeysMessage,null,dynamoDB.myAVDnum,recover,msg.originPort);
		}
	}
}
catch(NoSuchAlgorithmException nsa)
{
	nsa.printStackTrace();
}*/