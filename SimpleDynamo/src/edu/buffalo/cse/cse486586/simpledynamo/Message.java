package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Set;

import android.util.Log;

/*=========================================================================
 * Class name   : Message
 * Description  : A template for a message object
 * Author		: RAJARAM RABINDRANATH
 *=========================================================================*/
public class Message implements Serializable
{
	
	private static final long serialVersionUID = -6893921450147855832L;
	
	MessageType msgType =  null;
	String msgOrigin =  null;
	String query = null;
	Object payload = null;
	String originPort = null;
	int toPort = 0; // may come in use later
	int TTL = 0;
	
	private Message(MessageType msgType,String query, String myAvdNum, Object payload)
	{
		this.msgType = msgType;
		this.query = query;
		this.msgOrigin =  myAvdNum;
		this.originPort = Integer.toString((Integer.parseInt(this.msgOrigin)*2));
		this.payload =  payload;
		if(msgType == MessageType.objectReplicationMessage || msgType == MessageType.objectDelReplicaMessage) TTL = 2;
	}
	
	/*=========================================================================
     * Function   : ackMsg()
     * Description: 
     * Parameters : void
     * Return	  : String toPort 
     *=========================================================================*/
	public static Message ackMsg(String avdNum)
	{
		return (new Message(MessageType.pingAck,null, avdNum,null));
	}
	
	/*=========================================================================
     * Function   : forwardMessage()
     * Description: Forward the request from predecessor to successor
     * Parameters : void
     * Return	  : String toPort 
     *=========================================================================*/
    public void forwardMessage(String toPort)
	{
    	Boolean forwardSuccess = Boolean.valueOf(true);
		ClientOps forwardRequest = new ClientOps(new sendMsgParams(this,toPort,forwardSuccess));
		forwardRequest.start();
	}
	
    /*=========================================================================
     * Function   : sendMessage()
     * Description: Sends a message to successor -- could be anyone of the 12 
     * 				message types defined in MessageType enum
     * Parameters : a lot of them
     * Return	  : void
     *=========================================================================*/
    public static void sendMessage(MessageType msgType,String query,String avdNum,Object payload,String toPort)
	{
		Message respMsg = new Message(msgType,query,avdNum,payload);
		Boolean sendSuccess = Boolean.valueOf(false);
		ClientOps sendRequest = new ClientOps(new sendMsgParams(respMsg,toPort,sendSuccess));
		sendRequest.start();
	}
    
    
    /*=========================================================================
     * Function   : sendMessage()
     * Description: Sends a message to successor -- could be anyone of the 12 
     * 				message types defined in MessageType enum
     * Parameters : a lot of them
     * Return	  : void
     *=========================================================================*/
    public static void joinMessage(String avdnum)
    {
    	ArrayList<Integer> members =  new ArrayList<Integer>();
    	ChordMaster chord = ChordMaster.getChordMaster();
    	Set<String> ids =  chord._get_keySet();
    	
    	for(String id: ids)
    	{
    		if(chord._get(id).avdNum.equals(avdnum)) continue;
    		members.add(Integer.valueOf(chord._get(id).portNum));
    	}
    	
    	
    	
    	Log.e("sender_shopper","Members i am sending to are:::"+members.size());
    	
    	
    	
    	new Join(members,new Message(MessageType.joinMessage,null,avdnum,null)).start();
    }
}

