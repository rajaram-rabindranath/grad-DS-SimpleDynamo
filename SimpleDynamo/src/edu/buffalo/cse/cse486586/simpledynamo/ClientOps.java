package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Hashtable;

import android.util.Log;


/*=========================================================================
 * Class name   : ClientOps
 * Description  : A thread that is used to send messages to successor
 * Author		: RAJARAM RABINDRANATH
 *=========================================================================*/
public class ClientOps extends Thread
{
	sendMsgParams msgParams = null;
	static final String TAG = ClientOps.class.getName();
	Socket mySocket = null;
	
	//ClientOps(sendMsgParams msgParams,Socket eli)
	ClientOps(sendMsgParams msgParams)
	{
		this.msgParams = msgParams;
		//this.mySocket  = eli;  
	}
	
	
	public void run()
	{
		Message out_msg =  msgParams.msg;
		out_msg.toPort = Integer.valueOf(msgParams.receiverPortNum); 
		BufferedOutputStream bos = null;
		ObjectOutputStream oos = null;
		ChordMaster chord = ChordMaster.getChordMaster();
		
		while(true)
		{
			try
	        {
				
					//mySocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(msgParams.receiverPortNum));
			//	if(mySocket == null)
				{
					mySocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),out_msg.toPort);
				
				}
				bos = new BufferedOutputStream(mySocket.getOutputStream());
				oos = new ObjectOutputStream(bos);
				oos.writeObject(out_msg);  
				
				bos.flush();
				oos.flush();
			
				if(!isReceiverDead())// receiver is alive
				{
					oos.close();
					bos.close();
					mySocket.close();
					Log.e("window_shopper","Message sent::"+out_msg.msgType+"::"+(out_msg.toPort/2));
					break;
				}
				else
				{
					
					Log.e("window_shopper","\nReceiver::"+out_msg.toPort/2+"has failed ::"+out_msg.msgType);
					
					/**
					 *  receiver has failed need to take action
					 *  Contingent on what I wanted to do
					 *   1. if request message whom to send
					 *   2. if response message what next -- roll back
					 */
					out_msg.toPort =_failureHandling(out_msg,chord);
					if(out_msg.toPort == 0) 
					{
						Log.e("window_shopper","A different scenario");
						break; // a different scenario
					}
				}
			
				oos.close();
				bos.close();
				mySocket.close();
				
	        }
		    catch (UnknownHostException e) 
	        {
	        	e.printStackTrace();
	            Log.e("window_shopper", "ClientTask UnknownHostException");
	        } 
	    	catch(StreamCorruptedException ex)
	    	{
	    		ex.printStackTrace();
	    		Log.e("window_shopper"," stream corrupted bull crap"+ex.getLocalizedMessage());
	    	}
			catch(ClassNotFoundException cnf)
			{
				cnf.printStackTrace();
				Log.e(TAG,"ack problems");
			}
			catch(SocketException sockex)
			{
				sockex.printStackTrace();
				Log.e("window_shopper","Socket problems");
				// new port to send to
				out_msg.toPort = _failureHandling(out_msg, chord);
			}
	        catch (IOException e) 
	        {
	        	e.printStackTrace();
	            Log.e("window_shopper", "ClientTask socket IOException");
	        }
		}// end while	
	}

	
	
	
	/*=========================================================================
	 * Function Name: _failureHandling
	 * Description  : Handles failures detected -- 
	 * Authors		: RAJARAM RABINDRANATH
	 *=========================================================================*/
	private int _failureHandling(Message msg,ChordMaster chord)
	{
		// set node status as down so that we don't send queries to it
		//chord._setNodeStatus(msg.msgOrigin,nodeStatus.down);
		int to = 0;
		/**
		 * ================ What TYPE of MESSAGE failed ? =======================
		 * 1. Request message -- persist / be Obstinate
		 * 2. Response message -- do a quick roll back
		 * 		> fork a MessageProcessor thread to handle roll back
		 * 	 		> _deleteResp X _insertReq (to self)
		 *  		> _insertResp X _deleteReq (-"-) -- there is a special case --
		 *  		> _replicationResp X _deleteReq (-"-)
		 *================ What TYPE of MESSAGE failed ? =======================*/
		switch(msg.msgType)
		{
			case joinMessage:
				break;
			case peerListMessage:
				break;
			case pingAck:
				break;
			
			case objectInsertMessage:
				// the next one is responsible
				to = Integer.valueOf(chord.succNode(msg.toPort).portNum);
				Log.e("window_shopper","Will send "+msg.msgType+" to node::"+to/2);
				break;
				
			case GDelReqMessage:
				to = Integer.valueOf(chord.succNode(msg.toPort).portNum);
				Log.e("window_shopper","Will send "+msg.msgType+" to node::"+to/2);
				break;
				
			case GDumpQueryMessage:
				Log.e("window_shopper","The port number"+msg.toPort);
				node a = chord.succNode(msg.toPort);
				to = Integer.valueOf(a.portNum);
				Log.e("window_shopper","The heck this is happening::"+a.avdNum);
				Log.e("window_shopper","The heck this is happening::"+a.portNum);
				Log.e("window_shopper","Will send it to node::"+to/2);
				break;
				
			case objectReplicationMessage:
				to = Integer.valueOf(chord.succNode(msg.toPort).portNum);
				Log.e("window_shopper","Will send "+msg.msgType+" to node::"+to/2);
				break;
				
			case objectQueryMessage:
				to = Integer.valueOf(chord.predNode(msg.toPort).portNum);
				Log.e("window_shopper","Will send "+msg.msgType+" to node::"+to/2);
				break;
				
			
			case objectDelReplicaMessage:
				// want to decrement TTL
				to = Integer.valueOf(chord.succNode(msg.toPort).portNum);
				Log.e("window_shopper","Will send "+msg.msgType+" to node::"+to/2);
				break;
			
				
			case objectDelReqMessage:
				// this fails we need to 
				to = Integer.valueOf(chord.succNode(msg.toPort).portNum);
				Log.e("window_shopper","Will send "+msg.msgType+" to node::"+to/2);
				break;
					
				
			case objectQueryResponseMessage:
				break;
			case GDumpQueryResponseMessage:
				break;
			case GDelReqResponseMessage:
				break;
			case objectDelReqResponseMessage:
				break;
			case objectInsertResponseMessage:
				break;
			case objectReplicationRespMessage:
				break;
			case objectDelReplicaRespMessage:
				break;
			default:
				Log.e("window_shopper","LIFE IS VERY CRUEL INDEED");
				break;
		}
		
		Log.v("window_shopper","Will send "+msg.msgType+" to node::"+to/2);
		return to;
	}
	
	/*=========================================================================
	 * Function Name: waitForAck
	 * Description  : 
	 * Authors		: RAJARAM RABINDRANATH
	 *=========================================================================*/
	private boolean gotAck() throws ClassNotFoundException, SocketException
	{
		ObjectInputStream ois = null;
		Message in_msg = null;
		try
		{
			//mySocket.
	    	ois = new ObjectInputStream(new BufferedInputStream(mySocket.getInputStream()));
	    	//Log.e("window_shopper","done 1");
	    	mySocket.setSoTimeout(100);
	    	//Log.e("window_shopper","done 2");
	    	in_msg = (Message)ois.readObject();
	    	//Log.e("window_shopper","done 3");
	    }
		catch(EOFException eof)
		{
			eof.printStackTrace();
			Log.e("window_shopper","\n\n EOF HAS BEEN DOWN \n\n");
			return false;
		}
		catch(SocketTimeoutException timeoutex)
		{
			timeoutex.printStackTrace();
			Log.e("window_shopper","\n\n TIMEOUT HAS GONE DOWN AFTER RECEIVING MESSAGE \n\n");
			return false;	
		}
		catch(IOException ex)
		{
			ex.printStackTrace();
			Log.e("window_shopper","socket IOexpection trying to read and all that");
		}
		finally
		{
			try
			{
				if(ois!=null)ois.close();
			}
			catch(IOException iex)
			{
				iex.printStackTrace();
				Log.e("window_shopper","socket IOexpection when closing socket in gotAck()");
			}
		}
		if(in_msg.msgType == MessageType.pingAck)return true;
		else return false;
	}
	

	/*=========================================================================
	 * Function Name: isReceiverDead
	 * Description  : 
	 * Authors		: RAJARAM RABINDRANATH
	 *=========================================================================*/
	private boolean isReceiverDead() throws IOException,ClassNotFoundException,SocketException
	{
		boolean receiverDead = false;
		receiverDead = !(gotAck());
		return receiverDead;
	}
}


/*=========================================================================
 * Class name   : sendMsgParams
 * Description  : A aux class that helps build the parameters for the 
 * 					ClientOps class
 * Authors		: Rajaram Rabindranath
 *=========================================================================*/
class sendMsgParams
{
	Message msg =null;
	String receiverPortNum = null;
	Boolean sendSuccess = null;
	
	public sendMsgParams(Message msg,String receiverPortNum,Boolean sendSuccess)
	{
		this.msg = msg;
		this.receiverPortNum = receiverPortNum;
		this.sendSuccess = sendSuccess;
	}
}