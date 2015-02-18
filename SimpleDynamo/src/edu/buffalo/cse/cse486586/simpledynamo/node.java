package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.security.NoSuchAlgorithmException;

import android.util.Log;

enum nodeStatus
{
	up,
	down;
}


/*=========================================================================
 * Class name   : node
 * Description  : A template for a node object
 * Author		: RAJARAM RABINDRANATH
 *=========================================================================*/
class node implements Serializable
{
	private static final long serialVersionUID = -1558119999239749824L;

	String node_id=null;
	String portNum=null;
	String avdNum=null;
	nodeStatus status;
	static String TAG= node.class.getName();
	
	public node(String avdNum)
	{
		this.portNum =  Integer.toString((Integer.valueOf(avdNum)*2));
		this.avdNum = avdNum;
		this.node_id =genNode_id(this.avdNum);
		this.status = nodeStatus.up;
	}
	
	/*=========================================================================
     * Function   : adjNodes
     * Description: given a node_id fetches the predecessor and successor nodes
     * 				of the said node
     * Parameters : node n
     * Return	  : node[] 
     *=========================================================================*/
    public node(String portNum,String avdNum)
	{
		this.portNum = portNum;
		this.avdNum = avdNum;
		this.node_id = genNode_id(this.avdNum);
	}
	
	/*=========================================================================
     * Function   : adjNodes
     * Description: given a node_id fetches the predecessor and successor nodes
     * 				of the said node
     * Parameters : node n
     * Return	  : node[] 
     *=========================================================================*/
    private static String genNode_id(String avdNum)
	{
		String node_id=null;
		try
		{
			node_id = ChordMaster.genHash(avdNum);
		}
		catch(NoSuchAlgorithmException ex)
		{
			ex.printStackTrace();
			Log.e(TAG,"Having problems assigning node_id -- no such algorithm as SHA-1");
		}
		return node_id; 
	}
}
