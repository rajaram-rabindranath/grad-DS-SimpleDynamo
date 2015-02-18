package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;

import android.util.Log;


/*=========================================================================
 * Class name   : node
 * Description  : A template for a node object
 * Author		: RAJARAM RABINDRANATH
 *=========================================================================*/
public class ChordMaster implements Serializable
{
	private static final long serialVersionUID = 1L;
	private TreeMap<String, node> simpleChord = null;
	static String TAG = ChordMaster.class.getName();	
	
	node I = null, mypred = null,mysucc=null;
	
	boolean isfirstNode = false;
	int readerQuorum_size = 2;
	int writerQuorum_size = 2;
	private static int singleton = 0;
	private static ChordMaster chord = null;
	
	/*=========================================================================
     * Function   : getQueryNodes(args)
     * Description: 
     * Parameters : String hashedKey
     * Return	  : node[3]
     *=========================================================================*/
	public static ChordMaster makeChord(node self)
	{
		if(singleton == 0)
		{
			chord = new ChordMaster(self);
		}
		return chord;
	}
	
	/*=========================================================================
     * Function   : getQueryNodes(args)
     * Description: 
     * Parameters : String hashedKey
     * Return	  : node[3]
     *=========================================================================*/
	public static ChordMaster getChordMaster()
	{
		return chord;
	}
	
	/*=========================================================================
     * Function   : succNode()
     * Description: 
     * Parameters : String hashedKey
     * Return	  : node[3]
     *=========================================================================*/
	public node succNode(int toPort)
	{
		String avdnum = Integer.toString(toPort/2);
		try
		{
			String hashedKey = genHash(avdnum);
			Iterator<String> Iter = simpleChord.keySet().iterator();
			
			while(Iter.hasNext())
			{
				// found designate node
				if(Iter.next().equals(hashedKey))
				{
					// last item ?
					if(!Iter.hasNext()) return simpleChord.firstEntry().getValue();
					// no!
					return (simpleChord.get(Iter.next()));
				}
			}
		}
		catch(NoSuchAlgorithmException nsa)
		{
			nsa.printStackTrace();
		}
		return null;
	}
	
	
	/*=========================================================================
     * Function   : predNode()
     * Description: 
     * Parameters : String hashedKey
     * Return	  : node[3]
     *=========================================================================*/
	public node predNode(int toPort)
	{
		String avdnum = Integer.toString(toPort/2);
		try
		{
			String hashedKey = genHash(avdnum);
			Iterator<String> Iter = simpleChord.keySet().iterator();
			String prevKey = Iter.next(), currKey = null;
			
			if(prevKey.equals(hashedKey)) return(simpleChord.lastEntry().getValue());
			
			// designate was not first node 
			while(Iter.hasNext())
			{
				currKey = Iter.next();
				if(currKey.equals(hashedKey))
				{
					return(simpleChord.get(prevKey));
				}
				prevKey = currKey;
			}
		}
		catch(NoSuchAlgorithmException nsa)
		{
			nsa.printStackTrace();
		}
		return null;
	}
	
	public String getPredNode_hash(String Node_port)
	{
		node pred = predNode(Integer.valueOf(Node_port));
		return pred.node_id;
	}
	
	public int getPredNode_port(String Node_port)
	{
		node pred = predNode(Integer.valueOf(Node_port));
		return Integer.valueOf(pred.portNum);
	}

	public String getSuccNode_hash()
	{
		return null;
	}
	
	public int getSuccNode_port()
	{
		return 0;
	}
	
	
	
	/*=========================================================================
     * Function   : getQueryNodes(args)
     * Description: 
     * Parameters : String hashedKey
     * Return	  : node[3]
     *=========================================================================*/
	private ChordMaster(node self)
	{
		this.I = self;
		simpleChord = new TreeMap<String,node>();
		Integer seed = 5554;
		
		// construct chord of 5 members
		try
		{
			simpleChord.put(genHash(I.avdNum),I);
			for(int i=0;i<5;i++)
			{
				if(I.avdNum.equals(seed.toString())) 
				{
					seed+=2;
					continue;
				}
				node n = new node(seed.toString());
				simpleChord.put(genHash(n.avdNum),n);
				seed +=2;
			}
		}
		catch(NoSuchAlgorithmException nsa)
		{
			nsa.printStackTrace();
		}
		node[] adj= get_adjNodes(I);
		mypred = adj[0];
		mysucc = adj[1];
		
		isfirstNode = isFirstNode();
		printChord(); // debug purposes

		Log.e("window_shopper","my predecessor::"+mypred.avdNum);
		Log.e("window_shopper","my successor  ::"+mysucc.avdNum);
	}
	
	
	/*=========================================================================
     * Function   : getQueryNodes(args)
     * Description: 
     * Parameters : String hashedKey
     * Return	  : node[3]
     *=========================================================================*/
	/*public void _setNodeStatus(String avdNum,nodeStatus status)
	{
		try
		{
			node n = simpleChord.get(genHash(avdNum));
			synchronized (n) 
			{
				n.status = status;
			}
		}
		catch(NoSuchAlgorithmException nsa)
		{
			nsa.printStackTrace();
		}
		
		return;
	}
	
	
	=========================================================================
     * Function   : getQueryNodes(args)
     * Description: 
     * Parameters : String hashedKey
     * Return	  : node[3]
     *=========================================================================
	public nodeStatus _getNodeStatus(String avdNum)
	{
		node n = null;
		try
		{
			n = simpleChord.get(genHash(avdNum));
		}
		catch(NoSuchAlgorithmException nsa)
		{
			nsa.printStackTrace();
		}
		synchronized (n) 
		{
			return n.status; 
		}
	}*/
	
	/*=========================================================================
     * Function   : getQueryNodes(args)
     * Description: 
     * Parameters : String hashedKey
     * Return	  : node[3]
     *=========================================================================*/
	public node[] getQueryNodes(String hashedKey)
	{
		Iterator<String> iter = simpleChord.keySet().iterator();
		node[] queryNodes = new node[3];
		queryNodes[0] = getOwner(hashedKey);
		
		while(iter.hasNext())
		{
			// found the onwer in the dist system
			if(iter.next().equals(queryNodes[0].node_id))
			{
				// special case last node
				if(!iter.hasNext())
				{
					queryNodes[1]=simpleChord.firstEntry().getValue();
					queryNodes[2]=simpleChord.higherEntry(simpleChord.firstKey()).getValue();
					break;
				}
				
				queryNodes[1] = simpleChord.get(iter.next()); 
				// sepcial case last but one node
				if(!iter.hasNext())
				{
					queryNodes[2] = simpleChord.firstEntry().getValue();
					break;
				}
				queryNodes[2] = simpleChord.get(iter.next());
				break;
			}
		}
		return queryNodes;
	}
	
	/*=========================================================================
     * Function   : isFirstNode()
     * Description: verifies if it is the first node
     * Parameters : void
     * Return	  : boolean 
     *=========================================================================*/
    private node[] get_myBrothers()
	{
		Iterator<String> iter =  simpleChord.keySet().iterator();
		node[] preds = new node[2];
		preds[1] = mypred;
		String prevNode_id = iter.next(),currNode_id = null;
		
		if(prevNode_id.equals(mypred.node_id))
		{
			preds[0] = simpleChord.lastEntry().getValue();
			return preds;
		}
		
		while(iter.hasNext())
		{
			currNode_id = iter.next();
			if(currNode_id.equals(mypred.node_id))
			{
				preds[0] = simpleChord.get(prevNode_id);
			}
			prevNode_id =  currNode_id;
		}
		return preds;
	}
	
	
	/*=========================================================================
     * Function   : isFirstNode()
     * Description: verifies if it is the first node
     * Parameters : void
     * Return	  : boolean 
     *=========================================================================*/
    private boolean isFirstNode()
    {
    	// if my pred's node_id is > than mine then --- i am first node
    	if(I.node_id.compareTo(mypred.node_id) < 0)
    	{
    		return true;
    	}
    	return false;
    }
	
    /*=========================================================================
     * Function   : is_inMyDomain()
     * Description: checks if the hashedKey received as input belongs
     * 				this node partition
     * Parameters : String hashedKey (Hashed using SHA-1)
     * Return	  : boolean 
     *=========================================================================*/
    public String[] get_nonDomain(Set<String> keys)
    {
    	node[] brothers = get_myBrothers();
    	ArrayList<String> _delete= new ArrayList<String>();
    	
    	Log.e("recovery","My brothers are ::"+brothers[0].avdNum);
    	Log.e("recovery","My brothers are ::"+brothers[1].avdNum);
    	
    	node owner = null;
    	try
    	{
	    	for(String key : keys)
	    	{	    		
	    		owner = getOwner(genHash(key));
	    		
	    		// none of us are owners -- brothers and me ?
	    		if(!(owner.avdNum.equals(brothers[0].avdNum)||owner.avdNum.equals(I.avdNum)||
	    				owner.avdNum.equals(brothers[1].avdNum)))
	    			_delete.add(key); // then key needs to be deleted
	    	}
    	}
    	catch(NoSuchAlgorithmException nsa)
    	{
    		nsa.printStackTrace();
    	}
    	
    	String [] del = null;
    	if(_delete.size()!=0)
    	{
    		del = new String[_delete.size()];
    		for(int i=0;i<_delete.size();i++)
    		{
    			del[i]=_delete.get(i);
    		}
    		return del;
    	}
    	return del;
    }
    
    
    
    /*=========================================================================
     * Function   : is_inMyDomain()
     * Description: checks if the hashedKey received as input belongs
     * 				this node partition
     * Parameters : String hashedKey (Hashed using SHA-1)
     * Return	  : boolean 
     *=========================================================================*/
    public boolean isOwner(String key,String avdNum)
    {
    	
		String hashedKey = null; 
    	try
    	{
    		hashedKey = genHash(key);
    	}
    	catch(NoSuchAlgorithmException nsa)
    	{
    		nsa.printStackTrace();
    	}
    	
    	return(avdNum.equals(getOwner(hashedKey).avdNum));
    }
    
    public String whoOwner(String key)
    {
    	String owner = null;
    	try
    	{
    		String hashedKey = genHash(key);
    		
    		owner = getOwner(hashedKey).avdNum;
    	}
    	catch(NoSuchAlgorithmException nsa)
    	{
    		nsa.printStackTrace();
    	}
    	return owner;
    }
    /*=========================================================================
     * Function   : is_inMyDomain()
     * Description: checks if the hashedKey received as input belongs
     * 				this node partition
     * Parameters : String hashedKey (Hashed using SHA-1)
     * Return	  : boolean 
     *=========================================================================*/
    public node getOwner(String hashedKey)
    {
    	Iterator<String> iter =simpleChord.keySet().iterator();
    	String currNode_id = null;
    	String prevNode_id = iter.next();
    	
    	/**
    	 * to whom does this key belong
    	 */
    	while(iter.hasNext())
    	{
    		currNode_id =  iter.next();
    		// if the hashedkey falls between curr and prev
    		if((hashedKey.compareTo(prevNode_id)>0) && (hashedKey.compareTo(currNode_id)<=0))
        		return simpleChord.get(currNode_id);
    		
    		prevNode_id = currNode_id;
       	}
    	/**
    	 * exiting the loop without any result means
    	 * could be first node	
    	 */
		/* object fall between 0 and pred*/
		if(hashedKey.compareTo(simpleChord.lastKey()) >0)
			return simpleChord.firstEntry().getValue();
		/* object falls between 0 and me */
		else if(hashedKey.compareTo(simpleChord.firstKey()) <=0)
			return simpleChord.firstEntry().getValue();
    	return null;
    }
	
	
    /*=========================================================================
     * Function   : _get_keySet
     * Description: returns the keySet of the component simpleChord
     * Parameters : 
     * Return	  : Set<String> 
     *=========================================================================*/
	public Set<String> _get_keySet()
	{
		return simpleChord.keySet();
	}
	
	public node _get_firstNode()
	{
		return simpleChord.get(simpleChord.firstKey());
	}
	
	/*=========================================================================
     * Function   : genHash
     * Description: given a node_id <String> returns a node object
     * Parameters : String node_id
     * Return	  : node 
     *=========================================================================*/
	public node _get(String node_id)
	{
		return simpleChord.get(node_id);
	}

	/*=========================================================================
     * Function   : genHash
     * Description: 
     * Parameters : Uri uri, String[] projection, String selection, 
     * 				String[] selectionArgs,String sortOrder
     * Return	  : Cursor 
     *=========================================================================*/
	public void insert(String node_id,node n)
	{
		simpleChord.put(node_id,n);
	}
	
	/*=========================================================================
     * Function   : genHash
     * Description: Given a string input generates the hashkey for the input
     * 				using SHA-1
     * Parameters : String input
     * Return	  : String has value of "input"
     *=========================================================================*/
    public static String genHash(String input) throws NoSuchAlgorithmException 
    {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        int index = 0;
        for (byte b : sha1Hash)
        {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
    
	
    /*=========================================================================
     * Function   : printChord
     * Description: Lists all the members of the chord
     * Parameters : void
     * Return	  : void 
     *=========================================================================*/
    public void printChord()
    {
    	Set<String> ids = simpleChord.keySet();
    	Log.d("window_shopper","num of elements ="+simpleChord.size());
    	for(String node_id:ids)
    	{
    		Log.e("window_shopper","chord avds:: "+node_id+"::"+simpleChord.get(node_id).avdNum);
    	}
    }
    
    /*=========================================================================
     * Function   : adjNodes
     * Description: given a node_id fetches the predecessor and successor nodes
     * 				of the said node
     * Parameters : node n
     * Return	  : node[] 
     *=========================================================================*/
    public node[] get_adjNodes(node n)
    {
    	int pred=0, succ=1; // index of pred and succ in the node[] that shall be sent across to other nodes
    	int i = 1;
    	boolean found = false;
    	node[] adjNodes = {null,null};
    	Iterator<String> iter =simpleChord.keySet().iterator();
    	
    	String currNode_id = null;
    	
    	if(simpleChord.size() == 1)
	    {
    		adjNodes[pred] = adjNodes[succ] = simpleChord.get(iter.next());
    		return adjNodes;
	    }
	    	
    	// find pred and succ of node 
    	while(iter.hasNext())
    	{
    		currNode_id = iter.next();
    		
    		
    		if(n.node_id.equals(currNode_id))
    		{
    			found = true;
    			/**
    			 * we already know the predecessor
    			 * lets set the successor
    			 */
    			if(!iter.hasNext()) // if this is the last key
    			{
    				adjNodes[succ] = simpleChord.firstEntry().getValue();
    			}
    			else // go ahead set succ and handle special case
    			{
    				adjNodes[succ] = simpleChord.get(iter.next());
    				if(i == 1)adjNodes[pred]=(simpleChord.lastEntry()).getValue();
    			}
    			break;
    		}
    		adjNodes[pred] = simpleChord.get(currNode_id);
    		i++;
    	}
    	return adjNodes;
    }
}
