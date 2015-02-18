package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import android.content.ContentResolver;
import android.os.AsyncTask;
import android.util.Log;


/*=========================================================================
 * Class name   : SeverOps
 * Description  : Opens a socket and listens to incoming requests
 * Author		: RAJARAM RABINDRANATH
 *========================================================================*/
public class ServerOps extends Thread 
{
	static String TAG = ServerOps.class.getName();
	ServerSocket serverSocket = null;
	SimpleDynamoProvider dynamoDBProvider= null;
	
	public ServerOps(serverOps_params params)
	{
		serverSocket = params.serverSocket;
		dynamoDBProvider = params.dynamoDBProvider;
	}
	
	public void run() 
	{
		Message in_msg = null;
		Socket clientConnx = null;
		while(true) // for each message the client shall make a new connection therefore while(true)
		{
			try
	    	{
	    		clientConnx = serverSocket.accept();
		    	ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(clientConnx.getInputStream()));  
				in_msg = (Message)ois.readObject();  
				//ois.close(); //had this commented out previously
		    	//clientConnx.close();
				
				new MessageProcessor(in_msg,dynamoDBProvider,clientConnx).start();
		    }
	    	catch(ClassNotFoundException ex)
	    	{
	    		ex.printStackTrace();
	    		Log.e("window_shopper","received object is not of type message");
	    	}
			catch(IOException ex)
			{
				Log.e(TAG,"Problems with server socket");
				ex.printStackTrace();
			}
	    }
	}
}

/*=========================================================================
 * Class name   : SeverOps_params
 * Description  : An aux class that shall facilitate the creation of params
 * 					that are later passed to the ServerOps class
 * Author		: RAJARAM RABINDRANATH
 *========================================================================*/
class serverOps_params
{
	ServerSocket serverSocket = null;
	SimpleDynamoProvider dynamoDBProvider = null;
	
	public serverOps_params(ServerSocket serverSocket, SimpleDynamoProvider dynamoDBProvider)
	{
		this.serverSocket =serverSocket;
		this.dynamoDBProvider = dynamoDBProvider;
	}
}