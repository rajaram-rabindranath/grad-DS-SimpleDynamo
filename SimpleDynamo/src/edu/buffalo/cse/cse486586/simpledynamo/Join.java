package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import android.util.Log;

public class Join extends Thread 
{
	ArrayList<Integer> members =  new ArrayList<Integer>();
	Message join_msg =  null;
	
	public Join(ArrayList<Integer> members,Message join_msg)
	{
		this.members=members;
		this.join_msg = join_msg;
	}
	
	public void run()
	{
		
		BufferedOutputStream bos = null;
		ObjectOutputStream oos = null;
		Socket mySocket= null;
		String TAG = "window_shopper";
		/**
		 * Let all know that i am alive
		 */
		Log.e(TAG,"Will send join message to the following number of people::"+members.size());
		
		for(int i=0;i<members.size();i++)
		{
			try
			{
				
				mySocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),members.get(i));
				bos = new BufferedOutputStream(mySocket.getOutputStream());
				oos = new ObjectOutputStream(bos);
				oos.writeObject(join_msg);  
				bos.flush();
				oos.flush();
				mySocket.close();
				Log.e(TAG,"sending join message to :: "+members.get(i)/2);
			}
			catch(IOException ioex)
			{
				ioex.printStackTrace();
				Log.e(TAG,"IOException in join message");
			}
		}
	}
}
