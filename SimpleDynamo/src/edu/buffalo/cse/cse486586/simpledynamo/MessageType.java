package edu.buffalo.cse.cse486586.simpledynamo;

/*=========================================================================
 * enum name   : MessageType
 * Description : a set of constants representing message types
 * Author	   : RAJARAM RABINDRANATH
 *=========================================================================*/
public enum MessageType
{
	joinMessage,
	peerListMessage,
	
	recoverKeysMessage,
	
	pingAck, // ping ack
	
	objectQueryMessage,
	objectQueryResponseMessage,
	
	GDumpQueryMessage,
	GDumpQueryResponseMessage,
	
	GDelReqMessage,
	GDelReqResponseMessage,
	
	objectDelReqMessage,
	objectDelReqResponseMessage,
	
	objectInsertMessage,
	objectInsertResponseMessage,
	
	objectReplicationMessage,
	objectReplicationRespMessage,
	
	objectDelReplicaMessage,
	objectDelReplicaRespMessage;
}
