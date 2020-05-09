/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"
#define STABLE -1

/**
 * CLASS NAME: Transaction
 *
 * DESCRIPTION: This class encapsulates all transaction details:
 * 				1) ID
 * 				2) Timestamp
 * 				3) Reply and Success count
 */
class Transaction {
private:
	int id;
	int timestamp;

public:
	string key;
	string value;
	MessageType msgType;
	int replyCount;
	int successCount;
	Transaction(int transactionID, MessageType type, string key, string value, int timestamp);
	int getTimestamp() {
		return timestamp;
	}

};

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */
class MP2Node {
private:
	// Vector holding the next two neighbors in the ring who have my replicas
	vector<Node> hasMyReplicas;
	// Vector holding the previous two neighbors in the ring whose replicas I have
	vector<Node> haveReplicasOf;
	// Ring
	vector<Node> ring;
	// Hash Table
	HashTable * ht;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet * emulNet;
	// Object of Log
	Log * log;
	//Map of transactions
	map<int, Transaction*> transactionMap;
	//Map of transaction states
	map<int, bool> transactionState;

public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	Member * getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);
	void findNeighbors();

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	// receive messages from Emulnet
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);

	// handle messages from receiving queue
	void checkMessages();

	// coordinator dispatches messages to corresponding nodes
	void dispatchMessages(Message message);

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
	bool createKeyValue(string key, string value, ReplicaType replica, int transactionID);
	string readKey(string key, int transactionID);
	bool updateKeyValue(string key, string value, ReplicaType replica, int transactionID);
	bool deletekey(string key, int transactionID);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol();

	Message createMessage(MessageType type, string key, string value = "", bool success = false);
	void createTransaction(int transactionID, MessageType type, string key, string value);
	void checkTransactionMap();
	void sendReply(Address* fromAddr, int transactionID, bool success, MessageType type, string key, string content = "");
	void logOperation(Transaction* transaction, bool isCoordinator, bool success, int transactionID);

	~MP2Node();
};

#endif /* MP2NODE_H_ */