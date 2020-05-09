/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
Transaction::Transaction(int transactionID, MessageType type, string key, string value, int timestamp) {
	this->id = transactionID;
	this->msgType = type;
	this->key = key;
	this->value = value;
	this->timestamp = timestamp;
	this->replyCount = 0;
	this->successCount = 0;
}

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
	map<int, Transaction*>::iterator it = transactionMap.begin();
	while(it != transactionMap.end()){
		delete it->second;
		++it;
	}
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();
	curMemList.emplace_back(Node(memberNode->addr));

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());


	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	int ringSize = ring.size();
	if(curMemList.size() != ringSize) {
		change = true;
	} else if(ringSize > 0){
		for(int i=0; i<ringSize; ++i) {
			if(curMemList[i].getHashCode() != ring[i].getHashCode()){
				change = true;
				break;
			}
		}
	}

	ring = curMemList;
	if(change)
		stabilizationProtocol();
}

/**
 * FUNCTION NAME: getMembershipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	Message msg = createMessage(MessageType::CREATE, key, value);
	std::vector<Node> replicas = findNodes(key);
	for(int i=0; i<replicas.size(); ++i){
		emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), msg.toString());
	}
	++g_transID;
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	Message msg = createMessage(MessageType::READ, key);
	std::vector<Node> replicas = findNodes(key);
	for(int i=0; i<replicas.size(); ++i){
		emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), msg.toString());
	}
	++g_transID;
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	Message msg = createMessage(MessageType::UPDATE, key, value);
	std::vector<Node> replicas = findNodes(key);
	for(int i=0; i<replicas.size(); ++i){
		emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), msg.toString());
	}
	++g_transID;
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	Message msg = createMessage(MessageType::DELETE, key);
	std::vector<Node> replicas = findNodes(key);
	for(int i=0; i<replicas.size(); ++i){
		emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), msg.toString());
	}
	++g_transID;
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica, int transactionID) {
	bool success = false;
	if(transactionID != STABLE) {
		success = ht->create(key, value);
		if(success)
			log->logCreateSuccess(&memberNode->addr, false, transactionID, key, value);
		else
			log->logCreateFail(&memberNode->addr, false, transactionID, key, value);
	} else {
		if(ht->read(key) == "")
			success = ht->create(key, value);
	}
	return success;
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key, int transactionID) {
	string value = ht->read(key);
	if(value != "")
		log->logReadSuccess(&memberNode->addr, false, transactionID, key, value);
	else
		log->logReadFail(&memberNode->addr, false, transactionID, key);
	return value;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica, int transactionID) {
	bool success = ht->update(key, value);
	if(success)
		log->logUpdateSuccess(&memberNode->addr, false, transactionID, key, value);
	else
		log->logUpdateFail(&memberNode->addr, false, transactionID, key, value);
	return success;
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key, int transactionID) {
	bool success = ht->deleteKey(key);
	if(transactionID != STABLE){
		if(success)
			log->logDeleteSuccess(&memberNode->addr, false, transactionID, key);
		else
			log->logDeleteFail(&memberNode->addr, false, transactionID, key);		
	}
	return success;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 * 				3) Ensure all READ and UPDATE operation get QUORUM replies.
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);
		Message msg(message);

		switch(msg.type) {
			case MessageType::CREATE: {
				bool success = createKeyValue(msg.key, msg.value, msg.replica, msg.transID);
				if(msg.transID != STABLE)
					sendReply(&msg.fromAddr, msg.transID, success, msg.type, msg.key);
				break;
			}
			case MessageType::READ: {
				string value = readKey(msg.key, msg.transID);
				bool success = !value.empty();
				sendReply(&msg.fromAddr, msg.transID, success, msg.type, msg.key, value);
				break;
			}
			case MessageType::UPDATE: {
				bool success = updateKeyValue(msg.key, msg.value, msg.replica, msg.transID);
				sendReply(&msg.fromAddr, msg.transID, success, msg.type, msg.key);
				break;
			}
			case MessageType::DELETE: {
				bool success = deletekey(msg.key, msg.transID);
				if(msg.transID != STABLE)
					sendReply(&msg.fromAddr, msg.transID, success, msg.type, msg.key);
				break;				
			}
			case MessageType::REPLY: {
				map<int, Transaction*>::iterator it = transactionMap.find(msg.transID);
				if(it != transactionMap.end()) { //Found
					Transaction* t = transactionMap[msg.transID];
					t->replyCount ++;
					if(msg.success)
						t->successCount ++;
				}
				break;					
			}
			case MessageType::READREPLY: {
				map<int, Transaction*>::iterator it = transactionMap.find(msg.transID);
				if(it != transactionMap.end()) { //Found
					Transaction* t = transactionMap[msg.transID];
					t->replyCount ++;
					t->value = msg.value;
					if(!msg.value.empty())
						t->successCount ++;
				}
				break;				
			}						
		}
		checkTransactionMap();
	}
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	map<string, string>::iterator it = ht->hashTable.begin();
	while(it != ht->hashTable.end()) {
		string key = it->first;
		string value = it->second;
		vector<Node> replicas = findNodes(key);
		Message message(STABLE, memberNode->addr, MessageType::CREATE, key, value);
		for(int i=0; i<replicas.size(); ++i){
			emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), message.toString());
		}
		++it;
	}
}

Message MP2Node::createMessage(MessageType type, string key, string value, bool success) {
	createTransaction(g_transID, type, key, value);
	if(type == CREATE || type == UPDATE){
		Message msg(g_transID, memberNode->addr, type, key, value);
		return msg;
	} else if(type == READ || type == DELETE){
		Message msg(g_transID, memberNode->addr, type, key);
		return msg;
	}
	return Message("");
}

void MP2Node::createTransaction(int transactionID, MessageType type, string key, string value) {
	Transaction* transaction = new Transaction(transactionID, type, key, value, par->getcurrtime());
	transactionMap.emplace(transactionID, transaction);
}

void MP2Node::checkTransactionMap() {
	map<int, Transaction*>::iterator it = transactionMap.begin();
	while(it != transactionMap.end()) {
		int transactionID = it->first;
		Transaction* transaction = it->second;
		int replyCount = transaction->replyCount;
		int successCount = transaction->successCount;

		if(replyCount == 3) {
			(successCount >= 2) ? logOperation(transaction, true, true, transactionID) : logOperation(transaction, true, false, transactionID);
			delete transaction;
			it = transactionMap.erase(it);
			continue;
		} else {
			if(successCount == 2) {
				logOperation(transaction, true, true, transactionID);
				transactionState.emplace(transactionID, true);
				delete transaction;
				it = transactionMap.erase(it);
				continue;
			} 
			if(replyCount - successCount == 2) {
				logOperation(transaction, true, false, transactionID);
				transactionState.emplace(transactionID, false);
				delete transaction;
				it = transactionMap.erase(it);
				continue;
			}
		}
		if(par->getcurrtime() - transaction->getTimestamp() > 10) {
			logOperation(transaction, true, false, transactionID);
			transactionState.emplace(transactionID, false);
			delete transaction;
			it = transactionMap.erase(it);
			continue;
		}
		++it;
	}
}

void MP2Node::sendReply(Address* fromAddr, int transactionID, bool success, MessageType type, string key, string content) {
	if(type == READ) {
		Message message(transactionID, memberNode->addr, content);
		emulNet->ENsend(&memberNode->addr, fromAddr, message.toString());
	} else {
		Message message(transactionID, memberNode->addr, MessageType::REPLY, success);
		emulNet->ENsend(&memberNode->addr, fromAddr, message.toString());
	}
}

void MP2Node::logOperation(Transaction* transaction, bool isCoordinator, bool success, int transactionID) {
	string key = transaction->key;
	string value = transaction->value;

	switch(transaction->msgType) {
		case CREATE: { 
			if(success)
				log->logCreateSuccess(&memberNode->addr, isCoordinator, transactionID, key, value);
			else 
				log->logCreateFail(&memberNode->addr, isCoordinator, transactionID, key, value);
			break;
		}
		case READ: {
			if(success)
				log->logReadSuccess(&memberNode->addr, isCoordinator, transactionID, key, value);
			else 
				log->logReadFail(&memberNode->addr, isCoordinator, transactionID, key);
			break;
		}
		case UPDATE: {
			if(success)
				log->logUpdateSuccess(&memberNode->addr, isCoordinator, transactionID, key, value);
			else 
				log->logUpdateFail(&memberNode->addr, isCoordinator, transactionID, key, value);
			break;
		}
		case DELETE: {
			if(success)
				log->logDeleteSuccess(&memberNode->addr, isCoordinator, transactionID, key);
			else 
				log->logDeleteFail(&memberNode->addr, isCoordinator, transactionID, key);
			break;
		}
	}
}
