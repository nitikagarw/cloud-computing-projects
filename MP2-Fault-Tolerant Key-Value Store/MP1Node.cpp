/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        // create JOINREQ message: format of data is {struct Address myaddr}
        msg = new MessageHdr();
        msg->msgType = JOINREQ;
        msg->memberList = memberNode->memberList;
        msg->addr = &memberNode->addr;

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, sizeof(MessageHdr));

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
    MessageHdr* msg = (MessageHdr*) data;
    if(msg->msgType == JOINREQ) {
        AddToMemberList(msg);
        sendMessage(msg->addr, JOINREP);
    } else if(msg->msgType == JOINREP) {
        AddToMemberList(msg);
        memberNode->inGroup = true;
    } else if(msg->msgType == PING) {
        pingHandler(msg);
    }
    delete msg;
    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    ++(memberNode->heartbeat);

    for(int i = memberNode->memberList.size()-1; i >= 0; --i){
        int id = memberNode->memberList[i].id;
        short port = memberNode->memberList[i].port;
        long timestamp = memberNode->memberList[i].timestamp;

        if(this->par->getcurrtime() - timestamp >= TREMOVE) {
            Address* addressToRemove = getAddress(id, port);
            log->logNodeRemove(&memberNode->addr, addressToRemove);
            memberNode->memberList.erase(memberNode->memberList.begin()+i);
            delete addressToRemove;
        }
    }

    int num_members = memberNode->memberList.size();
    for (int i = 0; i < num_members; ++i) {
        int id = memberNode->memberList[i].id;
        short port = memberNode->memberList[i].port;
        Address* address = getAddress(id, port);
        sendMessage(address, PING);
        delete address;
    }

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}

/**
 * FUNCTION NAME: AddToMemberList  
 * 
 * DESCRIPTION: If a node does not exist in the memberList, it will be pushed to the memberList.
 */
void MP1Node::AddToMemberList(MessageHdr* msg) {
    cout << "AddToMemberList: msg" << endl;
    int id = 0;
    short port;
    memcpy(&id, &msg->addr->addr[0], sizeof(int));
    memcpy(&port, &msg->addr->addr[4], sizeof(short));

    if(checkMemberList(id, port) != nullptr)
        return;

    MemberListEntry memberListEntry(id, port, 1, this->par->getcurrtime());
    memberNode->memberList.push_back(memberListEntry);
    log->logNodeAdd(&memberNode->addr, msg->addr);
}

/**
 * FUNCTION NAME: AddToMemberList
 *
 * DESCRIPTION: If a node does not exist in the memberList, it will be pushed to the memberList.
 */
void MP1Node::AddToMemberList(MemberListEntry* memberListEntry) {
    cout << "AddToMemberList: MemberListEntry" << endl;
    Address* addr = getAddress(memberListEntry->id, memberListEntry->port);
    if(*addr == memberNode->addr) {
        delete addr;
        return;
    }

    if(this->par->getcurrtime() - memberListEntry->timestamp < TREMOVE) {
        log->logNodeAdd(&memberNode->addr, addr);
        memberNode->memberList.push_back(*memberListEntry);
    }
}

/**
 * FUNCTION NAME: checkMemberList
 *
 * DESCRIPTION: If the node exists in the memberList, the function will return true. Otherwise, the function will return false.
 */
MemberListEntry* MP1Node::checkMemberList(int id, short port) {
    int num_members = memberNode->memberList.size();
    for(int i=0; i<num_members; ++i){
        if(memberNode->memberList[i].id == id && memberNode->memberList[i].port == port){
            return &memberNode->memberList[i];
        }
    }
    return nullptr;
}

/**
 * FUNCTION NAME: sendMessage
 *
 * DESCRIPTION: sends message using EmulNet
 */
void MP1Node::sendMessage(Address* toAddress, MsgTypes msgType) {
    MessageHdr* msg = new MessageHdr();
    msg->msgType = msgType;
    msg->memberList = memberNode->memberList;
    msg->addr = &memberNode->addr;
    emulNet->ENsend(msg->addr, toAddress, (char*)msg, sizeof(MessageHdr));
}

/**
 * FUNCTION NAME: pingHandler
 *
 * DESCRIPTION: The function processing the PING messages.
 */
void MP1Node::pingHandler(MessageHdr* msg) {
    //Update source member
    int srcid = 0;
    short srcport;
    memcpy(&srcid, &msg->addr->addr[0], sizeof(int));
    memcpy(&srcport, &msg->addr->addr[4], sizeof(short));
    MemberListEntry* sourceMember = checkMemberList(srcid, srcport);
    if(sourceMember != nullptr) {
        ++(sourceMember->heartbeat);
        sourceMember->timestamp = this->par->getcurrtime();
    } else {
        AddToMemberList(msg);
    }

    int num_members = msg->memberList.size();
    for(int i=0; i<num_members; ++i) {
        int id = msg->memberList[i].id;
        short port = msg->memberList[i].port;
        long heartbeat = msg->memberList[i].heartbeat;

        MemberListEntry* memberListEntry = checkMemberList(id, port);
        if(memberListEntry == nullptr){
            AddToMemberList(&msg->memberList[i]);
        } else {
            if(heartbeat > memberListEntry->heartbeat) {
                memberListEntry->heartbeat = heartbeat;
                memberListEntry->timestamp = this->par->getcurrtime();
            }
        }
    }
}

/**
 * FUNCTION NAME: getAddress
 *
 * DESCRIPTION: return address given the id and port
 */
Address* MP1Node::getAddress(int id, short port) {
    Address* address = new Address();
    memcpy(&address->addr[0], &id, sizeof(int));
    memcpy(&address->addr[4], &port, sizeof(short));
    return address;
}



