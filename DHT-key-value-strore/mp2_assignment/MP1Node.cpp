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
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address)
{
    for (int i = 0; i < 6; i++)
    {
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
int MP1Node::recvLoop()
{
    if (memberNode->bFailed)
    {
        return false;
    }
    else
    {
        return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size)
{
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
void MP1Node::nodeStart(char *servaddrstr, short servport)
{
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if (initThisNode(&joinaddr) == -1)
    {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if (!introduceSelfToGroup(&joinaddr))
    {
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
int MP1Node::initThisNode(Address *joinaddr)
{
    /*
	 * This function is partially implemented and may require changes
	 */

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
int MP1Node::introduceSelfToGroup(Address *joinaddr)
{
    MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if (0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr)))
    {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;

        /* Add to own memberList */
        int id;
        short port;
        memcpy((char *)&id, (char *)&memberNode->addr.addr[0], sizeof(int));
        memcpy((char *)&port, (char *)&memberNode->addr.addr[4], sizeof(short));
        // MemberListEntry entry(id, port, memberNode->heartbeat, par->getcurrtime());
        MemberListEntry *firstNode = new MemberListEntry(id, port);
        memberNode->memberList.push_back(*firstNode);
    }
    else
    {
        msg = new MessageHdr();

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        msg->addr = Address(memberNode->addr);

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
int MP1Node::finishUpThisNode()
{
    /*
    * Your code goes here
    */
    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop()
{
    if (memberNode->bFailed)
    {
        return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if (!memberNode->inGroup)
    {
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
void MP1Node::checkMessages()
{
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while (!memberNode->mp1q.empty())
    {
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
bool MP1Node::recvCallBack(void *env, char *data, int size)
{
    MessageHdr *msg = (MessageHdr *)data;

    // printf("recvCallBack: msgtype: %d\n", (msg->msgType));
    if (msg->msgType == JOINREQ)
    {
        /* Adding to memberList */
        addNewNode(msg);

        /* Sending JOINREP */
        MessageHdr *repMsg = createMessage(JOINREP);
        emulNet->ENsend(&memberNode->addr, &msg->addr, (char *)repMsg, sizeof(MessageHdr));
        free(repMsg);
    }
    else if (msg->msgType == JOINREP)
    {
        memberNode->memberList = vector<MemberListEntry>(msg->memberList);
        memberNode->inGroup = true;

        for (uint i = 0; i < memberNode->memberList.size(); i++)
        {
            char addr[6];
            *(int *)&addr[0] = memberNode->memberList[i].id;
            *(short *)&addr[4] = memberNode->memberList[i].port;
            if (memcmp((char *)&addr, (char *)&memberNode->addr.addr, 6) != 0)
            {
                Address *a = new Address();
                memcpy((char *)&a->addr[0], (char *)&addr[0], sizeof(addr));
                log->logNodeAdd(&memberNode->addr, a);
            }
        }
    }
    else if (msg->msgType == GOSSIP){       
        gossipHandler(msg);
    }

    return 0;
}

void MP1Node::gossipHandler(MessageHdr *msg){
    //Update the heartbeat of the member from whom the message was received
    for(int k = 0; k < (int)memberNode->memberList.size(); k++){
        if(msg->addr == *getAddressFromId(memberNode->memberList[k].id, memberNode->memberList[k].port)){
            memberNode->memberList[k].heartbeat += 1;
            memberNode->memberList[k].timestamp = par->getcurrtime();
        }
    }

    for(int i = 0; i < (int)msg->memberList.size(); i++){
        bool entryExists = false;

        for(int j = 0; j < (int)memberNode->memberList.size(); j++){
            //Check if the node from msg->memberlist exists in the membernode->mamberlist
            if(msg->memberList[i].id == memberNode->memberList[j].id && msg->memberList[i].port == memberNode->memberList[j].port){
                entryExists = true;
                //Now update the above node's heartbeat
                if(msg->memberList[i].heartbeat > memberNode->memberList[j].heartbeat){
                    memberNode->memberList[j].heartbeat = msg->memberList[i].heartbeat;
                    memberNode->memberList[j].timestamp = par->getcurrtime();
                }
                break;
            }
        }

        //Add node to membership list if entryExists remains false
        if(!entryExists){
            MemberListEntry *newMember = new MemberListEntry(msg->memberList[i]);
            newMember->timestamp = par->getcurrtime();
            newMember->heartbeat = msg->memberList[i].heartbeat;
            memberNode->memberList.push_back(*newMember);
            Address *newAddr = getAddressFromId((int)newMember->id, (short)newMember->port);
            log->logNodeAdd(&memberNode->addr, newAddr);
            delete(newAddr);
        }
    }
}

void MP1Node::addNewNode(MessageHdr *msg){
    int id;
    short port;
    memcpy((char *)&id, (char *)&msg->addr.addr[0], sizeof(int));
    memcpy((char *)&port, (char *)&msg->addr.addr[4], sizeof(short));
    MemberListEntry *entry = new MemberListEntry(id, port, 0, par->getcurrtime());
    memberNode->memberList.push_back(*entry);
    log->logNodeAdd(&memberNode->addr, &msg->addr);
}

MessageHdr* MP1Node::createMessage(MsgTypes t){
    MessageHdr *newMsg = new MessageHdr();
    newMsg->msgType = t;

    if(t == JOINREP){
        newMsg->memberList = vector<MemberListEntry>(memberNode->memberList);
        newMsg->addr = Address(memberNode->addr);
    }
    else if(t == GOSSIP){
        //Create modified list based on TFAIL
        vector<MemberListEntry> newList;
        for(int j = 0; j < (int)memberNode->memberList.size(); j++){
            if(par->getcurrtime() - memberNode->memberList[j].timestamp < TFAIL){
                newList.push_back(memberNode->memberList[j]);
            }
        }
        newMsg->memberList = vector<MemberListEntry>(newList);
        newMsg->addr = Address(memberNode->addr);
    }
    
    return newMsg;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps()
{
    int id;
    short port;
    memcpy((char *)&id, (char *)&memberNode->addr.addr[0], sizeof(int));
    memcpy((char *)&port, (char *)&memberNode->addr.addr[4], sizeof(short));

    /* Update heartbeat value */
    memberNode->heartbeat++;
    for (int i = 0; i <(int)memberNode->memberList.size(); i++){
        /* Update heartbeat value in memberList */
        if (id == memberNode->memberList[i].id && port == memberNode->memberList[i].port){
            memberNode->memberList[i].heartbeat = memberNode->heartbeat;
            memberNode->memberList[i].timestamp = par->getcurrtime();
            break;
        }
    }

    /* Remove failed nodes after TREMOVE */
    removeFailedNode();

    /* Send gossip messages */
    sendGossips();

    return;
}

void MP1Node::removeFailedNode(){
    for(int i = 0; i < (int)memberNode->memberList.size(); i++){
        //Check if recorded timestamp is within the time limit TREMOVE
        if(par->getcurrtime() - memberNode->memberList[i].timestamp > TREMOVE){
            Address *removeAddr = getAddressFromId(memberNode->memberList[i].id, memberNode->memberList[i].port);
            log->logNodeRemove(&memberNode->addr, removeAddr);
            memberNode->memberList.erase(memberNode->memberList.begin() + i);
            delete removeAddr;
        }
    }
}

void MP1Node::sendGossips(){
    for(int i = 0; i < GOSSIP_FANOUT_VALUE; i++){
        //Randomize the nodes that get the gossip messages
        int n = rand() % memberNode->memberList.size();
        
        //Create gossip message and send it to the above 'n-th' node
        MessageHdr* gossipMsg = createMessage(GOSSIP);
        Address *sendAddr = getAddressFromId(memberNode->memberList[n].id, memberNode->memberList[n].port);
        emulNet->ENsend(&memberNode->addr, sendAddr, (char *)gossipMsg, sizeof(MessageHdr));
        delete sendAddr;
        free(gossipMsg); 
    }
}

/**
 * FUNCTION NAME: getAddressFromId
 *
 * DESCRIPTION: Convert id and port to an address
 */
Address *MP1Node::getAddressFromId(int id, short port)
{
    char addr[6];
    *(int *)&addr[0] = id;
    *(short *)&addr[4] = port;
    Address *a = new Address();
    memcpy((char *)&a->addr[0], (char *)&addr[0], sizeof(addr));
    return a;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr)
{
    return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress()
{
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
void MP1Node::initMemberListTable(Member *memberNode)
{
    memberNode->memberList.clear();
    int id;
    short port;
    memcpy((char *)&id, (char *)&memberNode->addr.addr[0], sizeof(int));
    memcpy((char *)&port, (char *)&memberNode->addr.addr[4], sizeof(short));
    // MemberListEntry entry(id, port, memberNode->heartbeat, par->getcurrtime());
    MemberListEntry entry(id, port);
    memberNode->memberList.push_back(entry);
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n", addr->addr[0], addr->addr[1], addr->addr[2],
           addr->addr[3], *(short *)&addr->addr[4]);
}