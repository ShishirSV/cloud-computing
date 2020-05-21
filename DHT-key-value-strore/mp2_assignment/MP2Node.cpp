
/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

// Expecting reply for trans_id
//trans_id, time, positive_replies, negative_replies, msg_type
vector<vector<int>> expectedReplies;
//key, value
vector<vector<string>> expectedRepliesStrings;

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *address)
{
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
MP2Node::~MP2Node()
{
	delete ht;
	delete memberNode;
}

struct less_than_key
{
	inline bool operator()(const Node &node1, const Node &node2)
	{
		return (node1.nodeHashCode < node2.nodeHashCode);
	}
};

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing()
{
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end(), less_than_key());

	/* Check if ring has changed */
	if (ring.size() != curMemList.size())
	{
		change = true;
		ring = vector<Node>(curMemList);
	}
	else
	{
		for (uint i = 0; i < ring.size(); i++)
		{
			if (ring[i].nodeHashCode != curMemList[i].nodeHashCode)
			{
				change = true;
				ring = vector<Node>(curMemList);
				break;
			}
		}
	}

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	if (change)
	{
		stabilizationProtocol();
	}
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList()
{
	unsigned int i;
	vector<Node> curMemList;
	for (i = 0; i < this->memberNode->memberList.size(); i++)
	{
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
size_t MP2Node::hashFunction(string key)
{
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret % RING_SIZE;
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
void MP2Node::clientCreate(string key, string value)
{
	/* Find replicas */
	vector<Node> replicas = findNodes(key);

	/* Construct and send message */
	g_transID++;
	for (uint i = 0; i < replicas.size(); i++)
	{
		Message message(g_transID, memberNode->addr, CREATE, key, value, i == 0 ? PRIMARY : i == 1 ? SECONDARY : TERTIARY); //TODO: pass replica type
		emulNet->ENsend(&memberNode->addr, &replicas[i].nodeAddress, message.toString());
	}

	expectedReplies.push_back(vector<int>{g_transID, par->globaltime, 0, 0, CREATE});
	expectedRepliesStrings.push_back(vector<string>{key, value});
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
void MP2Node::clientRead(string key)
{
	/* Find replicas */
	vector<Node> replicas = findNodes(key);

	/* Construct and send message */
	g_transID++;
	for (uint i = 0; i < replicas.size(); i++)
	{
		Message message(g_transID, memberNode->addr, READ, key); //TODO: pass replica type
		emulNet->ENsend(&memberNode->addr, &replicas[i].nodeAddress, message.toString());
	}

	expectedReplies.push_back(vector<int>{g_transID, par->globaltime, 0, 0, READ});
	expectedRepliesStrings.push_back(vector<string>{key});
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
void MP2Node::clientUpdate(string key, string value)
{
	/* Find replicas */
	vector<Node> replicas = findNodes(key);

	/* Construct and send message */
	g_transID++;
	for (uint i = 0; i < replicas.size(); i++)
	{
		Message message(g_transID, memberNode->addr, UPDATE, key, value); //TODO: pass replica type
		emulNet->ENsend(&memberNode->addr, &replicas[i].nodeAddress, message.toString());
	}

	expectedReplies.push_back(vector<int>{g_transID, par->globaltime, 0, 0, UPDATE});
	expectedRepliesStrings.push_back(vector<string>{key, value});
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
void MP2Node::clientDelete(string key)
{
	/* NOTE: Fails when ring has less than 3 members? */
	/* Find replicas */
	vector<Node> replicas = findNodes(key);

	/* Construct and send message */
	g_transID++;
	for (uint i = 0; i < replicas.size(); i++)
	{
		Message message(g_transID, memberNode->addr, DELETE, key); //TODO: pass replica type
		emulNet->ENsend(&memberNode->addr, &replicas[i].nodeAddress, message.toString());
	}

	expectedReplies.push_back(vector<int>{g_transID, par->globaltime, 0, 0, DELETE});
	expectedRepliesStrings.push_back(vector<string>{key});
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica)
{
	/* NOTE: The `create` function never returns false */
	bool ret = ht->create(key, value);
	return ret;
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key)
{
	string ret = ht->read(key);
	return ret;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica)
{
	/* NOTE: Haven't used replica here */
	bool ret = ht->update(key, value);
	return ret;
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key)
{
	bool ret = ht->deleteKey(key);
	return ret;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages()
{
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char *data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while (!memberNode->mp2q.empty())
	{
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		/*
		 * Handle the message types here
		 */
		Message msg(message);

		/* Find replicas */
		vector<Node> replicas = findNodes(msg.key);

		if (msg.type == CREATE)
		{
			bool ret = createKeyValue(msg.key, msg.value, msg.replica);
			if (ret)
			{
				log->logCreateSuccess(&memberNode->addr, false, msg.transID, msg.key, msg.value);
			}
			else
			{
				log->logCreateFail(&memberNode->addr, false, msg.transID, msg.key, msg.value);
			}
			Message sendMsg(msg.transID, memberNode->addr, REPLY, ret); //TODO: pass proper replica value
			emulNet->ENsend(&memberNode->addr, &msg.fromAddr, sendMsg.toString());
		}
		else if (msg.type == READ)
		{
			string ret = readKey(msg.key);
			if (ret.length() != 0)
			{
				log->logReadSuccess(&memberNode->addr, false, msg.transID, msg.key, ret);
			}
			else
			{
				log->logReadFail(&memberNode->addr, false, msg.transID, msg.key);
			}
			Message sendMsg(msg.transID, memberNode->addr, ret);
			emulNet->ENsend(&memberNode->addr, &msg.fromAddr, sendMsg.toString());
		}
		else if (msg.type == UPDATE)
		{
			bool ret = updateKeyValue(msg.key, msg.value, PRIMARY);
			if (ret)
			{
				log->logUpdateSuccess(&memberNode->addr, false, msg.transID, msg.key, msg.value);
			}
			else
			{
				log->logUpdateFail(&memberNode->addr, false, msg.transID, msg.key, msg.value);
			}
			Message sendMsg(msg.transID, memberNode->addr, REPLY, ret); //TODO: pass proper replica value
			emulNet->ENsend(&memberNode->addr, &msg.fromAddr, sendMsg.toString());
		}
		else if (msg.type == DELETE)
		{
			bool ret = deletekey(msg.key);
			if (ret)
			{
				log->logDeleteSuccess(&memberNode->addr, false, msg.transID, msg.key);
			}
			else
			{
				log->logDeleteFail(&memberNode->addr, false, msg.transID, msg.key);
			}
			Message sendMsg(msg.transID, memberNode->addr, REPLY, ret);
			emulNet->ENsend(&memberNode->addr, &msg.fromAddr, sendMsg.toString());
		}
		else if (msg.type == REPLY)
		{
			for (uint i = 0; i < expectedReplies.size(); i++)
			{
				if (msg.transID == expectedReplies[i][0])
				{
					if (msg.success == true)
					{
						expectedReplies[i][2]++;
					}
					else
					{
						expectedReplies[i][3]++;
					}
					if (expectedReplies[i][2] >= 2)
					{
						if (expectedReplies[i][4] == CREATE)
						{
							log->logCreateSuccess(&memberNode->addr, true, msg.transID, expectedRepliesStrings[i][0], expectedRepliesStrings[i][1]);
						}
						else if (expectedReplies[i][4] == UPDATE)
						{
							log->logUpdateSuccess(&memberNode->addr, true, msg.transID, expectedRepliesStrings[i][0], expectedRepliesStrings[i][1]);
						}
						else if (expectedReplies[i][4] == DELETE)
						{
							log->logDeleteSuccess(&memberNode->addr, true, msg.transID, expectedRepliesStrings[i][0]);
						}
						expectedReplies.erase(expectedReplies.begin() + i);
						expectedRepliesStrings.erase(expectedRepliesStrings.begin() + i);
						break;
					}
					else if (expectedReplies[i][3] >= 2)
					{
						if (expectedReplies[i][4] == CREATE)
						{
							log->logCreateFail(&memberNode->addr, true, msg.transID, expectedRepliesStrings[i][0], expectedRepliesStrings[i][1]);
						}
						else if (expectedReplies[i][4] == UPDATE)
						{
							log->logUpdateFail(&memberNode->addr, true, msg.transID, expectedRepliesStrings[i][0], expectedRepliesStrings[i][1]);
						}
						else if (expectedReplies[i][4] == DELETE)
						{
							log->logDeleteFail(&memberNode->addr, true, msg.transID, expectedRepliesStrings[i][0]);
						}
						expectedReplies.erase(expectedReplies.begin() + i);
						expectedRepliesStrings.erase(expectedRepliesStrings.begin() + i);
						break;
					}
				}
			}
		}
		else //READ REPLY
		{
			for (uint i = 0; i < expectedReplies.size(); i++)
			{
				if (msg.transID == expectedReplies[i][0])
				{
					printf("READ%s\n", expectedRepliesStrings[i][0].c_str());
					if (msg.value.length() == 0)
					{
						expectedReplies[i][3]++;
					}
					else
					{
						expectedReplies[i][2]++;
					}
					if (expectedReplies[i][2] >= 2)
					{
						log->logReadSuccess(&memberNode->addr, true, msg.transID, expectedRepliesStrings[i][0], msg.value);
						expectedReplies.erase(expectedReplies.begin() + i);
						expectedRepliesStrings.erase(expectedRepliesStrings.begin() + i);
					}
					else if (expectedReplies[i][3] >= 2)
					{
						log->logReadFail(&memberNode->addr, true, msg.transID, expectedRepliesStrings[i][0]);
						expectedReplies.erase(expectedReplies.begin() + i);
						expectedRepliesStrings.erase(expectedRepliesStrings.begin() + i);
					}
				}
			}
		}
	}

	/* Check for old expecting messages and mark as failed */
	for (int i = expectedReplies.size() - 1; i >= 0; i--)
	{
		if (par->globaltime - expectedReplies[i][1] > 3)
		{
			if (expectedReplies[i][4] == CREATE)
			{
				log->logCreateFail(&memberNode->addr, true, expectedReplies[i][0], expectedRepliesStrings[i][0], expectedRepliesStrings[i][1]);
			}
			else if (expectedReplies[i][4] == READ)
			{
				log->logReadFail(&memberNode->addr, true, expectedReplies[i][0], expectedRepliesStrings[i][0]);
			}
			else if (expectedReplies[i][4] == DELETE)
			{
				log->logDeleteFail(&memberNode->addr, true, expectedReplies[i][0], expectedRepliesStrings[i][0]);
			}
			else if (expectedReplies[i][4] == UPDATE)
			{
				log->logUpdateFail(&memberNode->addr, true, expectedReplies[i][0], expectedRepliesStrings[i][0], expectedRepliesStrings[i][1]);
			}
			expectedReplies.erase(expectedReplies.begin() + i);
			expectedRepliesStrings.erase(expectedRepliesStrings.begin() + i);
		}
	}
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key)
{
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3)
	{
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size() - 1).getHashCode())
		{
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else
		{
			// go through the ring until pos <= node
			for (uint i = 1; i < ring.size(); i++)
			{
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode())
				{
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i + 1) % ring.size()));
					addr_vec.emplace_back(ring.at((i + 2) % ring.size()));
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
bool MP2Node::recvLoop()
{
	if (memberNode->bFailed)
	{
		return false;
	}
	else
	{
		return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
	}
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size)
{
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
void MP2Node::stabilizationProtocol()
{
	/*
	 * Implement this
	 */
	map<string, string>::iterator it;
	for(it = ht->hashTable.begin(); it != ht->hashTable.end(); it++){
		string key = it->first;
		string value = it->second;
		
		vector<Node> replicas = findNodes(key);
		g_transID++;
		for (uint i = 0; i < replicas.size(); i++)
		{
			Message message(g_transID, memberNode->addr, CREATE, key, value, i == 0 ? PRIMARY : i == 1 ? SECONDARY : TERTIARY); //TODO: pass replica type
			emulNet->ENsend(&memberNode->addr, &replicas[i].nodeAddress, message.toString());
		}
	}
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP2Node::printAddress(Address *addr)
{
	printf("%d.%d.%d.%d:%d \n", addr->addr[0], addr->addr[1], addr->addr[2],
		   addr->addr[3], *(short *)&addr->addr[4]);
}