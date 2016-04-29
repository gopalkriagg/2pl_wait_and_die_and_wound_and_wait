#include <iostream>
#include <vector>
#include <stdlib.h>     /* srand, rand */
#include <time.h>       /* time */
#include <cstdio>
#include <math.h>
#include <limits.h>
using namespace std;



/**************************************************************
 * Class : ScheduleEntry
 *		Each instance of this class is an entry in the schedule
 *		that is to be built at the end of this program.
 ************************************************************/
class ScheduleEntry {
public:
	int txID;		//The tx ID of tx to which this schedule entry belongs
	char opType;	//The type of op performed i.e. read, write, commit or abort
	char var;		//The data item on which this operation is being performed
	int timeSlot;	//The time at which this tx is being executed
};

/*************************************************************
 * Class : Transaction
 *		An instance of this class contains all info about a tx.
 ************************************************************/
class Transaction {
public:
	int txID;					//The ID of this transaction.
	int timestamp;				//The timestamp when this tx entered into the system.
	vector<char *> operation;	//List of the operations in this transaction. e.g. rx, wy etc.
};

/**************************************************************
 * Class : LockTableEntry
 *		Each instance of this class is an entry in the lock
 *		table which stores the locked data item by var,
 *		the type of lock(Read/Write or Shared/Exclusive) by type,
 *		and the list of tx which has issued that lock
 *************************************************************/
class LockTableEntry {
public:
	char var;			//The data item locked
	char type;			//The type of lock
	vector<Transaction *> tx;	//The list of IDs of tx which has issued that lock
};


/************************************************************
 * Class : CurrentlyExecutable
 * 		Each instance of this class contains some info about
 * 		any tx which can currently be executed. Therefore,
 *		it doesn't contain tx whose timestamp is greater 
 *		than current time or tx which has committed.
 *		NOTE: It contains tx which has already been granted all the required locks!
 *		Additionally, it also contains info about the current
 *		operation the tx currently is on which is initially 0.
 ***********************************************************/
class CurrentlyExecutable {
public:
	Transaction * tx;	//The ptr to the tx
	int ptr;			//The op index within tx is currently to be executed
	CurrentlyExecutable() {
		ptr = 0;		//Initially ptr should be 0
	}
};
int t = 0;				//Time, t = 0 initially
vector<LockTableEntry *> LockTable;		//LockTable is a list of all the data items (variables) which are locked by any transcation currently.
vector<Transaction *> waitingTx;		//List of waiting transcations.
vector<CurrentlyExecutable *> currentlyExecutable;		//The list of tx which can currently be executed. Every second this list must be updated
vector<Transaction *> Transactions;		//It contains the list of all the transactions
vector<ScheduleEntry *> Schedule;		//Schedule which lists the order/interleaving of operations from various tx which will be performed by the system.
	
void updateCurrentlyExecutableTx();
void inputTransactions(vector<Transaction *> Transactions);
int chooseTxToExecute(vector<CurrentlyExecutable *> currentlyExecutable);
bool checkReadOrWriteLock(char dataItem);
//To check if there is a write lock on dataItem
bool checkWriteLock(char dataItem);
//To check if all locks can be granted to tx with index i
bool canAllLocksBeGranted(int i);
//To execute a currently executable tx
void execute(CurrentlyExecutable * c);
void checkWaitingQueue();
void freeLocks(int i);
	vector<int> waitingList (Transactions.size(), INT_MAX);

int main() {
		
	vector<int> waitingList (Transactions.size(), INT_MAX);
	inputTransactions(Transactions);		//Input all tx from stdin into 'Transactions'
	
	updateCurrentlyExecutableTx();	//Updating this list in the beginning
	
	int toExecute;	//To store the index (in currentlyExecuatable) of next tx which is to be executed next.
	while(currentlyExecutable.size() != 0) {
		toExecute = chooseTxToExecute(currentlyExecutable);

		//Execute an operation in currentlyExecutable[toExecute]; 
		//Since all read and write locks were done in the beginning it won't be any problem to simply execute this instruction
		//Execute also increments the ptr in this currently executable tx
		execute(currentlyExecutable[toExecute]);

		//If the currently executed op was the last op in the tx
		if(currentlyExecutable[toExecute]->ptr == currentlyExecutable[toExecute]->tx->operation.size()) {
			//In this case Free all the locks held by this tx.
			freeLocks(currentlyExecutable[toExecute]->tx->txID); 
			currentlyExecutable.erase(currentlyExecutable.begin() + toExecute);//@TODO
			//execute tx in its waiting list
			for (int j = 0; j<currentlyExecutable.size() ; j++){
				if (currentlyExecutable[j]->tx->txID == waitingList[currentlyExecutable[toExecute]->tx->txID])
				execute(currentlyExecutable[j]);
				}
			//execute(waitingList[currentlyExecutable[toExecute]->tx->txID])
			// then remove the tx from currently executable.
			
			//Check in waiting queue if any tx can be put in currently executable tx. If yes put it in currently exec.
			//checkWaitingQueue();	//@TODO
		}
		
			//(currentlyExecutable[toExecute]->ptr)++;	//To increment to the next instuction within tx that is to be executed
		
		updateCurrentlyExecutableTx();

	}
	
}

void updateCurrentlyExecutableTx() {
	CurrentlyExecutable * x;
	char dataItem;
	for(int i = 0; i < Transactions.size() && Transactions[i]->timestamp <= t; i++) {	//Loop through all Tx until timestamp = current time
		if(Transactions[i]->timestamp == t) {
			//If all the locks required by Transactions[i] can be granted (conservative 2PL) 
			//then only grant the locks
			//else put the tx in waiting queue
				//And put this tx into currently executable ones
				x = new CurrentlyExecutable();
				x->tx = Transactions[i];
				currentlyExecutable.push_back(x);
		}
			
		
	}
}

//inputTransactions inputs all the transactions from stdin and stores them in a list 'Transactions'
void inputTransactions(vector<Transaction *> Transactions) {
	char * line = new char[10];				//To store a line from the input
	char * op;								//
	Transaction * tx;						//To store the details of current transaction being input from stdin
	while(cin.getline(line, 10) ) {			//While there is input read a line
		cout << line;
		if(line[0] == 't') {				//If line starts with a 't' it means a new tx has started
			tx = new Transaction();			//In such a case allocate memory for the new tx
			sscanf(line, "t%d %d", &tx->txID, &tx->timestamp);		//Store the tx ID and timestamp when it entered the system.
			Transactions.push_back(tx);		//Add this transaction to the list of Transactions.
		}
		else {								//If first char is not 't' then this line must be a new operation in existing tx
			op = new char[2];				//Allocate memory for this operation
			op[0] = line[0];				//op[0] stores the type of operation ie read or write
			op[1] = line[1];				//op[1] stores the data item on which this op is being executed
			tx->operation.push_back(op);	//Add this operation to the current tx's list of operations.
		}
	}
}

int chooseTxToExecute(vector<CurrentlyExecutable *> currentlyExecutable) {
	srand(time(NULL));
	return rand() % currentlyExecutable.size();
}

//To check if there is a read or write lock on dataItem
bool checkReadOrWriteLock(char dataItem) {
	for(int i = 0; i < LockTable.size(); i++) {
		if(LockTable[i]->var == dataItem)
			return true;	//Indicating there is a read or write lock on dataItem
	}
	return false;	//Indicating there is no lock on dataItem
}

//To check if there is a write lock on dataItem
bool checkWriteLock(char dataItem) {
	for(int i = 0; i < LockTable.size(); i++) {
		if(LockTable[i]->var == dataItem && LockTable[i]->type == 'w')
			return true;	//Indicating there is a write lock on dataItem
	}
	return false;	//Indicating there is no write lock on dataItem
}

//To check if all locks can be granted to tx with index i
bool canAllLocksBeGranted(int i) {
	char dataItem;
	for(int j = 0; j < Transactions[i]->operation.size(); j++) {	//Loop through all the op in the tx
		dataItem = Transactions[i]->operation[j][1]);
		if(Transactions[i]->operation[j][0] == 'w') { //If a write lock is needed...
			if(checkReadOrWriteLock(dataItem)) {	//Check if there is any read or write lock on data item given as argument
				return false;	//In this case this tx cannot be put right now in currently execuatable list
			}
			//else lock('w', dataItem);	//Since it is a write lock there is no need to store which tx locked this data item.
		}
		else {	//If a read lock is needed...
			if(checkWriteLock(dataItem)) {	//...but already some tx has write lock on it
				return false;	//then set flag = 0 indicating it is not possible to execute this tx now
			}
		}
	}
	return true; //If program reaches this point then it is sure that all locks can be granted at this point
}

//To execute a currently executable tx
void execute(CurrentlyExecutable * c) {
	for (int i=0;i<LockTable.size()  ;i++){
		if (LockTable[i]->tx->txID != c->tx->txID){
		if (LockTable[i]->var = c->tx->operation[c->ptr][1]  && LockTable[i]->type == 'r') {
			if (c->tx->operation[c->ptr][0] =='r' ){
				ScheduleEntry scheduleEntry = new ScheduleEntry();
				scheduleEntry->txID = c->tx->txID;
				scheduleEntry->opType = c->tx->operation[c->ptr][0];
				scheduleEntry->var = c->tx->operation[c->ptr][1];
				scheduleEntry->timeSlot = t;
				Schedule.push_back(scheduleEntry);	//Put this schedule entry into the schedule
				(c->ptr)++;
				}

			else if (c->tx->operation[c->ptr][0] =='w'){	//Increment the ptr of this tx so that next time next operations is executed
				if (c->tx->timestamp < LockTable[i]->tx->timestamp){
					waitingList[LockTable[i]->tx->txID].push_back(c->tx->txID);
				}
				else {}
			}
		}

		else if (LockTable[i]->var = c->tx->operation[c->ptr][1] && LockTable[i]->type=='w')
		{ 
			if (c->tx->timestamp < LockTable[i]->tx->timestamp)
					waitingList[LockTable[i]->tx->txID].push_back(c->tx->txID);
				
				else {}
		}
	}
	else {
		ScheduleEntry scheduleEntry = new ScheduleEntry();
	scheduleEntry->txID = c->tx->txID;
	scheduleEntry->opType = c->tx->operation[c->ptr][0];
	scheduleEntry->var = c->tx->operation[c->ptr][1];
	scheduleEntry->timeSlot = t;
	Schedule.push_back(scheduleEntry);	//Put this schedule entry into the schedule
	(c->ptr)++;

	LockTableEntry lockTableEntry  = new LockTableEntry();
	lockTableEntry->var = c->tx->operation[c->ptr][1];
	lockTableEntry->type = c->tx->operation[c->ptr][0];
	lockTableEntry->tx = c->tx;
	LockTable.push_back(lockTableEntry);
	}
 
}
		
if (LockTable.size() == 0){

	LockTableEntry lockTableEntry  = new LockTableEntry();
	lockTableEntry->var = c->tx->operation[c->ptr][1];
	lockTableEntry->type = c->tx->operation[c->ptr][0];
	lockTableEntry->tx = c->tx;
	LockTable.push_back(lockTableEntry);
	
	scheduleEntry = new ScheduleEntry();
	scheduleEntry->txID = c->tx->txID;
	scheduleEntry->opType = c->tx->operation[c->ptr][0];
	scheduleEntry->var = c->tx->operation[c->ptr][1];
	scheduleEntry->timeSlot = t;
	Schedule.push_back(scheduleEntry);	//Put this schedule entry into the schedule
	(c->ptr)++;
	}
}





void freeLocks(int i){
int a=0;
	while (a<Transaction[i]->operation.size()){
	for (int j=0; j< LockTable.size();j++){
		if (LockTable[j]->var == Transaction[i]->operation[a][1])
		LockTable.erase(j);
	}
	a++;
	}
}
	
void PrintSchedule()
{
	cout<<"TX\t"<<"operation\t"<<"var\t"<<"time"<<endl;
	for (int i=0; i< Schedule.size();i++){
	cout<<Schedule[i].txID<<"\t"<<Schedule[i].opType<<"\t"<<Schedule[i].var<<"\t"<<Schedule[i].timeSlot<<endl;

}



