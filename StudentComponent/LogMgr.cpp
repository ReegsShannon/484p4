#include "LogMgr.h"
#include <sstream>
#include <string>

using namespace std;

/*
 * Find the LSN of the most recent log record for this TX.
 * If there is no previous log record for this TX, return 
 * the null LSN.
 */
int getLastLSN(int txnum){
    return this->tx_Table[txnum].lastLSN;
}

/*
 * Update the TX table to reflect the LSN of the most recent
 * log entry for this transaction.
 */
void setLastLSN(int txnum, int lsn){
    tx_Table[txnum].lastLSN = lsn;
}

/*
 * Force log records up to and including the one with the
 * maxLSN to disk. Don't forget to remove them from the
 * logtail once they're written!
 */
void flushLogTail(int maxLSN){
    for(int i = 0; i < logtail.size(); ++i){
        if(logtail[i].getLSN() <= maxLSN){
            se->updateLog(logtail[i]->toString());
        }
    }
}

/* 
 * Run the analysis phase of ARIES.
 */
void analyze(vector <LogRecord*> log){
    int checkNum = get_master();
    sstream ss (getLog());
    string recordString;
    for(int i = 0; i < checkNum + 1; ++i){
        getline(ss, recordString);
    }
    string endCheckPointString;
    getline(ss, endCheckPointString);
    LogRecord * endCheckPoint = stringToRecordPtr(endCheckPointString);
    tx_table = endCheckPoint->getTxTable();
    dirty_page_table = endCheckPoint->getDirtyPageTable();
    while(!ss.eof()){
        getline(ss, recordString);
        LogRecord * newRecord = stringToRecordPtr(recordString);
        int txID = newRecord.getTxID();
        if (newRecord.getType == END){
            tx_table.remove(txID); 
        }
        else{
            txTableEntry * tableEntry = &tx_table[txID];
            tableEntry->lastLSN = newRecord->getLSN();
            if(newRecord.getType() == COMMIT){
                tableEntry->status = C;
            }
            else{
                tableEntry->status = U;
            }
        }
        if(newRecord.getType == UPDATE || newRecord.getType == CLR){
            dirty_page_table[newRecord.getPageID()] = newRecord->getLSN();
        }
    }
}

/*
 * Run the redo phase of ARIES.
 * If the StorageEngine stops responding, return false.
 * Else when redo phase is complete, return true. 
 */
bool redo(vector <LogRecord*> log){
    sstream ss (getLog());
    string recordString;
    int firstDirty = min_element(mymap.begin(), mymap.end(), CompareSecond())->second;
    for(int i = 0; i < firstDirty; ++i){
        getline(ss, recordString);
    }
    while(!ss.eof()){
        getline(ss, recordString);
        LogRecord * newRecord = stringToRecordPtr(endCheckPointString);
        if(newRecord->type == UPDATE || newRecord->type == CLR){
            if(dirty_page_table.count(newRecord->getPageID()) && dirty_page_table[newRecord->getPageID()] <= newRecord->getLSN()){
                Page * p = &records[findPage(newRecord->getPageID())];
                if(p->pageLSN < newRecord->getLSN()){
                    if(newRecord->type == UPDATE){
                        
                    }
                    else{
                        
                    }
                    p->pageLSN = newRecord->getLSN();
                }
            }
        }
    }
}

/*
 * If no txnum is specified, run the undo phase of ARIES.
 * If a txnum is provided, abort that transaction.
 * Hint: the logic is very similar for these two tasks!
 */
void undo(vector <LogRecord*> log, int txnum = NULL_TX){
    
}

/*
 * Abort the specified transaction.
 * Hint: you can use your undo function
 */
void abort(int txid){
    
}

/*
 * Write the begin checkpoint and end checkpoint
 */
void checkpoint(){
    
}

/*
 * Commit the specified transaction.
 */
void commit(int txid){
    
}

/*
 * A function that StorageEngine will call when it's about to 
 * write a page to disk. 
 * Remember, you need to implement write-ahead logging
 */
void pageFlushed(int page_id){
    
}

/*
 * Recover from a crash, given the log from the disk.
 */
void recover(string log){
    
}

/*
 * Logs an update to the database and updates tables if needed.
 */
int write(int txid, int page_id, int offset, string input, string oldtext){
    
}

/*
 * Sets this.se to engine. 
 */
void setStorageEngine(StorageEngine* engine){
    this->se = engine;
}

typedef std::pair<int, int> MyPairType;
struct CompareSecond
{
    bool operator()(const MyPairType& left, const MyPairType& right) const
    {
        return left.second < right.second;
    }
};