#include "LogMgr.h"
#include <sstream>
#include <string>
#include <algorithm>

using namespace std;

typedef std::pair<int, int> MyPairType;
struct CompareSecond
{
    bool operator()(const MyPairType& left, const MyPairType& right) const
    {
        return left.second < right.second;
    }
};

/*
 * Find the LSN of the most recent log record for this TX.
 * If there is no previous log record for this TX, return 
 * the null LSN.
 */
int LogMgr::getLastLSN(int txnum){
    return tx_table[txnum].lastLSN;
}

/*
 * Update the TX table to reflect the LSN of the most recent
 * log entry for this transaction.
 */
void LogMgr::setLastLSN(int txnum, int lsn){
    tx_table[txnum].lastLSN = lsn;
}

/*
 * Force log records up to and including the one with the
 * maxLSN to disk. Don't forget to remove them from the
 * logtail once they're written!
 */
void LogMgr::flushLogTail(int maxLSN){
    for(int i = 0; i < logtail.size(); ++i){
        if(logtail[i]->getLSN() <= maxLSN){
            se->updateLog(logtail[i]->toString());
        }
    }
}

/* 
 * Run the analysis phase of ARIES.
 */
void LogMgr::analyze(vector <LogRecord*> log){
    int checkNum = se->get_master();
    LogRecord * newRecord;
    tx_table = dynamic_cast<ChkptLogRecord *>(log[checkNum + 1])->getTxTable();
    dirty_page_table = dynamic_cast<ChkptLogRecord *>(log[checkNum + 1])->getDirtyPageTable();
    for(int i = checkNum + 2; i < log.size(); ++i){
        newRecord = log[i];
        int txID = newRecord->getTxID();
        if (newRecord->getType() == END){
            tx_table.erase(txID); 
        }
        else{
            txTableEntry * tableEntry = &tx_table[txID];
            tableEntry->lastLSN = newRecord->getLSN();
            if(newRecord->getType() == COMMIT){
                tableEntry->status = C;
            }
            else{
                tableEntry->status = U;
            }
        }
        if(newRecord->getType() == UPDATE || newRecord->getType() == CLR){
            dirty_page_table[dynamic_cast<CompensationLogRecord *>(newRecord)->getPageID()] = newRecord->getLSN();
        }
    }
}

/*
 * Run the redo phase of ARIES.
 * If the StorageEngine stops responding, return false.
 * Else when redo phase is complete, return true. 
 */
bool LogMgr::redo(vector <LogRecord*> log){
    LogRecord * newRecord;
    int firstDirty = min_element(dirty_page_table.begin(), dirty_page_table.end(), CompareSecond())->second;
    for(int i = firstDirty; i < log.size(); ++i){
        newRecord = log[i];
        if(newRecord->getType() == UPDATE || newRecord->getType() == CLR){
            if(dirty_page_table.count(dynamic_cast<CompensationLogRecord *>(newRecord)->getPageID()) && dirty_page_table[dynamic_cast<CompensationLogRecord *>(newRecord)->getPageID()] <= newRecord->getLSN()){
                if(se->getLSN(dynamic_cast<CompensationLogRecord *>(newRecord)->getPageID()) < newRecord->getLSN()){
                    if(newRecord->getType() == UPDATE){
                        
                    }
                    else{
                        
                    }
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
void LogMgr::undo(vector <LogRecord*> log, int txnum){
    
}


vector<LogRecord*> LogMgr::stringToLRVector(string logstring){
    vector<LogRecord*> result;
    istringstream stream(logstring);
    string line;
    while (getline(stream, line)) {
        LogRecord* lr;
        lr->stringToRecordPtr(line);
        result.push_back(lr);
    }
    return result; 
}

/*
 * Abort the specified transaction.
 * Hint: you can use your undo function
 */
void LogMgr::abort(int txid){
    
}

/*
 * Write the begin checkpoint and end checkpoint
 */
void LogMgr::checkpoint(){
    
}

/*
 * Commit the specified transaction.
 */
void LogMgr::commit(int txid){
    
}

/*
 * A function that StorageEngine will call when it's about to 
 * write a page to disk. 
 * Remember, you need to implement write-ahead logging
 */
void LogMgr::pageFlushed(int page_id){
    
}

/*
 * Recover from a crash, given the log from the disk.
 */
void LogMgr::recover(string log){
    
}

/*
 * Logs an update to the database and updates tables if needed.
 */
int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext){
    
}

/*
 * Sets this.se to engine. 
 */
void LogMgr::setStorageEngine(StorageEngine* engine){
    this->se = engine;
}