#include "LogMgr.h"
#include <sstream>
#include <string>
#include <algorithm>
#include <queue>

using namespace std;

typedef std::pair<int, int> MyPairType;
struct CompareSecond
{
    bool operator()(const MyPairType& left, const MyPairType& right) const
    {
        return left.second < right.second;
    }
};

struct ToUndoComp
{
    bool operator()(LogRecord * left, LogRecord * right) 
    {
        return left->getLSN() > right->getLSN();
    }
};

int findLSN(vector <LogRecord*> log, int LSN);

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
    while(!logtail.empty() && logtail.front()->getLSN() <= maxLSN){
        se->updateLog(logtail.front()->toString());
        delete logtail.front();
        *(logtail.begin()) = nullptr;
        logtail.erase(logtail.begin());
    }
}

/* 
 * Run the analysis phase of ARIES.
 */
void LogMgr::analyze(vector <LogRecord*> log){
    //int a;
    //cin >> a;
    LogRecord * newRecord;
    int checkNum = findLSN(log, se->get_master());
    if(checkNum == NULL_LSN){
        checkNum = 0;
    }
    else{
        tx_table = dynamic_cast<ChkptLogRecord *>(log[checkNum + 1])->getTxTable();
        dirty_page_table = dynamic_cast<ChkptLogRecord *>(log[checkNum + 1])->getDirtyPageTable();
        checkNum += 2;
    }
    
    for(int i = checkNum; i < log.size(); ++i){
        newRecord = log[i];
        int txID = newRecord->getTxID();
        if (newRecord->getType() == END){
            tx_table.erase(txID); 
        }
        else{
            tx_table[txID].lastLSN = newRecord->getLSN();
            if(newRecord->getType() == COMMIT){
                tx_table[txID].status = C;
            }
            else{
                tx_table[txID].status = U;
            }
        }
        if(newRecord->getType() == CLR){
            dirty_page_table[dynamic_cast<CompensationLogRecord *>(newRecord)->getPageID()] = newRecord->getLSN();
        }
        else if(newRecord->getType() == UPDATE){
            dirty_page_table[dynamic_cast<UpdateLogRecord *>(newRecord)->getPageID()] = newRecord->getLSN();
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
    firstDirty = findLSN(log, firstDirty);
    for(int i = firstDirty; i < log.size(); ++i){
        newRecord = log[i];
        if(newRecord->getType() == CLR){
            CompensationLogRecord * cRecord = dynamic_cast<CompensationLogRecord *>(newRecord);
            if(dirty_page_table.count(cRecord->getPageID()) && dirty_page_table[cRecord->getPageID()] <= cRecord->getLSN()){
                if(se->getLSN(cRecord->getPageID()) < cRecord->getLSN()){
                    if(!se->pageWrite(cRecord->getPageID(), cRecord->getOffset(), cRecord->getAfterImage(), cRecord->getLSN())) return false;
                }
            }
        }
        else if(newRecord->getType() == UPDATE){
            UpdateLogRecord * uRecord = dynamic_cast<UpdateLogRecord *>(newRecord);
            if(dirty_page_table.count(uRecord->getPageID()) && dirty_page_table[uRecord->getPageID()] <= uRecord->getLSN()){
                if(se->getLSN(uRecord->getPageID()) < uRecord->getLSN()){
                    if(!se->pageWrite(uRecord->getPageID(), uRecord->getOffset(), uRecord->getAfterImage(), uRecord->getLSN())) return false;
                }
            }
        }
    }
    for(map<int, txTableEntry>::iterator it = tx_table.begin(); it != tx_table.end(); ){
        if(it->second.status == C){
            logtail.push_back(new LogRecord(se->nextLSN(), it->second.lastLSN, it->first, END));
            tx_table.erase(it++);
        }
        else{
            ++it;
        }
    }
    
    return true;
}

/*
 * If no txnum is specified, run the undo phase of ARIES.
 * If a txnum is provided, abort that transaction.
 * Hint: the logic is very similar for these two tasks!
 */
void LogMgr::undo(vector <LogRecord*> log, int txnum){
    priority_queue <LogRecord *, vector<LogRecord *>, ToUndoComp> ToUndo;
    if(txnum == NULL_TX){
        for(map<int, txTableEntry>::iterator it = tx_table.begin(); it != tx_table.end(); it++){
            ToUndo.push(log[findLSN(log,it->second.lastLSN)]);
        }
    }
    else{
        ToUndo.push(log[findLSN(log,tx_table[txnum].lastLSN)]);
    }
    while(!ToUndo.empty()){
        LogRecord * newRecord = ToUndo.top();
        ToUndo.pop();
        if(newRecord->getType() == CLR){
            CompensationLogRecord * cRecord = dynamic_cast<CompensationLogRecord *>(newRecord);
            if(cRecord->getUndoNextLSN() == NULL_LSN){
                LogRecord e(se->nextLSN(), cRecord->getLSN(), cRecord->getTxID(), END);
                logtail.push_back(new LogRecord (se->nextLSN(), cRecord->getLSN(), cRecord->getTxID(), END));
                tx_table.erase(cRecord->getTxID());
            }
            else{
                ToUndo.push(log[findLSN(log,cRecord->getUndoNextLSN())]);
            }
        }
        else if(newRecord->getType() == UPDATE){
            UpdateLogRecord * uRecord = dynamic_cast<UpdateLogRecord *>(newRecord);
            if(!se->pageWrite(uRecord->getPageID(), uRecord->getOffset(), uRecord->getBeforeImage(), uRecord->getprevLSN())) return;
            int cLSN = se->nextLSN();
            logtail.push_back(new CompensationLogRecord (cLSN, getLastLSN(uRecord->getTxID()), uRecord->getTxID(), uRecord->getPageID(), uRecord->getOffset(), uRecord->getBeforeImage(), uRecord->getprevLSN()));
            setLastLSN(uRecord->getTxID(), cLSN);
            if(uRecord->getprevLSN() != NULL_LSN){
                ToUndo.push(log[findLSN(log,uRecord->getprevLSN())]);
            }
            else{
                logtail.push_back(new LogRecord (se->nextLSN(), cLSN, uRecord->getTxID(), END));
                tx_table.erase(uRecord->getTxID());
            }
        }
        else{
            ToUndo.push(log[findLSN(log,newRecord->getprevLSN())]);
        }
    }
}


vector<LogRecord*> LogMgr::stringToLRVector(string logstring){
    vector<LogRecord*> result;
    istringstream stream(logstring);
    string line;
    while (getline(stream, line)) {
        LogRecord* lr;
        lr = LogRecord::stringToRecordPtr(line);
        result.push_back(lr);
    }
    return result; 
}

/*
 * Abort the specified transaction.
 * Hint: you can use your undo function
 */
void LogMgr::abort(int txid){
    int LSN = se->nextLSN();
    logtail.push_back(new LogRecord(LSN, getLastLSN(txid), txid, ABORT));
    setLastLSN(txid,LSN);
    flushLogTail(LSN);
    undo(stringToLRVector(se->getLog()),txid);
}

/*
 * Write the begin checkpoint and end checkpoint
 */
void LogMgr::checkpoint(){
    int LSN = se->nextLSN();
    logtail.push_back(new LogRecord(LSN, NULL_LSN, NULL_TX, BEGIN_CKPT));
    int LSN2 = se->nextLSN();
    logtail.push_back(new ChkptLogRecord(LSN2, LSN, NULL_TX, tx_table, dirty_page_table));
    flushLogTail(LSN2);
    se->store_master(LSN);
}

/*
 * Commit the specified transaction.
 */
void LogMgr::commit(int txid){
    int LSN = se->nextLSN();
    logtail.push_back(new LogRecord(LSN, getLastLSN(txid), txid, COMMIT));
    flushLogTail(LSN);
    tx_table.erase(txid);
    int LSN2 = se->nextLSN();
    logtail.push_back(new LogRecord(LSN2, LSN, txid, END));
}

/*
 * A function that StorageEngine will call when it's about to 
 * write a page to disk. 
 * Remember, you need to implement write-ahead logging
 */
void LogMgr::pageFlushed(int page_id){
    flushLogTail(se->getLSN(page_id));
}

/*
 * Recover from a crash, given the log from the disk.
 */
void LogMgr::recover(string log){
    vector<LogRecord*> v = stringToLRVector(log);
    analyze(v);
    if(!redo(v)) return;
    undo(v);
}

/*
 * Logs an update to the database and updates tables if needed.
 */
int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext){
    int LSN = se->nextLSN();
    if(!tx_table.count(txid)) setLastLSN(txid, NULL_LSN);
    logtail.push_back(new UpdateLogRecord(LSN, getLastLSN(txid), txid, page_id, offset, oldtext, input));
    setLastLSN(txid, LSN);
    tx_table[txid].status = U;
    if(!dirty_page_table.count(page_id)) dirty_page_table[page_id] = LSN;
    return LSN;
}

/*
 * Sets this.se to engine. 
 */
void LogMgr::setStorageEngine(StorageEngine* engine){
    this->se = engine;
}

//Given a vector of Records and a LSN, returns the index of the record with that LSN
int findLSN(vector <LogRecord*> log, int LSN){
    for(int i = 0; i < log.size(); ++i){
        if(log[i]->getLSN() == LSN) return i;
    }
    return -1; 
}