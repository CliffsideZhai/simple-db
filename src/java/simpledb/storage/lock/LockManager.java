package simpledb.storage.lock;

import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    //Key相当于资源，LockState存放事务id与锁类型，故每个LockState代表某事务在Key上加了锁
    //故整个map为所有资源的锁信息
    private Map<PageId, List<LockState>> lockStateMap;

    //Key为事务，PageId为正在等待的资源，相当于保存了等待的信息，
    // BufferPool中实际用的是sleep体现等待，我試一試使用wait notify的形式
    private Map<TransactionId, PageId> waitingInfo;

    public LockManager() {
        //使用支持并发的容器避免ConcurrentModificationException
        lockStateMap = new ConcurrentHashMap<>();
        waitingInfo = new ConcurrentHashMap<>();
    }


}
