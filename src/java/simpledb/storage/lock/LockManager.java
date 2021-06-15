package simpledb.storage.lock;

import simpledb.storage.IntField;
import simpledb.storage.PageId;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 使用兩個hashmap來存放事務對應的page 和page對應的lock
 * 通過wait 和notify來實現阻塞
 */
public class LockManager {
    enum LockType {
        SharedLock,ExclusiveLock
    }

    class Lock{
        LockType type;
        PageId pageId;
        ArrayList<TransactionId> holders;

        public Lock(LockType type, PageId pageId, ArrayList<TransactionId> holders) {
            this.type = type;
            this.pageId = pageId;
            this.holders = holders;
        }

        public void setType(LockType type) {
            this.type = type;
        }

        public LockType getType() {
            return type;
        }

        public PageId getPageId() {
            return pageId;
        }

        public ArrayList<TransactionId> getHolders() {
            return holders;
        }

        //锁升级操作
        public boolean tryUpgradeLock(TransactionId tid){
            //对tid的持有的共享锁进行锁升级，需要保证，锁是共享锁，持有锁的只有一个
            if (type == LockType.SharedLock && holders.size() ==1&&holders.get(0).equals(tid)){
                type = LockType.ExclusiveLock;
                return true;
            }
            return false;
        }

        public TransactionId addHolder(TransactionId tid){
            if (type == LockType.SharedLock){
                if (!holders.contains(tid)){
                    holders.add(tid);
                }
                return tid;
            }
            return null;
        }
    }

    // pageid to lock
    private ConcurrentHashMap<PageId,Lock> lockTable;
    // who has the page
    private ConcurrentHashMap<TransactionId,ArrayList<PageId>> transactionTable;

    public LockManager(int lockTabCap, int transTabCap) {
        this.lockTable = new ConcurrentHashMap<>(lockTabCap);
        this.transactionTable = new ConcurrentHashMap<>(transTabCap);
    }

    public synchronized boolean holdsLock(TransactionId tid,PageId pid){
        ArrayList<PageId> lockList = getLockList(tid);
        return lockList!=null && lockList.contains(pid);

    }
    public ArrayList<PageId> getLockList(TransactionId tid){
        return transactionTable.getOrDefault(tid,null);
    }

    private void updateTransactionTable(TransactionId tid, PageId pid) {
        if (transactionTable.containsKey(tid)) {
            if (!transactionTable.get(tid).contains(pid)) {
                transactionTable.get(tid).add(pid);
            }
        } else {
            // no entry tid
            ArrayList<PageId> lockList = new ArrayList<PageId>();
            lockList.add(pid);
            transactionTable.put(tid, lockList);
        }
    }

    public synchronized void acquireLock(TransactionId tid, PageId pid, LockType reqLock,int maxTimeout) throws TransactionAbortedException {
        long start = System.currentTimeMillis();
        Random random = new Random();
        //將一個lang完全轉爲int
        long randomTimeout = random.nextInt((maxTimeout-0)+1)+0;
        while (true){
            //如果page已經有了鎖
            if (lockTable.contains(pid)){
                //且是共享鎖
                if (lockTable.get(pid).getType() == LockType.SharedLock){
                    //request read lock
                    if (reqLock == LockType.SharedLock){
                        updateTransactionTable(tid,pid);

                        assert lockTable.get(pid).addHolder(tid) !=null;
                        return;
                    }else {// request write lock
                        if (transactionTable.containsKey(tid)
                                && transactionTable.get(tid).contains(pid)
                                && transactionTable.get(tid).size()==1){
                            //upgrade read lock to write lock
                            assert lockTable.get(pid).getHolders().get(0) == tid;
                            lockTable.get(pid).tryUpgradeLock(tid);
                            return;
                        }else {
                            block(pid,start,randomTimeout);
                        }
                    }
                }else {//如果page是排他鎖
                    //如果排他鎖的持有者剛好是當前事務
                    if (lockTable.get(pid).getHolders().get(0) == tid){
                        //那就不用搶了
                        assert lockTable.get(pid).getHolders().size() ==1;
                        return;
                    }else {//如果不是，那麼阻塞，等待其他事務釋放這個page
                        block(pid,start,randomTimeout);
                    }
                }
            }else {//如果page，沒有鎖，那麼好辦了，上鎖就完事兒
                ArrayList<TransactionId> transactionIds = new ArrayList<>();
                transactionIds.add(tid);
                lockTable.put(pid,new Lock(reqLock,pid,transactionIds));
                updateTransactionTable(tid,pid);
                return;
            }
        }
    }

    private void block(PageId pid, long start, long randomTimeout) throws TransactionAbortedException {
        if (System.currentTimeMillis() - start > randomTimeout){
            throw new TransactionAbortedException();
        }

        try{
            wait(randomTimeout);
            if (System.currentTimeMillis() - start > randomTimeout) {
                throw new TransactionAbortedException();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public synchronized void releaseLock(TransactionId tid,PageId pid){
        // remove from trans table
        if (transactionTable.containsKey(tid)) {
            transactionTable.get(tid).remove(pid);
            if (transactionTable.get(tid).size() == 0) {
                transactionTable.remove(tid);
            }
        }


        // remove from locktable
        if (lockTable.containsKey(pid)) {
            lockTable.get(pid).getHolders().remove(tid);
            if (lockTable.get(pid).getHolders().size() == 0) {
                // no more threads are waiting here
                lockTable.remove(pid);
            } else {
                // ObjLock lock = lockTable.get(pid);
                // synchronized (lock) {
                notifyAll();
                //}
            }
        }
    }

    public synchronized void releasePagesOnTransaction(TransactionId tid){
        if (transactionTable.containsKey(tid)){
            ArrayList<PageId> pageIds = transactionTable.get(tid);
            for (PageId pid : pageIds ) {
                releaseLock(tid,pid);
            }
        }
    }
}
