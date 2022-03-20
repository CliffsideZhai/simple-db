package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.storage.cache.BufferCache;
import simpledb.storage.cache.PageLruCache;
import simpledb.storage.lock.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.awt.font.ShapeGraphicAttribute;
import java.io.*;

import java.util.*;


/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     *  Bytes per page, including header.
     *  <p/>
     *  默认的page大小
     * */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int PageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     *  these are for buffer pool.
     *  maximum number of pages in this buffer pool
     *  <p/>
     *  buffer池中最大的page数量
     */
    private int numberPage ;

    /**
     * set map connections
     * 缓存
     */
    //private BufferCache bufferPool ;

    private PageLruCache bufferPool   ;
    //锁管理器
    private final LockManager lockManager;

    //事务获取不到锁时需要等待，由于实际用的是sleep来体现等待，此处参数是sleep的时间
    private final long SLEEP_INTERVAL;

    public PageLruCache getBufferPool() {
        return bufferPool;
    }

    public int getNumberPage() {
        return numberPage;
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numberPage = numPages;
        this.bufferPool = new PageLruCache(numPages);

        lockManager = new LockManager();
        //太小会造成忙碌的查询死锁，太大会浪费等待时间
        SLEEP_INTERVAL = 500;
    }

    public static int getPageSize() {
      return PageSize;
    }

    // TODO: THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.PageSize = pageSize;
    }

    // TODO: THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.PageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        //if it is present

        boolean result = (perm == Permissions.READ_ONLY) ? lockManager.grantSLock(tid, pid)
                : lockManager.grantXLock(tid, pid);
        //下面的while循环就是在模拟等待过程，隔一段时间就检查一次是否申请到锁了，还没申请到就检查是否陷入死锁
        while (!result) {
            if (lockManager.deadlockOccurred(tid, pid)) {
                throw new TransactionAbortedException();
            }
            try {
                Thread.sleep(SLEEP_INTERVAL);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //sleep之后再次判断result
            result = (perm == Permissions.READ_ONLY) ? lockManager.grantSLock(tid, pid)
                    : lockManager.grantXLock(tid, pid);
        }

        HeapPage page = (HeapPage) bufferPool.get(pid);
        if (page!=null){
            //緩存 命中
            return page;
        }

        HeapFile table = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
        HeapPage newPage = (HeapPage) table.readPage(pid);
        //addNewPage(pid, newPage);
        Page removedPage = bufferPool.put(pid, newPage);
        if (removedPage != null) {
            try {
                flushPage(removedPage);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return newPage;

    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public synchronized void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        if (!lockManager.unlock(tid, pid)) {
            //pid does not locked by any transaction
            //or tid  dose not lock the page pid
            throw new IllegalArgumentException();
        }

    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public synchronized void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        //transactionComplete(tid,true);
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.getLockState(tid, p) != null;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseTransactionLocks(tid);
        if (commit) {
            flushPages(tid);
        } else {
            revertTransactionAction(tid);
        }
    }

    /**
     * 在事务回滚时，撤销该事务对page造成的改变
     *
     * @param tid
     */
    public synchronized void revertTransactionAction(TransactionId tid) {
        Iterator<Page> it = bufferPool.iterator();
        while (it.hasNext()) {
            Page p = it.next();
            if (p.isDirty() != null && p.isDirty().equals(tid)) {
                bufferPool.reCachePage(p.getId());
            }
        }
    }
    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = databaseFile.insertTuple(tid, t);
        for (Page page:pages) {
            page.markDirty(true,tid);
            bufferPool.put(page.getId(),page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);

        List<Page> pages = null;
        try {
            pages = dbFile.deleteTuple(tid, t);
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (Page page:pages) {
            page.markDirty(true,tid);
            //bufferPool.put(page.getId(), page);
            bufferPool.put(page.getId(),page);
        }

    }

    /**
     * @apiNote  只是为了测试，自己不可以调用此方法<p></>
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        Iterator<Page> it = bufferPool.iterator();
        while (it.hasNext()) {
            Page p = it.next();
            if (p.isDirty() != null) {
                flushPage(p);
            }
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.

        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        //bufferPool.removePage(pid);
        bufferPool.removePage(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param page an ID indicating the page to flush
     */
    private synchronized  void flushPage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        DbFile databaseFile = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
        //HeapPage page = (HeapPage) databaseFile.readPage(pid);
        databaseFile.writePage(page);
        page.markDirty(false,null);

    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid){
        // some code goes here
        // not necessary for lab1|lab2
        Iterator<Page> it = bufferPool.iterator();
        while (it.hasNext()) {
            Page p = it.next();
            if (p.isDirty() != null && p.isDirty().equals(tid)) {
                try {
                    flushPage(p);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if (p.isDirty() == null) {
                    p.setBeforeImage();
                }
            }
        }

    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    @Deprecated
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1

    }

}
