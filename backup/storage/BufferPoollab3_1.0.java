package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.common.DbException;

import simpledb.storage.cache.BufferCache;
import simpledb.storage.cache.LruCache;
import simpledb.storage.lock.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

///**
// * BufferPool manages the reading and writing of pages into memory from
// * disk. Access methods call into it to retrieve pages, and it fetches
// * pages from the appropriate location.
// * <p>
// * The BufferPool is also responsible for locking;  when a transaction fetches
// * a page, BufferPool checks that the transaction has the appropriate
// * locks to read/write the page.
// *
// * @Threadsafe, all fields are final
// */
//public class BufferPool {
//    /**
//     *  Bytes per page, including header.
//     *  <p/>
//     *  默认的page大小
//     * */
//    private static final int DEFAULT_PAGE_SIZE = 4096;
//
//    private static int PageSize = DEFAULT_PAGE_SIZE;
//
//    /** Default number of pages passed to the constructor. This is used by
//    other classes. BufferPool should use the numPages argument to the
//    constructor instead. */
//    public static final int DEFAULT_PAGES = 50;
//
//    /**
//     *  these are for buffer pool.
//     *  maximum number of pages in this buffer pool
//     *  <p/>
//     *  buffer池中最大的page数量
//     */
//    private int numberPage ;
//
//    /**
//     * set map connections
//     * 缓存
//     */
//    private BufferCache bufferPool ;
//
//    /**
//     * 獲取lock 管理器
//     */
//    private LockManager lockMgr;
//    private static int TRANSACTION_FACTOR = 2;
//    // timeout 1s for deadlock detection
//    private static int DEFAUT_MAXTIMEOUT = 5000;
//
//    public LruCache<PageId, Page> getBufferPool() {
//        return bufferPool;
//    }
//
//    public int getNumberPage() {
//        return numberPage;
//    }
//
//    /**
//     * Creates a BufferPool that caches up to numPages pages.
//     *
//     * @param numPages maximum number of pages in this buffer pool.
//     */
//    public BufferPool(int numPages) {
//        // some code goes here
//        this.numberPage = numPages;
//        this.bufferPool = new BufferCache(numPages);
//        lockMgr = new LockManager(numPages,TRANSACTION_FACTOR * numPages);
//
//    }
//
//    public static int getPageSize() {
//      return PageSize;
//    }
//
//    // TODO: THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
//    public static void setPageSize(int pageSize) {
//    	BufferPool.PageSize = pageSize;
//    }
//
//    // TODO: THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
//    public static void resetPageSize() {
//    	BufferPool.PageSize = DEFAULT_PAGE_SIZE;
//    }
//
//    /**
//     * Retrieve the specified page with the associated permissions.
//     * Will acquire a lock and may block if that lock is held by another
//     * transaction.
//     * <p>
//     * The retrieved page should be looked up in the buffer pool.  If it
//     * is present, it should be returned.  If it is not present, it should
//     * be added to the buffer pool and returned.  If there is insufficient
//     * space in the buffer pool, a page should be evicted and the new page
//     * should be added in its place.
//     *
//     * @param tid the ID of the transaction requesting the page
//     * @param pid the ID of the requested page
//     * @param perm the requested permissions on the page
//     */
//    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
//            throws TransactionAbortedException, DbException {
//        // some code goes here
//        //if it is present
//
//        /**
//         * 如果是只讀權限，使用排他鎖
//         */
//        LockManager.LockType lockType;
//        if (perm == Permissions.READ_ONLY) {
//            lockType = LockManager.LockType.ExclusiveLock;
//        } else {
//            lockType = LockManager.LockType.SharedLock;
//        }
//
//        Debug.log(pid.toString() + ": before acquire lock\n");
//        lockMgr.acquireLock(tid, pid, lockType, DEFAUT_MAXTIMEOUT);
//        Debug.log(pid.toString() + ": acquired the lock\n");
//
//        Page page = null;
//        if ((page = this.bufferPool.get(pid))!=null){
//            return page;
//        }else {
//            HeapFile table = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
//            HeapPage newPage = (HeapPage) table.readPage(pid);
//            //addNewPage(pid, newPage);
//            Page removedPage = bufferPool.put(pid, newPage);
//            if (removedPage != null) {
//                try {
//                    flushPage(removedPage.getId());
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//            return newPage;
//        }
//
//    }
//
//    /**
//     * Releases the lock on a page.
//     * Calling this is very risky, and may result in wrong behavior. Think hard
//     * about who needs to call this and why, and why they can run the risk of
//     * calling it.
//     *
//     * @param tid the ID of the transaction requesting the unlock
//     * @param pid the ID of the page to unlock
//     */
//    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
//        // some code goes here
//        // not necessary for lab1|lab2
//        lockMgr.releaseLock(tid,pid);
//
//    }
//
//    /**
//     * Release all locks associated with a given transaction.
//     *
//     * @param tid the ID of the transaction requesting the unlock
//     */
//    public void transactionComplete(TransactionId tid) {
//        // some code goes here
//        // not necessary for lab1|lab2
//        transactionComplete(tid,true);
//    }
//
//    /** Return true if the specified transaction has a lock on the specified page */
//    public boolean holdsLock(TransactionId tid, PageId p) {
//        // some code goes here
//        // not necessary for lab1|lab2
//        return lockMgr.holdsLock(tid,p);
//
//    }
//
//    /**
//     * Commit or abort a given transaction; release all locks associated to
//     * the transaction.
//     *
//     * @param tid the ID of the transaction requesting the unlock
//     * @param commit a flag indicating whether we should commit or abort
//     */
//    public void transactionComplete(TransactionId tid, boolean commit) {
//        // some code goes here
//        // not necessary for lab1|lab2
//        ArrayList<PageId> lockList = lockMgr.getLockList(tid);
//        if (lockList!=null){
//            for (PageId pid : lockList) {
//                Page page = bufferPool.get(pid);
//                if (page!=null){
//                    //如果是commit操作
//                    if (commit == true){
//                        try {
//                            flushPage(pid);
//                        } catch (IOException e) {
//                            e.printStackTrace();
//                        }
//                        page.setBeforeImage();
//                    } else if (page.isDirty() != null){//如果是abort操作
//                        //所有的髒頁要被刷出去，非髒頁要繼續留在bufferpool裏
//                        discardPage(page.getId());
//                    }
//                }
//            }
//        }
//
//        //最後把事務上所有的鎖都釋放
//        lockMgr.releaseLocksOnTransaction(tid);
//
//
//    }
//
//    /**
//     * Add a tuple to the specified table on behalf of transaction tid.  Will
//     * acquire a write lock on the page the tuple is added to and any other
//     * pages that are updated (Lock acquisition is not needed for lab2).
//     * May block if the lock(s) cannot be acquired.
//     *
//     * Marks any pages that were dirtied by the operation as dirty by calling
//     * their markDirty bit, and adds versions of any pages that have
//     * been dirtied to the cache (replacing any existing versions of those pages) so
//     * that future requests see up-to-date pages.
//     *
//     * @param tid the transaction adding the tuple
//     * @param tableId the table to add the tuple to
//     * @param t the tuple to add
//     */
//    public void insertTuple(TransactionId tid, int tableId, Tuple t)
//        throws DbException, IOException, TransactionAbortedException {
//        // some code goes here
//        // not necessary for lab1
//
//        DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableId);
//        List<Page> pages = databaseFile.insertTuple(tid, t);
//        for (Page page:pages) {
//            page.markDirty(true,tid);
//            //bufferPool.put(page.getId(),page);
//            bufferPool.put(page.getId(),page);
//        }
//    }
//
//    /**
//     * Remove the specified tuple from the buffer pool.
//     * Will acquire a write lock on the page the tuple is removed from and any
//     * other pages that are updated. May block if the lock(s) cannot be acquired.
//     *
//     * Marks any pages that were dirtied by the operation as dirty by calling
//     * their markDirty bit, and adds versions of any pages that have
//     * been dirtied to the cache (replacing any existing versions of those pages) so
//     * that future requests see up-to-date pages.
//     *
//     * @param tid the transaction deleting the tuple.
//     * @param t the tuple to delete
//     */
//    public  void deleteTuple(TransactionId tid, Tuple t)
//            throws DbException, TransactionAbortedException, IOException {
//        // some code goes here
//        // not necessary for lab1
//
//        int tableId = t.getRecordId().getPageId().getTableId();
//        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
//
//        List<Page> pages = dbFile.deleteTuple(tid, t);
//        for (Page page:pages) {
//            page.markDirty(true,tid);
//            //bufferPool.put(page.getId(), page);
//            bufferPool.put(page.getId(),page);
//        }
//
//    }
//
//    /**
//     * @apiNote  只是为了测试，自己不可以调用此方法<p></>
//     * Flush all dirty pages to disk.
//     * NB: Be careful using this routine -- it writes dirty data to disk so will
//     *     break simpledb if running in NO STEAL mode.
//     */
//    public synchronized void flushAllPages() throws IOException {
//        // some code goes here
//        // not necessary for lab1
//        Iterator<Page> it = bufferPool.iterator();
//        while (it.hasNext()) {
//            flushPage(it.next().getId());
//        }
//    }
//
//    /** Remove the specific page id from the buffer pool.
//        Needed by the recovery manager to ensure that the
//        buffer pool doesn't keep a rolled back page in its
//        cache.
//
//        Also used by B+ tree files to ensure that deleted pages
//        are removed from the cache so they can be reused safely
//    */
//    public synchronized void discardPage(PageId pid) {
//        // some code goes here
//        // not necessary for lab1
//        bufferPool.removePage(pid);
//    }
//
//    /**
//     * Flushes a certain page to disk
//     * @param pid an ID indicating the page to flush
//     */
//    private synchronized  void flushPage(PageId pid) throws IOException {
//        // some code goes here
//        // not necessary for lab1
//        DbFile databaseFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
//        HeapPage page = (HeapPage) databaseFile.readPage(pid);
//        databaseFile.writePage(page);
//        page.markDirty(false,null);
////        if (bufferPool.isCached(pid)){
////            Page page = bufferPool.get(pid);
////            TransactionId dirty = page.isDirty();
////            if (dirty!=null){
////                DbFile databaseFile = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
////                page.markDirty(false,null);
////                databaseFile.writePage(page);
////            }
////        }
//    }
//
//    /** Write all pages of the specified transaction to disk.
//     */
//    public synchronized  void flushPages(TransactionId tid) throws IOException {
//        // some code goes here
//        // not necessary for lab1|lab2
//        ArrayList<PageId> lockList = lockMgr.getLockList(tid);
//        if (lockList!=null){
//            for (PageId pid: lockList) {
//                flushPage(pid);
//            }
//        }
//
//    }
//
//    /**
//     * Discards a page from the buffer pool.
//     * Flushes the page to disk to ensure dirty pages are updated on disk.
//     */
//    private synchronized  void evictPage() throws DbException {
//        // some code goes here
//        // not necessary for lab1
//        List<PageId> allCachedPages = bufferPool.getAllCachedPages();
//        for (PageId pid: allCachedPages
//             ) {
//            if (bufferPool.get(pid).isDirty() == null){
//                discardPage(pid);
//                return;
//            }
//        }
//        throw new DbException("BufferPool: evictPage: all pages are marked as dirty");
//    }
//
//}
public class BufferPool {
    //这个类没有设计为单例类，是因为作者认为：
    // The Database class provides a static method, Database.getBufferPool(),
    // that returns a reference to the single BufferPool instance for the entire SimpleDB process
    //但是我还是觉得应该设计为单例类

    /**
     * Bytes per page, including header.
     */
    public static int PAGE_SIZE = 4096;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    //页的最大数量
    public int PAGES_NUM;

    //当前的缓存页
    private LruCache<PageId,Page> lruPagesPool;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        PAGES_NUM = numPages;
        lruPagesPool = new LruCache<>(PAGES_NUM);
    }

    public static int getPageSize() {
        return PAGE_SIZE;
    }

    public static void resetPageSize() {
        PAGE_SIZE = 50;
    }

    public static void setPageSize(int i) {
        PAGE_SIZE = i;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        // TODO: 17-5-26 怎么用tid和perm？？？？
        HeapPage page = (HeapPage) lruPagesPool.get(pid);
        if (page != null) {//直接命中
            return page;
        }
        //未命中，访问磁盘并将其缓存
        HeapFile table = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
        HeapPage newPage = (HeapPage) table.readPage(pid);
        Page removedPage = lruPagesPool.put(pid, newPage);
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
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for proj1
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
             {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock
     * acquisition is not needed for lab2). May block if the lock cannot
     * be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        HeapFile table = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> affectedPages = (ArrayList<Page>) table.insertTuple(tid, t);
        for (Page page : affectedPages) {
            page.markDirty(true, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t   the tuple to add
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        int tableId=t.getRecordId().getPageId().getTableId();
        HeapFile table = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        Page affectedPage = (Page) table.deleteTuple(tid, t);
        affectedPage.markDirty(true,tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     * 我加上deprecated了，因为课程解释了这个方法是便于测试用的，原文如下
     * flushAllPages() method is not something you would ever need in a real implementation of a buffer pool.
     * However, we need this method for testing purposes. You should never call this method from any real code.
     */
    @Deprecated
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for proj1
        Iterator<Page> it = lruPagesPool.iterator();
        while (it.hasNext()) {
            flushPage(it.next());
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Flushes a certain page to disk
     *
     * //@param  pid an ID indicating the page to flush
     * 我改了方法参数，因为想用自己写的LruCache来实现替换策略，而不是将相关代码嵌入到这个类的设计中
     */
    // TODO: 17-6-10 改了原来的方法参数
//    private synchronized void flushPage(PageId pid) throws IOException {
    private synchronized void flushPage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
        HeapPage dirty_page = (HeapPage) page;
        HeapFile table = (HeapFile) Database.getCatalog().getDatabaseFile(page.getId().getTableId());
        table.writePage(dirty_page);
        dirty_page.markDirty(false, null);
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    }


    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     * 这个方法不实现了，具体的替换策略已经在LruCache中体现了，标志deprecated好了
     */
    @Deprecated
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for proj1
    }


}
