package backup.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.storage.HeapFile;
import simpledb.storage.HeapPage;
import simpledb.storage.Tuple;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

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
    private final int numberPage ;

    /**
     * set map connections
     * 缓存
     */
    private LruCache<PageId, Page> bufferPool ;

    public LruCache<PageId, Page> getBufferPool() {
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
        this.bufferPool = new LruCache<>(numPages);

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
        if (this.bufferPool.get(pid)!=null){
            return this.bufferPool.get(pid);
        }else {
            simpledb.storage.HeapFile table = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
            simpledb.storage.HeapPage newPage = (simpledb.storage.HeapPage) table.readPage(pid);
            //addNewPage(pid, newPage);
            Page removedPage = bufferPool.put(pid, newPage);
            if (removedPage != null) {
                try {
                    flushPage(removedPage.getId());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return newPage;
        }
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
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
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
    public void insertTuple(TransactionId tid, int tableId, simpledb.storage.Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = databaseFile.insertTuple(tid, t);
        for (Page page:pages) {
            page.markDirty(true,tid);
            //bufferPool.put(page.getId(),page);
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
            throws DbException, TransactionAbortedException, IOException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();

        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);

        List<Page> pages = dbFile.deleteTuple(tid, t);
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
            flushPage(it.next().getId());
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

    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        DbFile databaseFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        simpledb.storage.HeapPage page = (HeapPage) databaseFile.readPage(pid);
        databaseFile.writePage(page);
        page.markDirty(false,null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1

    }

}

/**
 * 使用 lru 最近最少使用算法来实现buffer pool的缓存换入换出
 * @param <K>
 * @param <V>
 */
class LruCache<K,V>{

    private class Node{
        Node front;
        Node next;
        K key;
        V value;

        public Node(K key,V v) {
            this.key = key;
            value = v;
        }
    }
    /**
     * 当前缓存
     */
    private ConcurrentHashMap<K,Node> cacheEntries;

    /**
     * 允许缓存
     */
    private int capacity;

    private Node head;

    private Node tail;

    public LruCache(int capacity) {
        this.capacity = capacity;
        cacheEntries =new ConcurrentHashMap<>(capacity);
        head= new Node(null,null);
    }

    /**
     * 删除结点
     * @param ruNode the recently used Node
     */
    private void unlink(Node ruNode) {
        //如果是最后一个结点
        if (ruNode.next == null) {
            ruNode.front.next = null;
        } else {
            ruNode.front.next=ruNode.next;
            ruNode.next.front=ruNode.front;
        }
    }

    /**
     * 讲最近使用的节点，添加到头结点
     * @param ruNode 最近使用的节点
     */
    private void linkFirst(Node ruNode){
        Node oldHead = this.head.next;
        this.head.next = ruNode;
        ruNode.next = oldHead;
        ruNode.front = this.head;
        if (oldHead == null){
            tail = ruNode;
        }else {
            oldHead.front = ruNode;
        }
    }
    /**
     * 删除链表的最后一个元素
     * @return  返回被删除的元素
     */
    private K removeTail() {
        K element=tail.key;
        Node newTail = tail.front;
        tail.front=null;
        newTail.next=null;
        tail=newTail;
        return element;
    }

    /**
     *
     * @param key
     * @param value
     * @return     被删除出缓存的条目，如果没有，返回null
     */
    public V put(K key, V value) {
        if (key == null | value == null) {//不允许插入null值
            throw new IllegalArgumentException();
        }
        if (isCached(key)) {
            //该结点存在于cache中，则更新其值，然后调整最近使用的条目，返回null(因为没有被删除的条目)
            Node ruNode = cacheEntries.get(key);
            ruNode.value=value;
            // TODO : 讲之前的节点断开
            unlink(ruNode);
            // TODO : 加入到头
            linkFirst(ruNode);
            return null;
        } else  {
            //不存在的话先判断是否已经达到容量，是的话要先删除尾结点最后将其返回
            //还没有的话只需要新建结点，然后插入到表头，返回null
            V removed=null;
            if (cacheEntries.size() == capacity) {
                K removedKey=removeTail();
                removed = cacheEntries.remove(removedKey).value;
            }
            Node ruNode = new Node(key, value);
            linkFirst(ruNode);
            cacheEntries.put(key, ruNode);
            return removed;
        }
    }
    /**
     *
     * @param key
     * @return  返回存在与缓存中的条目，不存在则返回null
     */
    public V get(K key) {
        if (isCached(key)) {
            //调整最近使用的条目
            Node ruNode = cacheEntries.get(key);
            unlink(ruNode);
            linkFirst(ruNode);
            return ruNode.value;
        }
        return null;
    }
    public boolean isCached(K key) {
        return cacheEntries.containsKey(key);
    }

    /**
     *
     * @return 当前缓存的所有value
     */
    public Iterator<V> iterator() {
        return new LruIter();
    }

    private class LruIter implements Iterator<V> {
        Node n = head;

        @Override
        public boolean hasNext() {
            return n.next!=null;
        }

        @Override
        public V next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            n=n.next;
            return n.value;
        }
    }
}

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
//    /** Bytes per page, including header. */
//    private static final int PAGE_SIZE = 4096;
//
//    private static int pageSize = PAGE_SIZE;
//
//    /** Default number of pages passed to the constructor. This is used by
//     other classes. BufferPool should use the numPages argument to the
//     constructor instead. */
//    public static final int DEFAULT_PAGES = 50;
//
//    // page buffer; PageId -> page
//    private ConcurrentHashMap<PageId, Page> pgBufferPool;
//    int capacity;
//    // int size;
//    /**
//     * Creates a BufferPool that caches up to numPages pages.
//     *
//     * @param numPages maximum number of pages in this buffer pool.
//     */
//    public BufferPool(int numPages) {
//        // some code goes here
//        this.capacity = numPages;
//        this.pgBufferPool = new ConcurrentHashMap<PageId, Page>();
//    }
//
//    public static int getPageSize() {
//        return pageSize;
//    }
//
//    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
//    public static void setPageSize(int pageSize) {
//        BufferPool.pageSize = pageSize;
//    }
//
//    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
//    public static void resetPageSize() {
//        BufferPool.pageSize = PAGE_SIZE;
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
//     * space in the buffer pool, an page should be evicted and the new page
//     * should be added in its place.
//     *
//     * @param tid the ID of the transaction requesting the page
//     * @param pid the ID of the requested page
//     * @param perm the requested permissions on the page
//     */
//    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
//            throws TransactionAbortedException, DbException {
//        // some code goes here
//        Page pg;
//        if (pgBufferPool.containsKey(pid)) {
//            pg = pgBufferPool.get(pid);
//        } else {
//            if (pgBufferPool.size() >= capacity) {
//                evictPage();
//            }
//            pg = Database
//                    .getCatalog()
//                    .getDatabaseFile(pid.getTableId())
//                    .readPage(pid);
//            pgBufferPool.put(pid, pg);
//        }
//        return pg;
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
//    public  void releasePage(TransactionId tid, PageId pid) {
//        // some code goes here
//        // not necessary for lab1|lab2
//    }
//
//    /**
//     * Release all locks associated with a given transaction.
//     *
//     * @param tid the ID of the transaction requesting the unlock
//     */
//    public void transactionComplete(TransactionId tid)  {
//        // some code goes here
//        // not necessary for lab1|lab2
//    }
//
//    /** Return true if the specified transaction has a lock on the specified page */
//    public boolean holdsLock(TransactionId tid, PageId p) {
//        // some code goes here
//        // not necessary for lab1|lab2
//        return false;
//    }
//
//    /**
//     * Commit or abort a given transaction; release all locks associated to
//     * the transaction.
//     *
//     * @param tid the ID of the transaction requesting the unlock
//     * @param commit a flag indicating whether we should commit or abort
//     */
//    public void transactionComplete(TransactionId tid, boolean commit)
//             {
//        // some code goes here
//        // not necessary for lab1|lab2
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
//            throws DbException, IOException, TransactionAbortedException {
//        // some code goes here
//        // not necessary for lab1
//        DbFile tableFile = Database.getCatalog().getDatabaseFile(tableId);
//        ArrayList<Page> affected = (ArrayList<Page>) tableFile.insertTuple(tid, t);
//        for (Page newPg : affected) {
//            newPg.markDirty(true, tid);
//            pgBufferPool.remove(newPg.getId());
//            pgBufferPool.put(newPg.getId(), newPg);
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
//    public void deleteTuple(TransactionId tid, Tuple t)
//            throws DbException, IOException, TransactionAbortedException {
//        // some code goes here
//        // not necessary for lab1
//
//        DbFile tableFile = Database
//                .getCatalog()
//                .getDatabaseFile(t.getRecordId().getPageId().getTableId());
//        ArrayList<Page> affected = (ArrayList<Page>) tableFile.deleteTuple(tid, t);
//        for (Page newPg : affected) {
//            newPg.markDirty(true, tid);
//            pgBufferPool.remove(newPg.getId());
//            pgBufferPool.put(newPg.getId(), newPg);
//        }
//    }
//
//    /**
//     * Flush all dirty pages to disk.
//     * NB: Be careful using this routine -- it writes dirty data to disk so will
//     *     break simpledb if running in NO STEAL mode.
//     */
//    public synchronized void flushAllPages() throws IOException {
//        // some code goes here
//        // not necessary for lab1
//        Enumeration<PageId> it = pgBufferPool.keys();
//        while (it.hasMoreElements()) {
//            flushPage(it.nextElement());
//        }
//    }
//
//    /** Remove the specific page id from the buffer pool.
//     Needed by the recovery manager to ensure that the
//     buffer pool doesn't keep a rolled back page in its
//     cache.
//
//     Also used by B+ tree files to ensure that deleted pages
//     are removed from the cache so they can be reused safely
//     */
//    public synchronized void discardPage(PageId pid) {
//        // some code goes here
//        // not necessary for lab1
//        pgBufferPool.remove(pid);
//    }
//
//    /**
//     * Flushes a certain page to disk
//     * @param pid an ID indicating the page to flush
//     */
//    private synchronized  void flushPage(PageId pid) throws IOException {
//        // some code goes here
//        // not necessary for lab1
//        if (pgBufferPool.contains(pid)) {
//            Page pg = pgBufferPool.get(pid);
//            if (pg.isDirty() != null) {
//                // then write back
//                DbFile tb = Database.getCatalog().getDatabaseFile(pg.getId().getTableId());
//                tb.writePage(pg);
//                pg.markDirty(false, null);
//            }
//        }
//    }
//
//    /** Write all pages of the specified transaction to disk.
//     */
//    public synchronized  void flushPages(TransactionId tid) throws IOException {
//        // some code goes here
//        // not necessary for lab1|lab2
//    }
//
//    /**
//     * Discards a page from the buffer pool.
//     * Flushes the page to disk to ensure dirty pages are updated on disk.
//     */
//    private synchronized  void evictPage() throws DbException {
//        // some code goes here
//        // not necessary for lab1
//        // randomly discard a page
//        PageId pid = null;
//        Enumeration<PageId> it = pgBufferPool.keys();
//        if (it.hasMoreElements()) {
//            pid = it.nextElement();
//            try {
//                flushPage(pid);
//                discardPage(pid);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//        public  void unsafeReleasePage(TransactionId tid, PageId pid) {
//        // some code goes here
//        // not necessary for lab1|lab2
//    }
//}
