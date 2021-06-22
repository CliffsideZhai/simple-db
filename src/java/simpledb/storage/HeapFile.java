package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File dbFile;
    private TupleDesc tupleDesc;

    private int numPage;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.dbFile = f;
        this.tupleDesc = td;
        // some code goes here
        //System.out.println(BufferPool.getPageSize());
        numPage = (int) (dbFile.length() / BufferPool.getPageSize());
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return dbFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        // generate unique tableid
        return dbFile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
//        // some code goes here
//        Page page = null;
//        final  int pageSize = Database.getBufferPool().getPageSize();
//        byte[] emptyPageData = HeapPage.createEmptyPageData();
//        //random access read from disk
//
//        try (RandomAccessFile raf = new RandomAccessFile(getFile(), "r")) {
//            // page在HeapFile的偏移量
//            int pos = pid.getPageNumber() * pageSize;
//            raf.seek(pos);
//            //raf.skipBytes(pageSize*pid.getPageNumber());
//            raf.read(emptyPageData, 0, emptyPageData.length);
//            page = new HeapPage((HeapPageId) pid,emptyPageData);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return page;
        // some code goes here
        int tableid = pid.getTableId();
        int pgNo = pid.getPageNumber();
        final int pageSize = Database.getBufferPool().getPageSize();
        byte[] rawPgData = HeapPage.createEmptyPageData();

        // random access read from disk
        FileInputStream in = null;
        try {
            in = new FileInputStream(dbFile);
            try {
                in.skip(pgNo * pageSize);
                in.read(rawPgData);
                return new HeapPage(new HeapPageId(tableid, pgNo), rawPgData);
            } catch (IOException e) {
                throw new IllegalArgumentException("HeapFile: readPage:");
            } finally {
                in.close();
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("HeapFile: readPage: file not found");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1

//        int pgNo = page.getId().getPageNumber();
//
//        if (pgNo>=0 && pgNo<= numPages()) {
//            RandomAccessFile raf = new RandomAccessFile(dbFile, "rws");
//            try {
//                raf.seek(BufferPool.getPageSize()*pgNo);
//                raf.write(page.getPageData(), 0, BufferPool.getPageSize());
//                return;
//            } finally {
//                raf.close();
//            }
//        }
//        throw new IllegalArgumentException("pageId out of range");

       ///////////////////////////////////////////////
        PageId pid = page.getId();
        int pgNo = pid.getPageNumber();

        final int pageSize = Database.getBufferPool().getPageSize();
        byte[] pgData = page.getPageData();

        RandomAccessFile dbfile = new RandomAccessFile(dbFile, "rws");
        dbfile.skipBytes(pgNo * pageSize);
        dbfile.write(pgData);

    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int fileSizeInByte = (int) dbFile.length();
        return fileSizeInByte/BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {

        // Done
//        ArrayList<Page> list = new ArrayList<Page>();
//        BufferPool pool = Database.getBufferPool();
//        int tableId = getId(), pgNo = 0;
//
//        for (; pgNo<numPages(); pgNo++) {
//            HeapPage page = (HeapPage)pool.getPage(tid, new HeapPageId(tableId,pgNo), Permissions.READ_WRITE);
//
//            if (page.getNumEmptySlots() > 0) {
//                page.insertTuple(t);
//                list.add(page);
//                break;
//            }
//        }
//        if (pgNo == numPages()) {
//            HeapPage page = new HeapPage(new HeapPageId(tableId, pgNo), HeapPage.createEmptyPageData());
//            page.insertTuple(t);
//            list.add(page);
//            writePage(page);
//        }
//        return list;
        // not necessary for lab1
//        ArrayList<Page> affected = new ArrayList<>(1);
//        int numPages = numPages();
//
//        for (int pgNo = 0; pgNo <= numPages; pgNo++) {
//            HeapPageId pid = new HeapPageId(getId(), pgNo);
//            HeapPage pg;
//            if (pgNo < numPages) {
//                pg = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
//            } else {
//                // pgNo = numpages -> we need add new page
//                pg = new HeapPage(pid, HeapPage.createEmptyPageData());
//            }
//
//            if (pg.getNumEmptySlots() > 0) {
//                // insert will update tuple when inserted
//                pg.insertTuple(t);
//                // writePage(pg);
//                if (pgNo < numPages) {
//                    affected.add(pg);
//                } else {
//                    // should append the dbfile
//                    writePage(pg);
//                }
//                return affected;
//            }
//
//        }
//        // otherwise create new page and insert
//        throw new DbException("HeapFile: InsertTuple: Tuple can not be added");

        /////////////
        // some code goes here
        ArrayList<Page> affectedPages = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
//            HeapPageId pid = new HeapPageId(getId(), i);
            HeapPageId pid = new HeapPageId(getId(), i);
            HeapPage page = null;
            page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() != 0) {
                //page的insertTuple已经负责修改tuple信息来表明其存储在该page上
                page.insertTuple(t);
                page.markDirty(true, tid);
                affectedPages.add(page);
                break;
            }
        }
        if (affectedPages.size() == 0) {//说明page都已经满了
            //创建一个新的空白的Page
//            HeapPageId npid = new HeapPageId(getId(), numPages());
            HeapPageId npid = new HeapPageId(getId(), numPages());
            HeapPage blankPage = new HeapPage(npid, HeapPage.createEmptyPageData());
            numPage++;
            //将其写入磁盘
            writePage(blankPage);
            //通过BufferPool来访问该新的page
            HeapPage newPage = null;
            newPage = (HeapPage) Database.getBufferPool().getPage(tid, npid, Permissions.READ_WRITE);
            newPage.insertTuple(t);
            newPage.markDirty(true, tid);
            affectedPages.add(newPage);
        }
        return affectedPages;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here

//        ArrayList<Page> pages = new ArrayList<>();
//        BufferPool pool = Database.getBufferPool();
//        HeapPage page = (HeapPage)pool.getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
//
//        page.deleteTuple(t);
//        pages.add(page);
//        return pages;
        ArrayList<Page> affected = new ArrayList<>(1);
        RecordId rid = t.getRecordId();
        HeapPageId pid = (HeapPageId) rid.getPageId();
        if (pid.getTableId() == getId()) {
            int pgNo = pid.getPageNumber();
            HeapPage pg = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            pg.deleteTuple(t);
            // writePage(pg);
            affected.add(pg);
            return affected;
        }
        throw new DbException("HeapFile: deleteTuple: tuple.tableid != getId");

    }

    private class HeapFileIterator implements DbFileIterator{

        private Integer pgCursor;
        private Iterator<Tuple> tupleIterator;
        private final TransactionId transactionId;
        private final int tableId;
        private final int numPages;

        private HeapFileIterator(TransactionId tid) {
            this.pgCursor = null;
            this.tupleIterator = null;
            this.transactionId = tid;
            this.tableId = getId();
            this.numPages = numPages();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.pgCursor = 0;
            tupleIterator = this.getTupleIterator(pgCursor);
        }

        @Override
        public boolean hasNext() throws TransactionAbortedException, DbException {
            if (pgCursor!=null){
                while (pgCursor<numPages-1){
                    if (tupleIterator.hasNext()){
                        return true;
                    }else {
                        tupleIterator = getTupleIterator(++pgCursor);
                    }
                }
                return tupleIterator.hasNext();
            }else {
                return false;
            }
        }

        @Override
        public Tuple next() throws TransactionAbortedException, DbException {
            if (this.hasNext()){
                return tupleIterator.next();
            }
            throw new NoSuchElementException("HeapFileIterator: error: next: no more elemens");
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            pgCursor = null;
            tupleIterator = null;
        }

        private Iterator<Tuple> getTupleIterator (int pgNO) throws TransactionAbortedException, DbException {
            HeapPageId heapPageId = new HeapPageId(tableId, pgNO);
            return ((HeapPage)Database.getBufferPool().
                    getPage(transactionId,heapPageId, Permissions.READ_ONLY))
                    .iterator();
        }
    }
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {

        return new HeapFileIterator(tid);
        //return new HeapIterator(tid);
    }
    /**
     * 这个类在实现时有不少疑惑，参考了别人的代码才清楚以下一些点：
     * 1.tableid就是heapfile的id，即通过getId。。但是这个不是从0开始的，按照课程源码推荐，这是文件的哈希码。。
     * 2.PageId是从0开始的。。。(哪里说了，这个可以默认的么，谁知道这个作业的设计是不是从0开始。。。)
     * 3.transactionId哪里来的让我非常困惑，现在知道不用理，反正iterator方法的调用者会提供，应该是以后章节的内容
     * 4.我觉得别人的一个想法挺好，就是存储一个当前正在遍历的页的tuples迭代器的引用，这样一页一页来遍历
     */
    private class HeapIterator implements DbFileIterator {

        private int pagePos;

        private Iterator<Tuple> tuplesInPage;

        private TransactionId tid;

        public HeapIterator(TransactionId tid) {
            this.tid = tid;
        }

        public Iterator<Tuple> getTuplesInPage(HeapPageId pid) throws TransactionAbortedException, DbException {
            // 不能直接使用HeapFile的readPage方法，而是通过BufferPool来获得page，理由见readPage()方法的Javadoc
            HeapPage page = null;
            page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            return page.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pagePos = 0;
            HeapPageId pid = new HeapPageId(getId(), pagePos);
            //加载第一页的tuples
            tuplesInPage = getTuplesInPage(pid);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (tuplesInPage == null) {
                //说明已经被关闭
                return false;
            }
            //如果当前页还有tuple未遍历
            if (tuplesInPage.hasNext()) {
                return true;
            }
            //如果遍历完当前页，测试是否还有页未遍历
            //注意要减一，这里与for循环的一般判断逻辑（迭代变量<长度）不同，是因为我们要在接下来代码中将pagePos加1才使用
            //如果不理解，可以自己举一个例子想象运行过程
            if (pagePos < numPages() - 1) {
                pagePos++;
                HeapPageId pid = new HeapPageId(getId(), pagePos);
                tuplesInPage = getTuplesInPage(pid);
                //这时不能直接return ture，有可能返回的新的迭代器是不含有tuple的
                return tuplesInPage.hasNext();
            } else return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException("not opened or no tuple remained");
            }
            return tuplesInPage.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            //直接初始化一次。。。。。
            open();
        }

        @Override
        public void close() {
            pagePos = 0;
            tuplesInPage = null;
        }
    }
}

