package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.print.attribute.HashPrintJobAttributeSet;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private final File dbFile;
    private final TupleDesc tupleDesc;
    private int numPages;

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
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.dbFile;
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
        int i = this.dbFile.getAbsoluteFile().hashCode();
        return i;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int tableId = pid.getTableId();
        int pageNumber = pid.getPageNumber();
        BufferPool bufferPool = Database.getBufferPool();
        int pageSize = bufferPool.getPageSize();
        byte[] emptyPageData = HeapPage.createEmptyPageData();
        //random access read from disk
        FileInputStream in = null;
        try {
            in =  new FileInputStream(this.dbFile);
            in.skip(pageNumber * pageSize);
            in.read(emptyPageData);
            return new HeapPage(new HeapPageId(tableId,pageNumber),emptyPageData);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;

    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
//        PageId id = page.getId();
//        int pageNumber = id.getPageNumber();
//        final int pageSize = BufferPool.getPageSize();
//
//        byte[] pageData = page.getPageData();
//        RandomAccessFile rws = new RandomAccessFile(dbFile, "rws");
//
//        rws.skipBytes(pageSize*pageNumber);
//        rws.write(pageData);
        int pgNo = page.getId().getPageNumber();

        if (pgNo>=0 && pgNo<= numPages()) {
            RandomAccessFile raf = new RandomAccessFile(dbFile, "rw");
            try {
                raf.seek(1L*BufferPool.getPageSize()*pgNo);
                raf.write(page.getPageData(), 0, BufferPool.getPageSize());
                return;
            } finally {
                raf.close();
            }
        }
        throw new IllegalArgumentException("pageId out of range");
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int fileSizeInByte = (int) dbFile.length();
        return numPages = fileSizeInByte/BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
//        assert tid!=null;
//        assert t!=null;
//
//        List<Page> pages = new ArrayList<>();
//
//        //HeapPageId heapPageId = null ;
//        for (int i = 0; i < numPages; i++) {
//            HeapPageId heapPageId = new HeapPageId(getId(), i);
//            HeapPage heapPage = (HeapPage)Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
//            if (heapPage!=null && heapPage.getNumEmptySlots()>0){
//                heapPage.insertTuple(t);
//                pages.add(heapPage);
//                break;
//            }
//        }
//        if (pages.size() == 0){
//            HeapPageId heapPageId = new HeapPageId(getId(), numPages);
//            HeapPage heapPage = new HeapPage(heapPageId, HeapPage.createEmptyPageData());
//            numPages++;
//            writePage(heapPage);
//            HeapPage hp = (HeapPage)Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
//            hp.insertTuple(t);
//            pages.add(hp);
//        }
//
//        return pages;
        // Done
        ArrayList<Page> list = new ArrayList<Page>();
        BufferPool pool = Database.getBufferPool();
        int tableId = getId(), pgNo = 0;

        for (; pgNo<numPages(); pgNo++) {
            HeapPage page = (HeapPage)pool.getPage(tid, new HeapPageId(tableId,pgNo), Permissions.READ_WRITE);

            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                list.add(page);
                break;
            }
        }
        if (pgNo == numPages()) {
            HeapPage page = new HeapPage(new HeapPageId(tableId, pgNo), HeapPage.createEmptyPageData());
            page.insertTuple(t);
            list.add(page);
            writePage(page);
        }
        return list;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here

        // not necessary for lab1
//        assert tid!=null;
//        assert t!=null;
//
//        ArrayList<Page> pages = new ArrayList<>();
//        //HeapPageId heapPageId= null;
//        for (int i = 0; i < this.numPages(); i++) {
//            HeapPageId heapPageId = new HeapPageId(getId(), i);
//            HeapPage heapPage = (HeapPage)Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
//            if (t.getRecordId().getPageId().equals(heapPageId)){
//                heapPage.deleteTuple(t);
//                pages.add(heapPage);
//            }
//        }
//
//        if (pages.size() ==0) {
//            throw new DbException("tuple " + t + " is not in this table");
//        }
//        return pages;
// Done
        ArrayList<Page> pages = new ArrayList<>();
        BufferPool pool = Database.getBufferPool();
        HeapPage page = (HeapPage)pool.getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);

        page.deleteTuple(t);
        pages.add(page);
        return pages;

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
        // some code goes here
       // return new HeapFileIterator(tid);
        return new DbFileIterator() {

            private final BufferPool pool = Database.getBufferPool();
            private final int tableId = getId();
            private int pid = -1;
            private Iterator<Tuple> child;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                pid = 0;
                child = null;
            }
            @Override
            public boolean hasNext() throws DbException,
                    TransactionAbortedException {
                if (null != child && child.hasNext()) {
                    return true;
                } else if (pid < 0 || pid >= numPages()) {
                    return false;
                } else {
                    child = ((HeapPage)pool.getPage(tid, new HeapPageId(tableId,pid++),
                            Permissions.READ_ONLY)).iterator();
                    return hasNext();
                }
            }
            @Override
            public Tuple next() throws DbException,
                    TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                } else {
                    return child.next();
                }
            }
            @Override
            public void rewind() throws DbException,
                    TransactionAbortedException {
                pid = 0;
                child = null;
            }
            @Override
            public void close() {
                pid = -1;
                child = null;
            }
        };
    }

}

