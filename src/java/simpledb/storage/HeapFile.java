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
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private final File dbFile;
    private final TupleDesc tupleDesc;
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
        // some code goes here
        Page page = null;
        final  int pageSize = Database.getBufferPool().getPageSize();
        byte[] emptyPageData = HeapPage.createEmptyPageData();
        //random access read from disk

        try (RandomAccessFile raf = new RandomAccessFile(getFile(), "r")) {
            // page在HeapFile的偏移量
            int pos = pid.getPageNumber() * BufferPool.getPageSize();
            raf.seek(pos);
            raf.read(emptyPageData, 0, emptyPageData.length);
            page = new HeapPage((HeapPageId) pid,emptyPageData);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1

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
        return fileSizeInByte/BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {

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

        return new HeapFileIterator(tid);
    }

}

