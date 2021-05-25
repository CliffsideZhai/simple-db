package backup.storage;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.storage.BufferPool;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.*;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see simpledb.storage.BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte[] header;
    final simpledb.storage.Tuple[] tuples;
    final int numSlots;

    byte[] oldData;
    private final Byte oldDataLock= (byte) 0;

    private HashMap<String, Boolean> dirtyMap = new HashMap<>();
    private HashMap<String,TransactionId> dirtyTrans = new HashMap<>();

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see simpledb.storage.BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();
        
        tuples = new simpledb.storage.Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {        
        // some code goes here
        return (simpledb.storage.BufferPool.getPageSize()* 8) / (td.getSize()*8+1);
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        // some code goes here
        return (getNumTuples()+7)/8;
                 
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
    // some code goes here
    //throw new UnsupportedOperationException("implement this");
        return this.pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private simpledb.storage.Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        simpledb.storage.Tuple t = new simpledb.storage.Tuple(td);
        simpledb.storage.RecordId rid = new simpledb.storage.RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = simpledb.storage.BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = simpledb.storage.BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(simpledb.storage.Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
//
//        assert t!=null;
//        RecordId recordId = t.getRecordId();
//        PageId pageId = recordId.getPageId();
//
//        if (recordId!=null && pid.equals(pageId)){
//            for (int i = 0; i < numSlots; i++) {
//                if (isSlotUsed(i)&&tuples[i].getRecordId().equals(recordId)){
//                    markSlotUsed(i,false);
//                    tuples[i] = null;
//                    return;
//                }
//            }
//            throw new DbException("deleteTuple: Error: tuple slot is empty");
//        }
//        throw new DbException("deleteTuple: Error: tuple is not on this page");
        // Done
        if (pid.equals(t.getRecordId().getPageId())) {
            int tupleno = t.getRecordId().getTupleNumber();

            if (tupleno >= 0 && tupleno < numSlots) {
                if (isSlotUsed(tupleno)) {
                    tuples[tupleno] = null;
                    markSlotUsed(tupleno, false);
                    return;
                } else {
                    throw new DbException("deletion on empty slot");
                }
            }
        }
        throw new DbException("tuple not on the page");
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(simpledb.storage.Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
//        assert t!=null;
//        RecordId recordId = t.getRecordId();
//
//        PageId pageId = recordId.getPageId();
//        if (this.td.equals(t.getTupleDesc())){
//            for (int i = 0; i < numSlots; i++) {
//               if (!isSlotUsed(i)){
//                   tuples[i]= t;
//                   markSlotUsed(i,true);
//                   t.setRecordId(new RecordId(this.pid,i));
//                   return;
//               }
//            }
//            throw new DbException("insertTuple: ERROR: no tuple is inserted");
//        }
//        throw new DbException("insertTuple: no empty slots or tupledesc is mismatch");
        for (int i=0; i<numSlots; i++) {
            if (!isSlotUsed(i)) {
             //   t.setRecordId(new RecordId(pid, i));
                tuples[i] = t;
                markSlotUsed(i, true);
                return;
            }
        }
        throw new DbException("insertion on full page");
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
        // not necessary for lab1
        if (dirty == true){

            dirtyMap.put("dirty",dirty);
            dirtyTrans.put("tid",tid);
        }else {
            dirtyMap.put("dirty",false);
            dirtyTrans.put("tid",null);
        }

    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
        // Not necessary for lab1
        return dirtyTrans.get("tid");
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // some code goes here
//        int numEmptySlots = 0;
//        for (int i = 0;i<numSlots;i++){
//            if (!isSlotUsed(i)){
//                numEmptySlots++;
//            }
//        }
//        return numEmptySlots;
        // some code goes here
        int num = 0;

        for (int i=0; i<numSlots; i++) {
            if (!isSlotUsed(i)) {
                num++;
            }
        }
        return num;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        // some code goes here
//        if (i<numSlots){
//            int headerNo = i/8;
//            int offset = i%8;
//            return (header[headerNo] & (0x1 << offset)) != 0;
//        }
//        return false;
        if (i < numSlots) {
            int hdNo = (i / 8);
            int offset = i % 8;
            return (header[hdNo] & (0x1 << offset)) != 0;
        }
        return false;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1
        if (i > numSlots){
            throw  new IllegalArgumentException("this page is out of header slot size");
        }else {
            if (value == true){
                if (isSlotUsed(i)){
                    return;
                }else {
                    int hdNo = (i / 8);
                    int offset = i % 8;
                    header[hdNo] = (byte) (header[hdNo] | (0x1<<offset));
                }
            }
            if (value == false){
                if (isSlotUsed(i)){
                    int hdNo = (i / 8);
                    int offset = i % 8;
                    header[hdNo] = (byte) (header[hdNo] & ~(0x1 << offset));
                }else {
                    return;
                }
            }
        }
    }

    protected class HeapPageTupleIterator implements Iterator{
//        private final Iterator<Tuple> iterator;
//
//
//        public HeapPageTupleIterator() {
//            ArrayList<Tuple> tupleArrayList = new ArrayList<Tuple>(tuples.length);
//            for (int i =0;i<tuples.length;i++){
//                if (isSlotUsed(i)){
//                    tupleArrayList.add(i,tuples[i]);
//                }
//            }
//            iterator = tupleArrayList.iterator();
//
//        }
//
//        @Override
//        public boolean hasNext() {
//            return iterator.hasNext();
//        }
//
//        @Override
//        public Object next() {
//            return iterator.next();
//        }
//
//        @Override
//        public void remove() {
//            throw new UnsupportedOperationException("TupleIterator: remove not supported");
//        }
        private final Iterator<simpledb.storage.Tuple> iter;
        public HeapPageTupleIterator() {
            ArrayList<simpledb.storage.Tuple> tupleArrayList = new ArrayList<simpledb.storage.Tuple>(tuples.length);
            for (int i = 0; i < tuples.length; i++) {
                if (isSlotUsed(i)) {
                    tupleArrayList.add(i, tuples[i]);
                }
            }
            iter = tupleArrayList.iterator();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("TupleIterator: remove not supported");
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public Object next() {
            return iter.next();
        }
    }
    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<simpledb.storage.Tuple> iterator() {
        // some code goes here
//        return new HeapPageTupleIterator();
        // Done
        return new Iterator<simpledb.storage.Tuple>() {
            private int idx = -1;

            @Override
            public boolean hasNext() {
                while (idx+1<numSlots && !isSlotUsed(idx+1)) {
                    idx++;
                }
                return idx+1<numSlots;
            }
            @Override
            public Tuple next() {
                if (hasNext()) {
                    return tuples[++idx];
                } else {
                    throw new NoSuchElementException();
                }
            }
            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

}

