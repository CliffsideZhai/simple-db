package simpledb.execution;

import java.awt.image.DataBuffer;
import java.lang.*;
import simpledb.common.Database;
import simpledb.storage.DbFile;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    //private int tableid;
    private String tableAlias;
    private DbFile dbFile;
    private DbFileIterator dbfileIterator;


    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid =tid;
        //this.tableid = tableid;
        this.tableAlias = tableAlias;
        this.dbFile = Database.getCatalog().getDatabaseFile(tableid);
        this.dbfileIterator = dbFile.iterator(tid);

    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(dbFile.getId());
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        //this.tableid = tableid;
        this.tableAlias = tableAlias;
        this.dbFile = Database.getCatalog().getDatabaseFile(tableid);
        this.dbfileIterator = dbFile.iterator(this.tid);
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        dbfileIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        final TupleDesc tupleDesc = Database.getCatalog().getTupleDesc(dbFile.getId());
        Type[] types = new Type[tupleDesc.numFields()];
        String[] fields = new String[tupleDesc.numFields()];
        String prefix = "";
        if (tableAlias != null){
            prefix = tableAlias;
        }
        for (int i =0;i<tupleDesc.numFields();i++){
            types[i] = tupleDesc.getFieldType(i);
            String s= tupleDesc.getFieldName(i);
            if (s ==null){
                s = "";
            }
            s = prefix+"."+s;
            fields[i] = s;
        }
        return new TupleDesc(types,fields);
        ///////////////////////////
        // Done
//        TupleDesc td = Database.getCatalog().getTupleDesc(dbFile.getId());
//        Type[] types = new Type[td.numFields()];
//        String[] names = new String[td.numFields()];
//
//        for (int i=0; i<td.numFields(); i++) {
//            types[i] = td.getFieldType(i);
//            names[i] = tableAlias+'.'+td.getFieldName(i);
//        }
//        return new TupleDesc(types, names);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return dbfileIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        return dbfileIterator.next();
    }

    public void close() {
        // some code goes here
        dbfileIterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        dbfileIterator.rewind();
    }
}
