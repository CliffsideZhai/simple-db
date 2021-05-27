package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Utility;
import simpledb.storage.BufferPool;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;


/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     *
     */
    private TransactionId tid;

    private OpIterator child;

    private int tableId;

    private OpIterator[] children;

    //insert操作影响了的tuple数量
    private int count;

    private TupleDesc td;

    private boolean hasAccessed ;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        TupleDesc childTupleDesc = child.getTupleDesc();
        TupleDesc tupleDesc = Database.getCatalog().getTupleDesc(tableId);
        System.out.println("child"+childTupleDesc.toString());
        System.out.println("now"+tupleDesc.toString());
        if (!childTupleDesc.equals(tupleDesc)){
            throw new DbException("两个table不一致");
        }else {
            this.tableId = tableId;
            this.tid =t;
            this.child =child;
            count = 0;
        }

    }

    /**
     * 在这个类里可以自定义一个tuple desc 且都是int类型的field
     * @see Utility#getTupleDesc(int)
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        if (child!=null){
            return Utility.getTupleDesc(1);
        }
        return null;
    }

    /**
     * open里应该只做open操作
     * @throws DbException
     * @throws TransactionAbortedException
     */
    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        hasAccessed = false;
        super.open();
        child.open();


    }

    public void close() {
        // some code goes here
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        hasAccessed = false;
    }

    /**
     * 把child insert一遍，同时返回一个tuple字段 ，就一个int value是添加了的个数<p></>
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     * @see Utility#getTuple(int[], int)
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (hasAccessed){
            return null;
        }
        count = 0;
        while (child.hasNext()){
            Tuple next = child.next();
            try {
                Database.getBufferPool().insertTuple(tid,tableId,next);
                count++;
            } catch (IOException e) {
                throw new DbException("添加错误，在inset操作发生IO异常");
            }
        }
        hasAccessed = true;

        return Utility.getTuple(new int[]{count},1);
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return this.children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.children = children;
    }
}
