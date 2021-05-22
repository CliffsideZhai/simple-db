package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield ;
    //private final int afield;
    private Type gbfieldtype;
    private Op op;

    private final TupleDesc tupleDesc;
    //private final Map<Field,Integer> vals;
    private final Map<Field,Integer> cnts;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT){
            throw new IllegalArgumentException();
        }
        this.gbfieldtype = gbfieldtype;
        this.gbfield = gbfield;
        //this.afield = afield;
        this.op = what;

        tupleDesc = (gbfield == Aggregator.NO_GROUPING)? new TupleDesc(new Type[]{Type.INT_TYPE}):
                new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE});
        //vals = new HashMap<Field,Integer>();
        cnts = new HashMap<Field,Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field key = (gbfield == NO_GROUPING)?null:tup.getField(gbfield);
        if (key!=null){
            if (cnts.containsKey(key)){
                cnts.put(key,cnts.get(key)+1);
            }else {
                cnts.put(key,1);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        //throw new UnsupportedOperationException("please implement me for lab2");
        return new OpIterator() {
            private Iterator<Field> child;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                child = cnts.keySet().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return child!=null && child.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                Tuple tuple = new Tuple(tupleDesc);
                Field next = child.next();
                if (1 == tupleDesc.numFields()){
                    tuple.setField(0,new IntField(cnts.get(next)));
                }else {
                    tuple.setField(0,next);
                    tuple.setField(1,new IntField(cnts.get(next)));
                }
                return tuple;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                child = cnts.keySet().iterator();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return tupleDesc;
            }

            @Override
            public void close() {
                child = null;
            }
        };
    }

}
