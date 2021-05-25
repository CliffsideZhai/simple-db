package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * 该field值指定了要使用tuple的哪一个列来分组
     */
    private int gbfield ;
    /**
     * 该field指定了要使用tuple的哪一个列来聚合
     */
    private int afield;

    //聚合前tuple的行描述
    TupleDesc originalTd;

    //聚合后的tuple的行描述
    TupleDesc td;

    //指定了作为分组依据的那一列的值的类型
    private Type gbfieldtype;

    //指定使用哪种聚合操作
    private Op op;

    //Key：每个不同的分组字段(groupby value)  Vlaue：聚合的结果
    private Map<Field,Integer> vals;
    //Key：每个不同的分组字段(groupby value)  Value：该分组进行平均值聚合过程处理的所有值的个数以及他们的和
    //这个map仅用于辅助在计算平均值时得到以前聚合过的总数
    private Map<Field,Integer[]> aveMap;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfieldtype = gbfieldtype;
        this.gbfield = gbfield;
        this.afield = afield;
        this.op = what;

        td = (gbfield == Aggregator.NO_GROUPING)? new TupleDesc(new Type[]{Type.INT_TYPE}):
                new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE});
        vals = new HashMap<>();
        aveMap = new HashMap<>();
    }

    /**
     * Aggregate constructor
     *
     * @param gbIndex     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbFieldType the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param agIndex     the 0-based index of the aggregate field in the tuple
     * @param aggreOp     the aggregation operator
     * @param td          我加上的一个参数，由聚合器的使用者(一般是Aggregate类)负责传入
     */

    public IntegerAggregator(int gbIndex, Type gbFieldType, int agIndex, Op aggreOp,TupleDesc td) {
        // some code goes here
        this.gbfield = gbIndex;
        this.gbfieldtype = gbFieldType;
        this.afield = agIndex;
        this.op = aggreOp;
        this.td=td;
        vals = new HashMap<>();
        aveMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        //待聚合值所在的Field
        Field aggreField;
        //分组依据的Field
        Field gbField = null;
        //新的聚合结果
        Integer newVal;
        aggreField = tup.getField(afield);
        //待聚合值
        int toAggregate;

        if (aggreField.getType() != Type.INT_TYPE) {
            throw new IllegalArgumentException("该tuple的指定列不是Type.INT_TYPE类型");
        }

        toAggregate = ((IntField) aggreField).getValue();

        //初始化originalTd，并确保每一次聚合的tuple的td与其相同
        if (originalTd == null) {
            originalTd = tup.getTupleDesc();
        } else if (!originalTd.equals(tup.getTupleDesc())) {
            throw new IllegalArgumentException("待聚合tuple的tupleDesc不一致");
        }

        if (gbfield != Aggregator.NO_GROUPING) {
            //如果gbIdex为NO_GROUPING，那么不用给gbField赋值，即为初始值null即可
            gbField = tup.getField(gbfield);
        }

        //开始进行聚合操作
        //平均值的操作需要维护cnts，所以单独处理
        if (op == Op.AVG) {
            if (aveMap.containsKey(gbField)) {//如果这个map已经处理过这个分组
                Integer[] oldCountAndSum = aveMap.get(gbField);//之前处理该分组的总次数以及所有操作数的和
                int oldCount = oldCountAndSum[0];
                int oldSum = oldCountAndSum[1];
                //更新该分组对应的记录，将次数加1,并将总和加上待聚合的值
                aveMap.put(gbField, new Integer[]{oldCount + 1, oldSum + toAggregate});
            } else {//否则为第一次处理该分组的tuple
                aveMap.put(gbField, new Integer[]{1, toAggregate});
            }
            //直接由ave这个map记录的信息得到该分组对应的聚合值并保存在gval2agval中
            Integer[] c2s=aveMap.get(gbField);
            int currentCount = c2s[0];
            int currentSum = c2s[1];
            vals.put(gbField, currentSum / currentCount);
            //在这里结束，此方法剩下的代码是对应除了求平均值其他的操作的
            return;
        }

        //除了求平均值的其他聚合操作
        if (vals.containsKey(gbField)) {
            Integer oldVal = vals.get(gbField);
            newVal = calcuNewValue(oldVal, toAggregate, op);
        } else if (op == Op.COUNT) {//如果是对应分组的第一个参加聚合操作的tuple，那么除了count操作，其他操作结果都是待聚合值
            newVal = 1;
        } else {
            newVal = toAggregate;
        }
        vals.put(gbField, newVal);
    }
    /**
     * 由旧的聚合结果和新的聚合值得到新的聚合结果
     *
     * @param oldVal      旧的聚合结果
     * @param toAggregate 新的聚合值
     * @param aggreOp     聚合操作
     * @return 新的聚合值
     */
    private int calcuNewValue(int oldVal, int toAggregate, Op aggreOp) {
        switch (aggreOp) {
            case COUNT:
                return oldVal + 1;
            case MAX:
                return Math.max(oldVal, toAggregate);
            case MIN:
                return Math.min(oldVal, toAggregate);
            case SUM:
                return oldVal + toAggregate;
            default:
                throw new IllegalArgumentException("不应该到达这里");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        //throw new
        //UnsupportedOperationException("please implement me for lab2");
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> g2a : vals.entrySet()) {
            Tuple t = new Tuple(td);//该tuple不必setRecordId，因为RecordId对进行操作后的tuple没有意义
            //分别处理不分组与有分组的情形
            if (gbfield == Aggregator.NO_GROUPING) {
                t.setField(0, new IntField(g2a.getValue()));
            } else {
                t.setField(0, g2a.getKey());
                t.setField(1, new IntField(g2a.getValue()));
            }
            tuples.add(t);
        }
        return new TupleIterator(td, tuples);
    }

}
