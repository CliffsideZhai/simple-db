package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a query.
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {

        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {

        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * 表中的tuple个数
     */
    private int ntups;
    /**
     * 表中存在的属性fields 个数
     */
    private int nfields;
    /**
     * number pages;
     */
    private int npages;
    private int ioCostPerPage;

    private Object[] HisStats;

    private int[] maxs;
    private int[] mins;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.ioCostPerPage = ioCostPerPage;
        TransactionId tid = new TransactionId();
        SeqScan seqScan = new SeqScan(tid, tableid, "");

        this.nfields = seqScan.getTupleDesc().numFields();
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        this.npages = heapFile.numPages();
        /**
         * 获取每个filed的最大值，构成一个数组
         */
        maxs = new int[this.nfields];
        /**
         * 获取每个filed的最小值，构成一个数组
         */
        mins = new int[this.nfields];
        // unsafe should convert based on type
        /**
         * 每个filed对应的统计直方图结果
         */
        this.HisStats = new Object[this.nfields];

        for (int i = 0; i < nfields; i++) {
            maxs[i] = Integer.MIN_VALUE;
            mins[i] = Integer.MAX_VALUE;
        }

        Scan(seqScan);
    }

    private void Scan(SeqScan seqScan){
        try {
            seqScan.open();
            // scan and updating min and max for intField
            while (seqScan.hasNext()){
                this.ntups ++;
                Tuple next = seqScan.next();
                /** 遍历一遍tuple里的每个属性，如果出现有*/
                for (int i = 0; i < nfields; i++) {
                    if (next.getField(i).compare(Predicate.Op.GREATER_THAN,new IntField(maxs[i]))){
                        maxs[i] = ((IntField)(next.getField(i))).getValue();
                        //System.out.println("maxs update is"+maxs[i]);
                    }
                    if (next.getField(i).compare(Predicate.Op.LESS_THAN, new IntField(mins[i]))) {
                        mins[i] = ((IntField) next.getField(i)).getValue();
                        //System.out.println("minx update is"+ mins[i]);
                    }
                }
            }


            for (int i = 0; i < nfields; ++i) {
                if (seqScan.getTupleDesc().getFieldType(i) == Type.INT_TYPE) {
                    HisStats[i] = new IntHistogram(NUM_HIST_BINS, mins[i], maxs[i]);
                } else {
                    // String type
                    HisStats[i] = new StringHistogram(NUM_HIST_BINS);
                }
            }

            seqScan.rewind();

            /* 重新扫描一遍，给每个tuple的每个filed对应的直方图上增加数据 */
            while (seqScan.hasNext()) {
                Tuple next = seqScan.next();
                for (int i = 0; i < nfields; ++i) {
                    if (next.getField(i).getType() == Type.INT_TYPE) {
                        ((IntHistogram) HisStats[i]).addValue(((IntField) next.getField(i)).getValue());
                    } else {
                        // string type
                        ((StringHistogram) HisStats[i]).addValue(((StringField) next.getField(i)).getValue());
                    }
                }
            }
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }finally {
            seqScan.close();
        }

    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return npages * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (selectivityFactor * ntups);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        if (constant.getType() == Type.INT_TYPE){
            IntHistogram intHistogram = (IntHistogram)HisStats[field];
            double v = intHistogram.estimateSelectivity(op, ((IntField) constant).getValue());
            return v;
        }else {
            StringHistogram stringHistogram = (StringHistogram)HisStats[field];
            double v = stringHistogram.estimateSelectivity(op, ((StringField) constant).getValue());
            return v;
        }
        //return 1.0;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return ntups;
    }

}
