package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 * 表示基于单个整数字段的固定宽度直方图的类
 */
public class IntHistogram {
    /**
     * bucket的数量
     */
    private int numBuckets;

    private int min;

    private int max;

    private int width;

    private int lastBucketWidth;

    private int[] buckets;

    private int ntups;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.numBuckets = buckets;
        this.min = min;
        this.max = max;

        this.width = (max - min +1)/numBuckets;

        if (this.width == 0){
            this.width = 1;
        }else if ((max-min+1) % numBuckets != 0 ){
            this.numBuckets += 1;
        }
        this.lastBucketWidth = max - (min +(numBuckets -1)*this.width)+1;
        this.buckets = new int[this.numBuckets];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int pos = (v - this.min) / this.width;
        buckets[pos] += 1;
        ntups += 1;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        /**
         * 先把 不在范围内的条件判断出去
         */
        if (op == Predicate.Op.EQUALS && (v < this.min || v > this.max)) return 0.0;
        if (op == Predicate.Op.NOT_EQUALS && (v < this.min || v > this.max)) return 1.0;
        if ((op == Predicate.Op.GREATER_THAN && v >= this.max) || (op == Predicate.Op.LESS_THAN && v <= this.min)) return 0.0;
        if ((op == Predicate.Op.GREATER_THAN_OR_EQ && v > this.max) || (op == Predicate.Op.LESS_THAN_OR_EQ && v < this.min)) return 0.0;
        if ((op == Predicate.Op.GREATER_THAN && v < this.min) || (op == Predicate.Op.LESS_THAN && v > this.max)) return 1.0;
        if ((op == Predicate.Op.GREATER_THAN_OR_EQ && v <= this.min) || (op == Predicate.Op.LESS_THAN_OR_EQ && v >= this.max)) return 1.0;

        /**
         * 剩下的结果 min <= v <= max
         */
        int pos = (v - this.min) / this.width;
        int bucket_beforeone_right = this.min + pos* this.width;
        double numQualifiedTup = 0.0;
        int curBucketWidth = this.width;
        if (pos == numBuckets -1){
            curBucketWidth = this.lastBucketWidth;
        }

        if (op == Predicate.Op.EQUALS) {
            numQualifiedTup = (1.0 / curBucketWidth) * buckets[pos];
        } else if (op == Predicate.Op.NOT_EQUALS) {
            numQualifiedTup = ntups - (1.0 / curBucketWidth) * buckets[pos];
        }else if (op == Predicate.Op.GREATER_THAN || op == Predicate.Op.GREATER_THAN_OR_EQ) {
            for (int i = pos+1; i < buckets.length; ++i) {
                numQualifiedTup += buckets[i];
            }
            if (op == Predicate.Op.GREATER_THAN) {
                numQualifiedTup += ((double) (curBucketWidth - (v - bucket_beforeone_right)) / curBucketWidth) * buckets[pos];
            } else {
                numQualifiedTup += ((double) (curBucketWidth - (v - bucket_beforeone_right) + 1) / curBucketWidth) * buckets[pos];
            }
        } else {
            // less and lessequal
            for (int i = 0; i < pos; ++i) {
                numQualifiedTup += buckets[i];
            }
            if (op == Predicate.Op.LESS_THAN ) {
                numQualifiedTup += ((double) (v - bucket_beforeone_right) / curBucketWidth) * buckets[pos];
            } else {
                numQualifiedTup += ((double) (v - bucket_beforeone_right + 1) / curBucketWidth) * buckets[pos];
            }
        }
        return numQualifiedTup / ntups;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return buckets.toString();
    }
}
