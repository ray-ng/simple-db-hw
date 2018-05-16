package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

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

    private int buckets;
    private int min;
    private int max;
    private int ntups = 0;
    private int gaplenth;
    private int[] numperbucket;
    private int[] lowerbound;
    private int[] upperbound;

    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.numperbucket = new int[buckets];
        this.lowerbound = new int[buckets];
        this.upperbound = new int[buckets];
        this.gaplenth = (max - min + 1) / buckets;
//        boundnode[0] = min;
//        boundnode[buckets] = max;
        for (int i = 0; i < buckets; i++) {
            lowerbound[i] = min + gaplenth * i;
            upperbound[i] = min + gaplenth - 1 + gaplenth * i;
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
//        System.out.println(v);
//        for (int i = 0; i < buckets; i++) {
//            if (v >= boundnode[i] && v < boundnode[i+1]) {
//                ++numperbucket[i];
//                return;
//            }
//        }
//        if (v == max)
//            ++numperbucket[buckets-1];
        if (v <= max && v >= min) {
            ++ntups;
            int idx = (v - min) / gaplenth;
            if (idx < buckets)
                ++numperbucket[idx];
            else
                ++numperbucket[buckets-1];
        }
//        else if (v == max)
//            ++numperbucket[buckets-1];
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
        double result = 0.0;
//        for ( ; i < buckets; i++) {
//            if (v >= boundnode[i] && v < boundnode[i+1]) {
        if (v < min) {
            switch (op) {
                case EQUALS:
                case LESS_THAN:
                case LESS_THAN_OR_EQ:
                    result = 0.0;
                    break;
                case GREATER_THAN:
                case GREATER_THAN_OR_EQ:
                case NOT_EQUALS:
                    result = 1.0;
                    break;
            }
        }
        else if (v <= max) {
            int i = (v - min) / gaplenth;
            i = (i < buckets) ? i : buckets - 1;
            switch (op) {
                case EQUALS:
                    result = (double) numperbucket[i] / (double) (gaplenth * ntups);
                    break;
                case GREATER_THAN:
                    result = (double) (upperbound[i] - v) * numperbucket[i] / (double) (gaplenth * ntups);
                    for (int j = i + 1; j < buckets; j++) {
                        result += ((double) (numperbucket[j]) / (double) ntups);
                    }
                    break;
                case LESS_THAN:
                    result = (double) (v - lowerbound[i]) * numperbucket[i] / (double) (gaplenth * ntups);
                    for (int j = i - 1; j >= 0; j--) {
                        result += ((double) (numperbucket[j]) / (double) ntups);
                    }
                    break;
                case LESS_THAN_OR_EQ:
                    result = (double) (v - lowerbound[i] + 1) * numperbucket[i] / (double) (gaplenth * ntups);
                    for (int j = i - 1; j >= 0; j--) {
                        result += ((double) (numperbucket[j]) / (double) ntups);
                    }
                    break;
                case GREATER_THAN_OR_EQ:
                    result = (double) (upperbound[i] - v + 1) * numperbucket[i] / (double) (gaplenth * ntups);
                    for (int j = i + 1; j < buckets; j++) {
                        result += ((double) (numperbucket[j]) / (double) ntups);
                    }
                    break;
                case NOT_EQUALS:
                    result = 1.0 - (double) numperbucket[i] / (double) (gaplenth * ntups);
                    break;
            }
        }
        else {
            switch (op) {
                case NOT_EQUALS:
                case LESS_THAN:
                case LESS_THAN_OR_EQ:
                    result = 1.0;
                    break;
                case GREATER_THAN:
                case GREATER_THAN_OR_EQ:
                case EQUALS:
                    result = 0.0;
                    break;
            }
        }
        return result;
//        return -1.0;
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
        return null;
    }
}
