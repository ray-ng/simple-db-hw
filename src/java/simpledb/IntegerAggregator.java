package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private TupleDesc tupledesc;

    private ArrayList<Tuple> mergelist;
    private HashMap<Integer, Integer> gbval2idx;
    private HashMap<Integer, Integer> gbval2cnt;

    private class itr implements OpIterator {

        private Iterator<Tuple> groupitr = null;

        public void open() throws DbException, TransactionAbortedException {
            groupitr = mergelist.iterator();
        }

        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (groupitr == null)
                throw new IllegalStateException("haven't opened!");
            return groupitr.hasNext();
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (groupitr == null)
                throw new IllegalStateException("haven't opened!");
            if (!groupitr.hasNext())
                throw new NoSuchElementException("no more tuples!");
            return groupitr.next();
        }

        public void rewind() throws DbException, TransactionAbortedException {
            if (groupitr == null)
                throw new IllegalStateException("haven't opened!");
            groupitr = mergelist.iterator();
        }

        public TupleDesc getTupleDesc() {
            return tupledesc;
        }

        public void close() {
            groupitr = null;
        }
    }

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
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.mergelist = new ArrayList<Tuple>();
        this.gbval2idx = new HashMap<Integer, Integer>();
        this.gbval2cnt = new HashMap<Integer, Integer>();
        Type[] typeAr1 = {Type.INT_TYPE};
        Type[] typeAr2 = {Type.INT_TYPE, Type.INT_TYPE};
        if (gbfieldtype != null)
            tupledesc = new TupleDesc(typeAr2);
        else
            tupledesc = new TupleDesc(typeAr1);
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
        if (gbfieldtype != null) {
            Field avalfield = tup.getField(afield);
            Field gbvalfield = tup.getField(gbfield);
            int gbval = gbvalfield.hashCode();
            int aval = avalfield.hashCode();
            Integer idx = gbval2idx.get(gbval);
            Integer cnt = gbval2cnt.get(gbval);
            if (idx == null) {
                Tuple temp = new Tuple(tupledesc);
                temp.setField(0, gbvalfield);
                temp.setField(1, avalfield);
                mergelist.add(temp);
                gbval2idx.put(gbval, mergelist.size()-1);
                gbval2cnt.put(gbval, 1);
            }
            else {
                Tuple idxtuple = mergelist.get(idx);
                int tempaval = idxtuple.getField(1).hashCode();
                switch (what) {
                    case MIN:
                        if (aval < tempaval) {
                            idxtuple.setField(1, avalfield);
                        }
                        break;
                    case MAX:
                        if (aval > tempaval) {
                            idxtuple.setField(1, avalfield);
                        }
                        break;
                    case SUM:
                        idxtuple.setField(1, new IntField(tempaval + aval));
                        gbval2cnt.put(gbval, cnt+1);
                        break;
                    case AVG:
                        idxtuple.setField(1, new IntField((tempaval*cnt+aval)/(cnt+1)));
                        gbval2cnt.put(gbval, cnt+1);
                    case COUNT:
                        idxtuple.setField(1, new IntField(tempaval+1));
                        gbval2cnt.put(gbval, cnt+1);
                }
            }
        }
        else {
            Field avalfield = tup.getField(afield);
            int aval = avalfield.hashCode();
            Integer idx = gbval2idx.get(0);
            Integer cnt = gbval2cnt.get(0);
            if (idx == null) {
                Tuple temp = new Tuple(tupledesc);
                temp.setField(0, avalfield);
                mergelist.add(temp);
                gbval2idx.put(0, mergelist.size()-1);
                gbval2cnt.put(0, 1);
            }
            else {
                Tuple idxtuple = mergelist.get(idx);
                int tempaval = idxtuple.getField(0).hashCode();
                switch (what) {
                    case MIN:
                        if (aval < tempaval) {
                            idxtuple.setField(0, avalfield);
                        }
                        break;
                    case MAX:
                        if (aval > tempaval) {
                            idxtuple.setField(0, avalfield);
                        }
                        break;
                    case SUM:
                        idxtuple.setField(0, new IntField(tempaval + aval));
                        gbval2cnt.put(0, cnt+1);
                        break;
                    case AVG:
                        idxtuple.setField(0, new IntField((tempaval*cnt+aval)/(cnt+1)));
                        gbval2cnt.put(0, cnt+1);
                    case COUNT:
                        idxtuple.setField(0, new IntField(tempaval+1));
                        gbval2cnt.put(0, cnt+1);
                }
            }
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
//        throw new
//        UnsupportedOperationException("please implement me for lab2");
        return new itr();
    }

}
