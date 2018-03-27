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
    private HashMap<String, Integer> gbval2idx;
    private HashMap<String, Integer> gbval2cnt;
    private HashMap<String, Integer> gbval2sum;

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
        this.gbval2idx = new HashMap<String, Integer>();
        this.gbval2cnt = new HashMap<String, Integer>();
        this.gbval2sum = new HashMap<String, Integer>();
        Type[] typeAr1 = {Type.INT_TYPE};
        Type[] typeAr2 = {gbfieldtype, Type.INT_TYPE};
        if (gbfield != Aggregator.NO_GROUPING)
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
        if (gbfield != Aggregator.NO_GROUPING) {
//            System.out.println("is--grouping");
            Field avalfield = tup.getField(afield);
            Field gbvalfield = tup.getField(gbfield);
            String gbval = gbvalfield.toString();
            int aval = avalfield.hashCode();
            Integer idx = gbval2idx.get(gbval);
            Integer cnt = gbval2cnt.get(gbval);
            if (idx != null) {
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
                        gbval2sum.put(gbval, tempaval + aval);
                        break;
                    case AVG:
                        int tempsum = gbval2sum.get(gbval);
                        gbval2sum.put(gbval, tempsum + aval);
//                        idxtuple.setField(1, new IntField((tempaval*cnt+aval)/(cnt+1)));
                        gbval2cnt.put(gbval, cnt+1);
                        idxtuple.setField(1, new IntField((tempsum+aval)/(cnt+1)));
                        break;
                    case COUNT:
                        idxtuple.setField(1, new IntField(tempaval+1));
                        gbval2cnt.put(gbval, cnt+1);
                        break;
                }
            }
            else {
                Tuple temp = new Tuple(tupledesc);
                temp.setField(0, gbvalfield);
                temp.setField(1, avalfield);
                if (what == Op.COUNT)
                    temp.setField(1, new IntField(1));
                mergelist.add(temp);
                gbval2idx.put(gbval, mergelist.size()-1);
                gbval2cnt.put(gbval, 1);
                gbval2sum.put(gbval, aval);
            }
        }
        else {
//            System.out.println("no--grouping");
            Field avalfield = tup.getField(afield);
            int aval = avalfield.hashCode();
            if (!mergelist.isEmpty()) {
                Tuple idxtuple = mergelist.get(0);
                int tempaval = idxtuple.getField(0).hashCode();
                int cnt = gbval2cnt.get("0");
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
                        gbval2cnt.put("0", cnt+1);
                        gbval2sum.put("0", tempaval + aval);
                        break;
                    case AVG:
//                        idxtuple.setField(0, new IntField((tempaval*cnt+aval)/(cnt+1)));
//                        gbval2cnt.put("0", cnt+1);
//                        break;
                        int tempsum = gbval2sum.get("0");
                        gbval2sum.put("0", tempsum + aval);
//                        idxtuple.setField(1, new IntField((tempaval*cnt+aval)/(cnt+1)));
                        gbval2cnt.put("0", cnt+1);
                        idxtuple.setField(0, new IntField((tempsum+aval)/(cnt+1)));
                        break;
                    case COUNT:
//                        idxtuple.setField(0, new IntField(tempaval+1));
//                        gbval2cnt.put("0", cnt+1);
//                        break;
                        idxtuple.setField(0, new IntField(tempaval+1));
                        gbval2cnt.put("0", cnt+1);
                        break;
                }
            }
            else {
                Tuple temp = new Tuple(tupledesc);
                temp.setField(0, avalfield);
                if (what == Op.COUNT)
                    temp.setField(0, new IntField(1));
                mergelist.add(temp);
                gbval2cnt.put("0", 1);
                gbval2sum.put("0", aval);
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
