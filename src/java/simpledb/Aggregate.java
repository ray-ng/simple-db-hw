package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private Aggregator result;
    private TupleDesc tupledesc;
    private OpIterator itr;
    private OpIterator[] children;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;

        Type gbFieldType;
        Type aFieldType = child.getTupleDesc().getFieldType(afield);

        if (gfield==Aggregator.NO_GROUPING) {
            gbFieldType = null;
        } else {
            gbFieldType = child.getTupleDesc().getFieldType(gfield);
        }

        if (aFieldType == Type.INT_TYPE) {
//            System.out.println("int--type");
            result = new IntegerAggregator(gfield, gbFieldType, afield, aop);
        } else if (aFieldType == Type.STRING_TYPE) {
//            System.out.println("string-type");
            result = new StringAggregator(gfield, gbFieldType, afield, aop);
        }

        if (gfield == Aggregator.NO_GROUPING) {
//            Type[] typeAr =  {child.getTupleDesc().getFieldType(afield)};
//            String[] fieldAr = {"aggName(" + aop.toString() + ")(" + child.getTupleDesc().getFieldName(afield) +")"};
//            this.tupledesc = new TupleDesc(typeAr, fieldAr);
            Type[] typeAr = {aFieldType};
            this.tupledesc = new TupleDesc(typeAr);
        }
        else {
//            Type[] typeAr =  {child.getTupleDesc().getFieldType(gfield), child.getTupleDesc().getFieldType(afield)};
//            String[] fieldAr = {"aggName(" + aop.toString() + ")(" + child.getTupleDesc().getFieldName(gfield) +")", "aggName(" + aop.toString() + ")(" + child.getTupleDesc().getFieldName(afield) +")"};
//            this.tupledesc = new TupleDesc(typeAr, fieldAr);
            Type[] typeAr =  {gbFieldType, aFieldType};
            this.tupledesc = new TupleDesc(typeAr);
        }

        this.children = new OpIterator[]{child};

//        if (gfield == Aggregator.NO_GROUPING) {
//            result = new IntegerAggregator(Aggregator.NO_GROUPING, null, afield, aop);
//        }
//        else if (child.getTupleDesc().getFieldType(gfield) == Type.INT_TYPE)
//            result = new IntegerAggregator(gfield, Type.INT_TYPE, afield, aop);
//        else if (child.getTupleDesc().getFieldType(gfield) == Type.STRING_TYPE)
//            result = new StringAggregator(gfield, Type.STRING_TYPE, afield, aop);

        try {
            child.open();
            while (child.hasNext()) {
//                System.out.println("here");
                result.mergeTupleIntoGroup(child.next());
            }
        }
        catch (DbException e) {
            e.printStackTrace();
        }
        catch (TransactionAbortedException e) {
            e.printStackTrace();
        }
        finally {
            child.close();
        }
        itr = result.iterator();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
	return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
        if (gfield == Aggregator.NO_GROUPING)
            return null;
	    return tupledesc.getFieldName(0);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
        if (gfield == Aggregator.NO_GROUPING)
            return tupledesc.getFieldName(0);
	return tupledesc.getFieldName(1);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
        super.open();
        itr.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
        if (itr.hasNext())
            return itr.next();
	return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
        itr.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
	return tupledesc;
    }

    public void close() {
	// some code goes here
        super.close();
        itr.close();
    }

    @Override
    public OpIterator[] getChildren() {
	// some code goes here
	return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
	// some code goes here
        this.children = children;
    }
    
}
