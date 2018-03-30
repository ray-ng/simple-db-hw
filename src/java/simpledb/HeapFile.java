package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File storefile;
    private TupleDesc tuple_desc;
//    private int page_tuple_num;
//    private int page_header_size;
//    private int page_num;
    private int page_size;
//    private byte[] totalheader;
    private ArrayList<Page> modifiedpage;

    private class itr implements DbFileIterator {
        private TransactionId txnid;
        private Iterator<Tuple> fileitr;
        private int pageopened;

        public itr(TransactionId txnid) {
            this.txnid = txnid;
            this.fileitr = null;
            this.pageopened = 0;
        }

        public void open() throws DbException, TransactionAbortedException{
            PageId pageid = new HeapPageId(getId(), 0);
            HeapPage filepage = (HeapPage)Database.getBufferPool().getPage(txnid, pageid, Permissions.READ_ONLY);
            fileitr = filepage.iterator();
        }

        public boolean hasNext() throws DbException, TransactionAbortedException{
            if (fileitr == null)
                return false;
            if (fileitr.hasNext() || pageopened<numPages()-1)
                return true;
            return false;
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (fileitr == null)
                throw new NoSuchElementException();
            if (fileitr.hasNext())
                return fileitr.next();

            ++pageopened;
            PageId pageid = new HeapPageId(getId(), pageopened);
            HeapPage filepage = (HeapPage)Database.getBufferPool().getPage(txnid, pageid, Permissions.READ_ONLY);
            fileitr = filepage.iterator();

            return fileitr.next();
        }

        public void rewind() throws DbException, TransactionAbortedException {
            open();
            pageopened = 0;
        }

        public void close() {
            pageopened = 0;
            fileitr = null;
        }
    }

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        storefile = f;
        tuple_desc = td;
//        page_tuple_num = (BufferPool.getPageSize() * 8) / (tuple_desc.getSize() * 8 + 1);
//        page_header_size = (int)Math.ceil(page_tuple_num / 8);
        page_size = BufferPool.getPageSize();
//        page_num = (int)(storefile.length()/page_size);
//        if (storefile.length()%page_size != 0)
//            ++page_num;
//        totalheader = new byte[page_header_size * page_num];
        modifiedpage = new ArrayList<Page>();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return storefile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return storefile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return tuple_desc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pagenum = pid.getPageNumber();
        int pagesize = BufferPool.getPageSize();
        byte[] temp = new byte[pagesize];
        HeapPage heapPage = null;
        try {
            RandomAccessFile f = new RandomAccessFile(storefile, "r");
            f.seek(pagenum * pagesize);
            int cnt = 0;
            while (cnt != pagesize) {
                cnt += f.read(temp, cnt, pagesize-cnt);
            }
            heapPage = new HeapPage((HeapPageId)pid, temp);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return heapPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pagenum = page.getId().getPageNumber();
        HeapPage heappage = (HeapPage)page;
        byte[] temp = heappage.getPageData();
        int pagesize = BufferPool.getPageSize();
        try {
            RandomAccessFile f = new RandomAccessFile(storefile, "rw");
            f.seek(pagenum * pagesize);
            f.write(temp);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)(storefile.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
//        System.out.println(numPages());
        for (int i = 0; i < numPages(); ++i) {
//            System.out.println(i);
            PageId pageid = new HeapPageId(getId(), i);
            HeapPage filepage = (HeapPage) Database.getBufferPool().getPage(tid, pageid, Permissions.READ_WRITE);
//            System.out.println(filepage);
            if (filepage.getNumEmptySlots() > 0 ) {
                filepage.insertTuple(t);
                modifiedpage.add(filepage);
                return modifiedpage;
            }
        }
        HeapPageId hpid=new HeapPageId(getId(), numPages());
        HeapPage hp=new HeapPage(hpid, HeapPage.createEmptyPageData());
        hp.insertTuple(t);
        writePage(hp);
        modifiedpage.add(hp);
        return modifiedpage;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        PageId tuplepageid = t.getRecordId().getPageId();
        if (tuplepageid.getTableId() != getId())
            throw new DbException("not a member!");
        HeapPage tuplepage = (HeapPage) Database.getBufferPool().getPage(tid, tuplepageid, Permissions.READ_WRITE);
//        if (!tuplepage.isDirty().equals(tid))
//            throw new DbException("cannot be deleted!");
        tuplepage.deleteTuple(t);
        modifiedpage.add(tuplepage);
        return modifiedpage;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new itr(tid);

    }

}

