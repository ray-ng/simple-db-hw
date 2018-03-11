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
    private int page_tuple_num;
    private int page_header_size;
    private int page_num;
    private int page_size;
    private byte[] totalheader;

    private class itr implements DbFileIterator {
        private TransactionId txnid;
        private int cursor;
        private byte[] tempheader;

        public itr(TransactionId txnid) {
            this.txnid = txnid;
            cursor = 0;
            tempheader = null;
        }

        private boolean isSlotUsed(int i) {
            // some code goes here
            int headeridx = i / 8;
            int bitidx = i % 8;
            byte temp = totalheader[headeridx];
            return ((temp >> bitidx) & 1) == 1;
        }

        private PageId getpageid(int i) {
            int pagenum = i / page_tuple_num;
            int tableid = getId();
            return new HeapPageId(tableid, pagenum);
        }

        public void open() {
            try {
                byte[] temp = new byte[page_header_size];
                FileInputStream f = new FileInputStream(storefile);
                tempheader = totalheader;
                for (int i = 0; i < page_num; ++i) {
                    int cnt = 0;
                    while (cnt != page_header_size) {
                        cnt += f.read(temp, cnt, page_header_size-cnt);
                    }
                    System.arraycopy(temp, 0, tempheader, i*page_header_size, page_header_size);
                    f.skip(page_size-page_header_size);
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }

        public boolean hasNext() {
            if (tempheader == null)
                return false;
            while (cursor < totalheader.length && !isSlotUsed(cursor)) {
                ++cursor;
            }
            if (cursor < totalheader.length)
                return true;
            return false;
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException{
            if (tempheader == null)
                throw new NoSuchElementException("haven't opened!");
            if (cursor < totalheader.length) {
                try {
                    HeapPage temp = (HeapPage) Database.getBufferPool().getPage(txnid, getpageid(cursor), Permissions.READ_WRITE);
                    return temp.tuples[cursor++];
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            throw  new NoSuchElementException("no more tuple!");
        }

        public void rewind() {
            cursor = 0;
        }

        public void close() {
            tempheader = null;
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
        page_tuple_num = (BufferPool.getPageSize() * 8) / (tuple_desc.getSize() * 8 + 1);
        page_header_size = (int)Math.ceil(page_tuple_num / 8);
        page_size = BufferPool.getPageSize();
        page_num = (int)Math.ceil(storefile.length()/page_size);
        totalheader = new byte[page_header_size * page_num];
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
            FileInputStream f = new FileInputStream(storefile);
            f.skip(pagenum * pagesize);
            int cnt = 0;
            while (cnt != pagesize) {
                cnt += f.read(temp, cnt, pagesize-cnt);
            }
            heapPage = new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), temp);
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
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new itr(tid);
    }

}

