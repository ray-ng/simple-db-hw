package simpledb;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int numPages;
    private Object bplock;
    private ConcurrentHashMap<PageId, Page> bp;
    private int bpcap;
//    private class pageitem {
//        public TransactionId txnid;
//        public PageId pgid;
//        public Permissions perms;
//        public Page pagefile;
//        public pageitem(TransactionId txnid, PageId pgid, Permissions perms, Page pagefile) {
//            this.txnid = txnid;
//            this.pgid = pgid;
//            this.perms = perms;
//            this.pagefile = pagefile;
//        }
//    }

//    private pageitem[] pagearray;
//    private Stack<Integer> valididx;
//    private LinkedList<Integer> invalididx;
//    private ConcurrentLinkedQueue<Page> mempages;
    private ConcurrentHashMap<TransactionId, Set<PageId>> editedpagesoftxn;
    private LockUtil lockutil;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
//        pagearray = new pageitem[numPages];
//        pageid2index = new LinkedHashMap<PageId, Integer>();
////        txnid2index = new LinkedHashMap<TransactionId, LinkedList<Integer> >();
//        valididx = new Stack<Integer>();
//        for (int i = 0; i < numPages; ++i) {
//            valididx.push(i);
//        }
//        invalididx = new LinkedList<Integer>();
        this.bplock = new Object();
        this.bp = new ConcurrentHashMap<PageId, Page>();
        this.bpcap = 0;
//        this.mempages = new ConcurrentLinkedQueue<Page>();
        this.editedpagesoftxn = new ConcurrentHashMap<TransactionId, Set<PageId>>();
        this.lockutil = new LockUtil();
//        txnpages = new Object[numPages];
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    private Set<PageId> geteditedpagesoftxn(TransactionId tid) {
        Set<PageId> pageset = new HashSet<PageId>();
        editedpagesoftxn.putIfAbsent(tid, pageset);
        return editedpagesoftxn.get(tid);
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        try {
            Page pagefile = null;
            Object pagelock = lockutil.AcquireLock(tid, pid, perm);
            if (perm == Permissions.READ_WRITE)
                geteditedpagesoftxn(tid).add(pid);

            pagefile = bp.get(pid);
            if (pagefile != null) {
//                System.out.println("here");
                return pagefile;
            }

            if (perm == Permissions.READ_WRITE) {
//                System.out.println("here");
                DbFile databasefile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                pagefile = databasefile.readPage(pid);
            } else {
                synchronized (pagelock) {
                    if ((pagefile = bp.get(pid)) == null) {
                        DbFile databasefile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                        pagefile = databasefile.readPage(pid);
                        synchronized (bplock) {
                            if (bpcap < numPages) {
                                bp.put(pid, pagefile);
                                ++bpcap;
                            } else {
                                evictPage();
                                bp.put(pid, pagefile);
                            }
                        }
                    }
                }
                return pagefile;
            }

            if (pagefile == null) {
                pagefile = new HeapPage((HeapPageId) pid, HeapPage.createEmptyPageData());
            }

            synchronized (bplock) {
                if (bpcap < numPages) {
                    bp.put(pid, pagefile);
                    ++bpcap;
                } else {
                    evictPage();
                    bp.put(pid, pagefile);
                }
            }

            return pagefile;
        }
        catch (IOException e) {
            throw new DbException("IOException");
        }
        catch (InterruptedException e) {
            throw new DbException("InterruptedException");
        }
//        Integer idx = pageid2index.get(pid);
//        if (idx == null) {
//            DbFile databasefile = Database.getCatalog().getDatabaseFile(pid.getTableId());
//            pagefile = databasefile.readPage(pid);
//            if (!valididx.isEmpty()) {
//                Integer tempidx = valididx.pop();
//                invalididx.add(tempidx);
//                pageid2index.put(pid, tempidx);
//                pagearray[tempidx] = new pageitem(tid, pid, perm, pagefile);
//            }
//            else {
//                evictPage();
//                if (!valididx.isEmpty()) {
//                    Integer tempidx = valididx.pop();
//                    invalididx.add(tempidx);
//                    pageid2index.put(pid, tempidx);
//                    pagearray[tempidx] = new pageitem(tid, pid, perm, pagefile);
//                }
//            }
//        }
//        else {
//            pageitem temp = pagearray[idx];
//            pagefile = temp.pagefile;
//        }
//
//        return pagefile;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockutil.releaselock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
//        return getpagesoftxn(tid).contains(p);
        return lockutil.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit)
            flushPages(tid);
        synchronized (bplock) {
            for (PageId pid : geteditedpagesoftxn(tid)) {
                bp.remove(pid);
                --bpcap;
            }
        }
        editedpagesoftxn.remove(tid);
        lockutil.releaselocks(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile databasefile = Database.getCatalog().getDatabaseFile(tableId);
        databasefile.insertTuple(tid, t);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile databasefile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        databasefile.deleteTuple(tid, t);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
//        for (int i = 0; i < numPages; ++i) {
//            if (pagearray[i] != null) {
//                PageId pid = pagearray[i].pgid;
//                valididx.push(i);
//                invalididx.remove(i);
//                pagearray[i] = null;
//                pageid2index.put(pid, null);
//            }
//        }
//        System.out.println(bp.size());
        for (Page p : bp.values()) {
            if (p != null && p.isDirty() != null) {
                flushPage(p.getId());
            }
        }
        bp.clear();
        bpcap = 0;
        editedpagesoftxn.clear();
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
//        Integer idx = pageid2index.get(pid);
//        valididx.push(idx);
//        invalididx.remove(idx);
//        pagearray[idx] = null;
//        pageid2index.put(pid, null);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
//        Integer idx = pageid2index.get(pid);
//        if (idx != null) {
//            DbFile databasefile = Database.getCatalog().getDatabaseFile(pid.getTableId());
//            Page pagefile = databasefile.readPage(pid);
//            if (pagefile.isDirty() != null) {
//                databasefile.writePage(pagefile);
//                pagefile.markDirty(false, null);
//            }
//        }
        if (bp.get(pid) != null) {
            if (bp.get(pid).isDirty() != null) {
                DbFile pagefile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                pagefile.writePage(bp.get(pid));
            }
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pid : geteditedpagesoftxn(tid)) {
            flushPage(pid);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException, IOException {
        // some code goes here
        // not necessary for lab1
//        Integer idx = invalididx.getFirst();
//        PageId pageid = pagearray[idx].pgid;
//        try {
//            flushPage(pageid);
//            discardPage(pageid);
//        }
//        catch (IOException e) {
//            e.printStackTrace();
//        }
        for (Page p : bp.values()) {
            if (p != null && p.isDirty() != null) {
                continue;
            }
//            flushPage(p.getId());
            bp.remove(p.getId());
            return;
        }
        throw new DbException("no enough space");
    }

}
