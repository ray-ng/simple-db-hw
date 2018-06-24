package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentSkipListSet;

public class LockUtil {

//    private static LockUtil single = null;

    private final ConcurrentHashMap<PageId, Object> pagelock;
    private final ConcurrentHashMap<PageId, TransactionId> exclusivelocks;
    private final ConcurrentHashMap<PageId, Set<TransactionId>> sharedlocks;
    private final ConcurrentHashMap<TransactionId, Set<PageId>> pagesoftxn;
    private final HashMap<TransactionId, LinkedList<TransactionId>> lockdag;
    private final HashMap<TransactionId, Integer> indegree;
    private final HashMap<TransactionId, HashMap<PageId, LinkedList<TransactionId>>> prevertex;
    private final Object lockcheck;

//    private final Map<TransactionId, PageId> wishLock;


    public LockUtil() {
        pagelock = new ConcurrentHashMap<PageId, Object>();
        exclusivelocks = new ConcurrentHashMap<PageId, TransactionId>();
        sharedlocks = new ConcurrentHashMap<PageId, Set<TransactionId>>();
        pagesoftxn = new ConcurrentHashMap<TransactionId, Set<PageId>>();
        lockdag = new HashMap<TransactionId, LinkedList<TransactionId>>();
        indegree = new HashMap<TransactionId, Integer>();
        prevertex = new HashMap<TransactionId, HashMap<PageId, LinkedList<TransactionId>>>();
        lockcheck = new Object();

//        wishLock = new HashMap<>();
    }

//    public static LockUtil init() {
//        if (single == null) {
//            single = new LockUtil();
//        }
//        return single;
//    }

    public Object GetPageLock(PageId pid) {
        Object lock = new Object();
        pagelock.putIfAbsent(pid, lock);
        return pagelock.get(pid);
    }

    private Set<TransactionId> getsharedlocks(PageId pid) {
        HashSet<TransactionId> txnset = new HashSet<TransactionId>();
        sharedlocks.putIfAbsent(pid, txnset);
        return sharedlocks.get(pid);
    }

    private Set<PageId> getpageoftxn(TransactionId tid) {
        Set<PageId> pageset = new HashSet<PageId>();
        pagesoftxn.putIfAbsent(tid, pageset);
        return pagesoftxn.get(tid);
    }

    public Object AcquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException, InterruptedException {
        if (perm == Permissions.READ_ONLY)
            return AcquireSharedLock(tid, pid);
        return AcquireExclusiveLock(tid, pid);
    }

    private Object AcquireSharedLock(TransactionId tid, PageId pid) throws InterruptedException, TransactionAbortedException {
        Object lock = GetPageLock(pid);

        synchronized (lock) {
            deadlockcheck(tid, pid, false);

            while (exclusivelocks.get(pid) != null && !exclusivelocks.get(pid).equals(tid)) {
//                try {
//                    lock.wait();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
                lock.wait();
            }

            if (exclusivelocks.get(pid) == null) {
                Set<TransactionId> txnset = getsharedlocks(pid);
                txnset.add(tid);
            }

            Set<PageId> pageoftxn = getpageoftxn(tid);
            pageoftxn.add(pid);
        }

        return lock;
    }

    private Object AcquireExclusiveLock(TransactionId tid, PageId pid) throws TransactionAbortedException, InterruptedException {
        Object lock = GetPageLock(pid);
        synchronized (lock) {
            deadlockcheck(tid, pid, true);

            while (exclusivelocks.get(pid) != null && !exclusivelocks.get(pid).equals(tid)) {
//                try {
//                    lock.wait();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
                lock.wait();
            }

            if (exclusivelocks.get(pid) != null && exclusivelocks.get(pid).equals(tid)) {
//                exclusivelocks.put(pid, tid);
//                Set<PageId> pageoftxn = getpageoftxn(tid);
//                pageoftxn.add(pid);
                return lock;
            }

            while (!getsharedlocks(pid).isEmpty() && !(getsharedlocks(pid).size() == 1 && getsharedlocks(pid).contains(tid))) {
                lock.wait();
            }

            if (getsharedlocks(pid).size() == 1 && getsharedlocks(pid).contains(tid)) {
                getsharedlocks(pid).clear();
                exclusivelocks.put(pid, tid);
                Set<PageId> pageoftxn = getpageoftxn(tid);
                pageoftxn.add(pid);
                return lock;
            }

            exclusivelocks.put(pid, tid);
            Set<PageId> pageoftxn = getpageoftxn(tid);
            pageoftxn.add(pid);
            return lock;
        }

    }

    public void releaselock(TransactionId tid, PageId pid) {
        Object lock = GetPageLock(pid);

        synchronized (lock) {
            editgraph(tid, pid);
            if (prevertex.get(tid) != null)
                prevertex.get(tid).remove(pid);

            if (exclusivelocks.get(pid) != null) {
                if (exclusivelocks.get(pid).equals(tid)) {
                    exclusivelocks.remove(pid);
                    lock.notify();
                }
                return;
            }

            Set<TransactionId> txnset = getsharedlocks(pid);
            txnset.remove(tid);

            Set<PageId> pageoftxn = getpageoftxn(tid);
            pageoftxn.remove(pid);

//            if (txnset.isEmpty())
            lock.notify();
        }
    }

    public void releaselocks(TransactionId tid) {
        Set<PageId> pageset = new HashSet<PageId>(getpageoftxn(tid));
        for (PageId pid : pageset)
            releaselock(tid, pid);
        indegree.remove(tid);
        lockdag.remove(tid);
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        return pagesoftxn.get(tid).contains(pid);
    }

    private LinkedList<TransactionId> getedge(TransactionId tid) {
        if (lockdag.get(tid) == null) {
            LinkedList<TransactionId> edgelist = new LinkedList<TransactionId>();
            lockdag.put(tid, edgelist);
        }
        return lockdag.get(tid);
    }

    private LinkedList<TransactionId> getvertex(TransactionId tid, PageId pid) {
        if (prevertex.get(tid) == null) {
            HashMap<PageId, LinkedList<TransactionId>> vertexlist = new HashMap<PageId, LinkedList<TransactionId>>();
            prevertex.put(tid, vertexlist);
        }

        if (prevertex.get(tid).get(pid) == null) {
            LinkedList<TransactionId> tidlist = new LinkedList<TransactionId>();
            prevertex.get(tid).put(pid, tidlist);
        }

        return prevertex.get(tid).get(pid);
    }

    private void deadlockcheck(TransactionId tid, PageId pid, boolean exclusive) throws TransactionAbortedException {
        synchronized (lockcheck) {
            indegree.putIfAbsent(tid, 0);
            HashMap<TransactionId, Integer> tempindegree = new HashMap<TransactionId, Integer>(indegree);
            int cnt = tempindegree.get(tid);

            LinkedList<TransactionId> lockholders;
            if (exclusive)
                lockholders = new LinkedList<TransactionId>(getsharedlocks(pid));
            else
                lockholders = new LinkedList<TransactionId>();
            if (exclusivelocks.get(pid) != null && !tid.equals(exclusivelocks.get(pid)))
                lockholders.add(exclusivelocks.get(pid));
            if (lockholders.size() == 0)
                return;

//        System.out.println(lockholders.size());

            for (TransactionId holder : lockholders) {
                if (holder == null)
                    continue;
                if (tid.equals(holder))
                    continue;
//            if (!getedge(holder).contains(tid)) {
                ++cnt;
                getedge(holder).addLast(tid);
                getvertex(holder, pid).addLast(tid);
//            }
            }

            tempindegree.put(tid, cnt);
//        if (tempindegree.get(tid) != null) {
//            int temp = tempindegree.get(tid);
//            ++temp;
//            tempindegree.put(tid, temp);
//        }
//        else
//            tempindegree.put(tid, 1);

            LinkedList<TransactionId> firstvertex = new LinkedList<TransactionId>();
            for (TransactionId t : tempindegree.keySet()) {
                if (tempindegree.get(t).intValue() == 0) {
                    firstvertex.addLast(t);
                }
            }
            while (!firstvertex.isEmpty()) {
                TransactionId t = firstvertex.pollFirst();
                tempindegree.remove(t);
                if (lockdag.get(t) != null) {
                    for (TransactionId temptxn : lockdag.get(t)) {
                        int temp = tempindegree.get(temptxn);
                        --temp;
                        if (temp == 0)
                            firstvertex.addLast(temptxn);
                        tempindegree.put(temptxn, temp);
                    }
                }
            }

            if (tempindegree.isEmpty()) {
//            if (indegree.get(tid) != null) {
//                int temp = indegree.get(tid);
//                ++temp;
//                indegree.put(tid, cnt);
//            }
//            else
//                indegree.put(tid, 1);
                indegree.put(tid, cnt);
            } else {
//            getedge(temptid).removeLast();
                for (TransactionId holder : lockholders) {
                    if (holder == null)
                        continue;
                    if (tid.equals(holder))
                        continue;
                    getedge(holder).removeLast();
                }
//            System.out.println("here");
                throw new TransactionAbortedException();
            }
        }
    }

    private void editgraph(TransactionId tid, PageId pid){
        synchronized (lockcheck) {
            LinkedList<TransactionId> tidlist = null;
//        int cnt = indegree.get(tid);
            if (prevertex.get(tid) != null)
                tidlist = prevertex.get(tid).get(pid);
            if (tidlist != null) {
                for (TransactionId t : tidlist) {
//                lockdag.get(t).remove(tid);
//                --cnt;
//                if (lockdag.get(t) != null)
                    lockdag.get(tid).removeFirstOccurrence(t);
                }
//            indegree.put(tid, cnt);
            }
            if (lockdag.get(tid) != null) {
                for (TransactionId t : lockdag.get(tid)) {
                    int temp = indegree.get(t);
                    --temp;
                    indegree.put(t, temp);
                }
            }
        }
    }

}
