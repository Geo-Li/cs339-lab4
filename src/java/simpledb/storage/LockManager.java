package simpledb.storage;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import simpledb.transaction.*;

public class LockManager {	
	private class Lock {
		public TransactionId tid;
		public boolean lockType;
		
		public Lock(TransactionId tid, boolean lockType) {
			this.tid = tid;
			this.lockType = lockType;
		}
	}
	
	private ConcurrentHashMap<PageId, Vector<Lock>> lockMap;
	
	public LockManager() {
		lockMap = new ConcurrentHashMap<>();
	}
	
	public synchronized boolean acquireLock(PageId pid, TransactionId tid, boolean lockType) {
		if (!lockMap.containsKey(pid)) {
			Lock lock = new Lock(tid, lockType);
			Vector<Lock> locks = new Vector<>();
			locks.add(lock);
			lockMap.put(pid, locks);
			return true;
		}
		
		Vector<Lock> locks = lockMap.get(pid);
		for (Lock lock : locks) {
			if (lock.tid.equals(tid)) {
				if (lock.lockType == lockType) {
					return true;
				}
				if (lock.lockType) {
					return true;
				}
				if (locks.size() == 1) {
					lock.lockType = true;
					return true;
				} else {
					return false;
				}
			}
		}
		
		if (locks.size() == 1 && locks.get(0).lockType) {
			return false;
		}
		
		if (!lockType) {
			Lock lock = new Lock(tid, lockType);
			locks.add(lock);
			lockMap.put(pid, locks);
			return true;
		}
		return false;
	}
	
	public synchronized boolean releaseLock(PageId pid, TransactionId tid) {
		if (!lockMap.containsKey(pid)) {
			return false;
		}
		Vector<Lock> locks = lockMap.get(pid);
		
		for (Lock lock : locks) {			
			if (lock.tid.equals(tid)) {
				locks.remove(lock);
				if (locks.size() == 0) {
					lockMap.remove(pid);
				}
				return true;
			}
		}
		return false;
	}
	
	public synchronized boolean holdsLock(PageId pid, TransactionId tid) {
		if (lockMap.get(pid) == null) {
			return false;
		}
		Vector<Lock> locks = lockMap.get(pid);
		
		for (Lock lock : locks) {
			if (lock.tid.equals(tid)) {
				return true;
			}
		}
		return false;
	}
}
