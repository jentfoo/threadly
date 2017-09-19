package org.threadly.concurrent.collections;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.threadly.util.Clock;

public class UnfairQueue<T> implements Collection<T> {
  protected Queue<T>[] queues;
  
  public UnfairQueue() {
    queues = new Queue[16];
    for (int i = 0; i < queues.length; i++) {
      queues[i] = new ConcurrentLinkedQueue<>();
    }
  }
  
  public T poll() {
    T result = null;
    for (Queue<T> q : queues) {
      result = q.poll();
      if (result != null) {
        return result;
      }
    }
    return null;
  }

  public T peek() {
    T result = null;
    for (Queue<T> q : queues) {
      result = q.peek();
      if (result != null) {
        return result;
      }
    }
    return null;
  }
  
  @Override
  public boolean add(T item) {
    queues[(int)((item.hashCode() ^ Clock.lastKnownTimeMillis()) % queues.length)].add(item);
    return true;
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    for (T item : c) {
      add(item);
    }
    return true;
  }

  @Override
  public boolean remove(Object o) {
    for (Queue<T> q : queues) {
      if (q.remove(o)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void clear() {
    for (Queue<T> q : queues) {
      q.clear();
    }
  }

  @Override
  public int size() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<T> iterator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean contains(Object o) {
    for (Queue<T> q : queues) {
      if (q.contains(o)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    for (Object o : c) {
      if (! contains(o)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean isEmpty() {
    for (Queue<T> q : queues) {
      if (! q.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    boolean result = false;
    for (Object o : c) {
      result |= remove(o);
    }
    return result;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException();
  }
}
