package org.threadly.concurrent.collections;

import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class UnfairQueue<T> implements Collection<T> {
  protected final ThreadLocal<ThreadLocalQueue> threadLocalQueue;
  protected final Queue<Queue<T>> deadQueues;
  protected final Collection<WeakReference<ThreadLocalQueue>> allQueues;
  
  public UnfairQueue() {
    threadLocalQueue = new ThreadLocal<>();
    deadQueues = new ConcurrentLinkedQueue<>();
    allQueues = new ConcurrentLinkedQueue<>();
  }
  
  public T poll() {
    T result = null;
    if (threadLocalQueue.get() != null) {
      result = threadLocalQueue.get().queue.poll();
    }
    if (result == null) {
      Iterator<Queue<T>> it = deadQueues.iterator();
      while (it.hasNext()) {
        result = it.next().poll();
        if (result == null) {
          it.remove();
        } else {
          break;
        }
      }
    }
    if (result == null) {
      Iterator<WeakReference<ThreadLocalQueue>> it = allQueues.iterator();
      while (it.hasNext()) {
        WeakReference<ThreadLocalQueue> wr = it.next();
        ThreadLocalQueue tlq = wr.get();
        if (tlq == null) {
          it.remove();
        } else {
          result = tlq.queue.poll();
          if (result != null) {
            break;
          }
        }
      }
    }
    return result;
  }

  public T peek() {
    T result = null;
    if (threadLocalQueue.get() != null) {
      result = threadLocalQueue.get().queue.peek();
    }
    if (result == null) {
      Iterator<Queue<T>> it = deadQueues.iterator();
      while (it.hasNext()) {
        result = it.next().poll();
        if (result == null) {
          it.remove();
        } else {
          break;
        }
      }
    }
    if (result == null) {
      Iterator<WeakReference<ThreadLocalQueue>> it = allQueues.iterator();
      while (it.hasNext()) {
        WeakReference<ThreadLocalQueue> wr = it.next();
        ThreadLocalQueue tlq = wr.get();
        if (tlq == null) {
          it.remove();
        } else {
          result = tlq.queue.peek();
          if (result != null) {
            break;
          }
        }
      }
    }
    return result;
  }
  
  @Override
  public boolean add(T item) {
    ThreadLocalQueue tlq = threadLocalQueue.get();
    if (tlq == null) {
      threadLocalQueue.set(tlq = new ThreadLocalQueue());
      allQueues.add(new WeakReference<>(tlq));
    }
    tlq.queue.add(item);
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
    Iterator<WeakReference<ThreadLocalQueue>> it = allQueues.iterator();
    while (it.hasNext()) {
      WeakReference<ThreadLocalQueue> wr = it.next();
      ThreadLocalQueue tlq = wr.get();
      if (tlq == null) {
        it.remove();
      } else if (tlq.queue.remove(o)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void clear() {
    Iterator<WeakReference<ThreadLocalQueue>> it = allQueues.iterator();
    while (it.hasNext()) {
      WeakReference<ThreadLocalQueue> wr = it.next();
      ThreadLocalQueue tlq = wr.get();
      if (tlq == null) {
        it.remove();
      } else {
        tlq.queue.clear();
      }
    }
  }

  @Override
  public int size() {
    int result = 0;
    Iterator<WeakReference<ThreadLocalQueue>> it = allQueues.iterator();
    while (it.hasNext()) {
      WeakReference<ThreadLocalQueue> wr = it.next();
      ThreadLocalQueue tlq = wr.get();
      if (tlq == null) {
        it.remove();
      } else {
        result += tlq.queue.size();
      }
    }
    return result;
  }

  @Override
  public Iterator<T> iterator() {
    // TODO
    return Collections.emptyIterator();
  }

  @Override
  public boolean contains(Object o) {
    Iterator<WeakReference<ThreadLocalQueue>> it = allQueues.iterator();
    while (it.hasNext()) {
      WeakReference<ThreadLocalQueue> wr = it.next();
      ThreadLocalQueue tlq = wr.get();
      if (tlq == null) {
        it.remove();
      } else if (tlq.queue.contains(o)) {
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
    Iterator<WeakReference<ThreadLocalQueue>> it = allQueues.iterator();
    while (it.hasNext()) {
      WeakReference<ThreadLocalQueue> wr = it.next();
      ThreadLocalQueue tlq = wr.get();
      if (tlq == null) {
        it.remove();
      } else if (! tlq.queue.isEmpty()) {
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
  
  protected class ThreadLocalQueue {
    private final Queue<T> queue = new ConcurrentLinkedQueue<>();
    
    @Override
    protected void finalize() throws Throwable {
      if (! queue.isEmpty()) {
        deadQueues.add(queue);
      }
      super.finalize();
    }
  }
}
