package org.threadly.concurrent.collections;

import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class UnfairQueue<T> implements Collection<T> {
  protected ThreadLocal<ThreadLocalQueue> threadLocalQueue;
  protected Collection<WeakReference<ThreadLocalQueue>> allQueues;
  protected Queue<Queue<T>> deadQueues;
  
  public UnfairQueue() {
    allQueues = new ConcurrentLinkedQueue<>();
    deadQueues = new ConcurrentLinkedQueue<>();
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
    }
    tlq.queue.add(item);
    return true;
  }
  
  protected class ThreadLocalQueue {
    private final Queue<T> queue;
    
    public ThreadLocalQueue() {
      queue = new ConcurrentLinkedQueue<>();
      allQueues.add(new WeakReference<>(this));
    }
    
    @Override
    protected void finalize() throws Throwable {
      deadQueues.add(queue);
      super.finalize();
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
  public boolean remove(Object item) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    for (T item : c) {
      add(item);
    }
    return true;
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean contains(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isEmpty() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
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
