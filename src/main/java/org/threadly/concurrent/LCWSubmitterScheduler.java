package org.threadly.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.threadly.util.Clock;

public class LCWSubmitterScheduler extends AbstractSubmitterScheduler {
  private static final DelayedTask EMPTY_TASK = new DelayedTask(new Runnable() {
    @Override
    public void run() {
      // no-op, just used to wake up queue
    }
  });
  
  private final Thread[] threads;
  private final AtomicBoolean doingSchedule = new AtomicBoolean(false);
  private final LinkedBlockingDeque<DelayedTask> queue = new LinkedBlockingDeque<DelayedTask>();
  private final ArrayList<DelayedTask> delayQueue = new ArrayList<DelayedTask>();
  private final AtomicInteger delayQueueSize = new AtomicInteger(0);
  private volatile boolean running;
  
  public LCWSubmitterScheduler(int nThreads) {
    running = true;
    threads = new Thread[nThreads];
    for(int i=0; i<nThreads; i++) {
      Thread t = new Thread(new ThreadRunnable());
      t.setDaemon(true);
      t.start();
      threads[i] = t;
    }
  }
  
  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay) {
    DelayedTask dt = new DelayedTask(task, Clock.accurateForwardProgressingMillis() + initialDelay, 
                                     recurringDelay, true, true);
    addToDelayQueue(dt);
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period) {
    DelayedTask dt = new DelayedTask(task, Clock.accurateForwardProgressingMillis() + initialDelay, 
                                     period, true, false);
    addToDelayQueue(dt);
  }

  @Override
  protected void doSchedule(Runnable task, long delayInMillis) {
    if(delayInMillis <= 0L ) {
      queue.add(new DelayedTask(task));
    } else {
      DelayedTask dt = new DelayedTask(task, Clock.accurateForwardProgressingMillis() + delayInMillis, 
                                       delayInMillis, false, false);
      addToDelayQueue(dt);
    }
  }

  public void shutdown() {
    // TODO Auto-generated method stub
    
  }

  public List<Runnable> shutdownNow() {
    // TODO Auto-generated method stub
    return null;
  }
  
  private void addToDelayQueue(DelayedTask dt) {
    synchronized(delayQueue) {
      int size = delayQueue.size();
      if (size == 0) {
        delayQueue.add(dt);
        delayQueueSize.incrementAndGet();
        queue.add(EMPTY_TASK);
        return;
      } else if (dt.execTime >= delayQueue.get(size - 1).execTime) {
        delayQueue.add(dt);
        delayQueueSize.incrementAndGet();
        return;
      } else if (dt.execTime <= delayQueue.get(0).execTime){
        delayQueue.add(0, dt);
        delayQueueSize.incrementAndGet();
        queue.add(EMPTY_TASK);
        return;
      } else {
        int max = size-1;
        int cur = size/2;
        while(true) {
          if(dt.execTime < delayQueue.get(cur).execTime) {
            max = cur-1;
            cur = cur/2;
          } else if(dt.execTime > delayQueue.get(cur).execTime) {
            if(cur >= max) {
              delayQueue.add(cur+1, dt);
              delayQueueSize.incrementAndGet();
              return;
            } else {
              int diff = max - cur;
              cur = cur+(diff/2)+1;
            }
          } else {
            delayQueue.add(cur, dt);
            delayQueueSize.incrementAndGet();
            return;
          }
        }
      }
    }
  }
  
  private void processScheduled() {
    synchronized(delayQueue) {
      while (delayQueue.size() > 0 && delayQueue.get(0).getDelay(TimeUnit.MILLISECONDS) <= 0) {
        delayQueueSize.decrementAndGet();
        DelayedTask dt = delayQueue.remove(0);
        queue.add(dt);
        if (dt.recurring && ! dt.fixedDelay) {
          dt.execTime = Clock.accurateForwardProgressingMillis() + dt.delay;
          addToDelayQueue(dt);
        }
      }
    }
  }
  
  private long getNextDelayedTime() {
    synchronized(delayQueue) {
      if(delayQueueSize.get() > 0) {
        return delayQueue.get(0).getDelay(TimeUnit.MILLISECONDS);
      } else {
        return 5000;
      }
    }
  }
  
  private class ThreadRunnable implements Runnable {
    @Override
    public void run() {
      while(running) {
        try {
          long delayMillis = getNextDelayedTime();
          if (delayMillis <= 0 && doingSchedule.compareAndSet(false, true)) {
            processScheduled();
            doingSchedule.set(false);
            delayMillis = getNextDelayedTime();
          }
          DelayedTask dt = queue.pollFirst(delayMillis, TimeUnit.MILLISECONDS);
          if (dt != null) {
            try {
              dt.runner.run();
            }catch (Exception e) {
              e.printStackTrace();
            }
            if (dt.recurring && dt.fixedDelay) {
              dt.execTime = Clock.accurateForwardProgressingMillis()+dt.delay;
              addToDelayQueue(dt);
            }
          }
        } catch(Exception e) {
          // TODO - handle exception better
          e.printStackTrace();
        }
      }
    }
  }
  
  private static class DelayedTask extends AbstractDelayed {
    final Runnable runner;
    long execTime;
    long delay;
    boolean recurring;
    boolean fixedDelay;
    
    public DelayedTask(Runnable runner) {
      this.runner = runner;
    }
    
    public DelayedTask(Runnable runner, long execTime, long delay, boolean recurring, boolean fixedDelay) {
      this.runner = runner;
      this.execTime = execTime;
      this.delay = delay;
      this.recurring = recurring;
      this.fixedDelay = fixedDelay;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(execTime - ClockWrapper.getSemiAccurateMillis(), TimeUnit.MILLISECONDS);
    }
  }
}