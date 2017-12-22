package org.threadly.concurrent.wrapper.limiter;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.Callable;

import org.threadly.concurrent.AbstractPriorityScheduler;
import org.threadly.concurrent.PrioritySchedulerService;
import org.threadly.concurrent.RunnableRunnableContainer;
import org.threadly.concurrent.SchedulerService;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.future.ListenableFuture;

public class PrioritySchedulerLimiter extends SchedulerServiceLimiter 
                                      implements PrioritySchedulerService {
  protected final PrioritySchedulerVisibilityAccessor.RunnableRunnableContainerQueue queueManager;

  public PrioritySchedulerLimiter(PrioritySchedulerService scheduler, int maxConcurrency) {
    this(scheduler, scheduler.getDefaultPriority(), scheduler.getMaxWaitForLowPriority(), 
         maxConcurrency);
  }

  public PrioritySchedulerLimiter(PrioritySchedulerService scheduler, int maxConcurrency, 
                                  boolean limitFutureListenersExecution) {
    this(scheduler, scheduler.getDefaultPriority(), scheduler.getMaxWaitForLowPriority(), 
         maxConcurrency, limitFutureListenersExecution);
  }

  public PrioritySchedulerLimiter(SchedulerService scheduler, 
                                  TaskPriority defaultPriority, long maxWaitForLowPriorityInMs, 
                                  int maxConcurrency) {
    this(scheduler, defaultPriority, maxWaitForLowPriorityInMs, 
         maxConcurrency, DEFAULT_LIMIT_FUTURE_LISTENER_EXECUTION);
  }

  public PrioritySchedulerLimiter(SchedulerService scheduler, 
                                  TaskPriority defaultPriority, long maxWaitForLowPriorityInMs, 
                                  int maxConcurrency, boolean limitFutureListenersExecution) {
    super(scheduler, maxConcurrency, limitFutureListenersExecution);
  }

  @Override
  protected void addToQueue(RunnableRunnableContainer lrw) {
    // TODO - we may need to change how the task is added to the queue?
    throw new UnsupportedOperationException();
  }

  @Override
  public void execute(Runnable task, TaskPriority priority) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result, TaskPriority priority) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task, TaskPriority priority) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void schedule(Runnable task, long delayInMs, TaskPriority priority) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, long delayInMs,
                                                 TaskPriority priority) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs,
                                                 TaskPriority priority) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay,
                                     TaskPriority priority) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period,
                                  TaskPriority priority) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public TaskPriority getDefaultPriority() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long getMaxWaitForLowPriority() {
    return queueManager.getMaxWaitForLowPriority();
  }

  @Override
  public int getQueuedTaskCount(TaskPriority priority) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getWaitingForExecutionTaskCount(TaskPriority priority) {
    // TODO Auto-generated method stub
    return 0;
  }
  
  /**
   * This class serves to gain visibility into protected classes inside 
   * {@link AbstractPriorityScheduler}.  We do this rather than promoting their inner classes to 
   * avoid the javadoc pollution (and because right now this is a one off need).
   */
  protected static class PrioritySchedulerVisibilityAccessor extends AbstractPriorityScheduler {
    protected static class RunnableRunnableContainerQueue extends QueueManager 
                                                          implements Queue<RunnableRunnableContainer> {

      public RunnableRunnableContainerQueue(QueueSetListener queueSetListener, 
                                            long maxWaitForLowPriorityInMs) {
        super(queueSetListener, maxWaitForLowPriorityInMs);
      }

      @Override
      public boolean addAll(Collection<? extends RunnableRunnableContainer> c) {
        if (c == null || c.isEmpty()) {
          return false;
        }
        
        for (RunnableRunnableContainer rrc : c) {
          add(rrc);
        }
        return true;
      }

      @Override
      public boolean offer(RunnableRunnableContainer e) {
        add(e); // we always accept new items
        return true;
      }


      @Override
      public boolean add(RunnableRunnableContainer e) {
        // TODO Auto-generated method stub
        return true;
      }
      
      @Override
      public void clear() {
        super.clearQueue();
      }

      @Override
      public boolean isEmpty() {
        /* we need to slightly pervert this meaning.  Because normal limiters operate by only
         * ready to execute tasks in the queue, we must return false unless we have ready to 
         * execute tasks.
         * 
         * This is an advantage in that we don't need to additional burden the delegate pool for 
         * scheduling capabilities (just to add the task to the limit queue).  But is a confusing 
         * detail.
         */
        return ! super.hasTasksReadyToExecute();
      }

      @Override
      public int size() {
        /* Similar to isEmpty(), we need to report only the number of tasks currently waiting to 
         * execute. 
         */
        return highPriorityQueueSet.getWaitingForExecutionTaskCount() + 
                 lowPriorityQueueSet.getWaitingForExecutionTaskCount() + 
                 starvablePriorityQueueSet.getWaitingForExecutionTaskCount();
      }

      @Override
      public boolean removeAll(Collection<?> c) {
        if (c == null || c.isEmpty()) {
          return false;
        }
        
        for (Object o : c) {
          if (remove(o)) {
            return true;
          }
        }
        
        return false;
      }

      @Override
      public boolean remove(Object o) {
        if (o instanceof Runnable) {
          return super.remove((Runnable)o);
        } else if (o instanceof Callable) {
          return super.remove((Callable<?>)o);
        } else {
          // should not be possible, safer to throw than to return false
          throw new UnsupportedOperationException();
        }
      }

      @Override
      public RunnableRunnableContainer element() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public RunnableRunnableContainer peek() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public RunnableRunnableContainer poll() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public RunnableRunnableContainer remove() {
        // TODO Auto-generated method stub
        return null;
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

      @Override
      public Iterator<RunnableRunnableContainer> iterator() {
        throw new UnsupportedOperationException();  // not needed currently
      }
    }
    
    protected PrioritySchedulerVisibilityAccessor() {
      super(TaskPriority.High);
      throw new UnsupportedOperationException("Can not be constructed");
    }

    @Override
    public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay,
                                       TaskPriority priority) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void scheduleAtFixedRate(Runnable task, long initialDelay, long period,
                                    TaskPriority priority) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getActiveTaskCount() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isShutdown() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected OneTimeTaskWrapper doSchedule(Runnable task, long delayInMillis,
                                            TaskPriority priority) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected QueueManager getQueueManager() {
      throw new UnsupportedOperationException();
    }
  }
  
}
