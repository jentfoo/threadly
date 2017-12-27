package org.threadly.concurrent.wrapper.limiter;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.Callable;

import org.threadly.concurrent.AbstractPriorityScheduler;
import org.threadly.concurrent.PrioritySchedulerService;
import org.threadly.concurrent.RunnableRunnableContainer;
import org.threadly.concurrent.SchedulerService;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.wrapper.PrioritySchedulerDefaultPriorityWrapper;

public class PrioritySchedulerLimiter extends SchedulerServiceLimiter 
                                      implements PrioritySchedulerService {
  protected final PriorityTaskQueue taskQueue;

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
    super(scheduler instanceof PrioritySchedulerService ? 
            PrioritySchedulerDefaultPriorityWrapper.wrapIfNecessary((PrioritySchedulerService)scheduler, 
                                                                    defaultPriority) : 
            scheduler, 
          maxConcurrency, limitFutureListenersExecution);
    
    this.taskQueue = new PriorityTaskQueue(defaultPriority, maxWaitForLowPriorityInMs);
  }
  
  @Override
  protected void executeOrQueueWrapper(LimiterRunnableWrapper lrw) {
    if (canRunTask()) {
      executor.execute(lrw);
    } else {
      addToQueue(lrw);
    }
  }

  @Override
  protected void addToQueue(RunnableRunnableContainer lrw) {
    addToQueue(lrw, taskQueue.getDefaultPriority());
  }
  
  protected void addToQueue(RunnableRunnableContainer lrw, TaskPriority priority) {
    taskQueue.doSchedule(lrw, 0, priority);
    // we don't need to consume available, the queue listener will do that if needed
  }

  @Override
  protected void doSchedule(Runnable task, ListenableFuture<?> future, long delayInMs) {
    doSchedule(task, future, delayInMs, taskQueue.getDefaultPriority());
  }
  
  /**
   * Adds a task to either execute (delay zero), or schedule with the provided delay.  No safety 
   * checks are done at this point, so only provide non-null inputs.
   * 
   * @param task Task for execution
   * @param delayInMs delay in milliseconds, greater than or equal to zero
   */
  protected void doSchedule(Runnable task, ListenableFuture<?> future, long delayInMs, 
                            TaskPriority priority) {
    if (delayInMs == 0) {
      executeOrQueue(task, future);
    } else {
      taskQueue.doSchedule(new DelayedExecutionRunnable(task), delayInMs, priority);
    }
  }

  @Override
  public void execute(Runnable task, TaskPriority priority) {
    taskQueue.execute(task, priority);
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result, TaskPriority priority) {
    return taskQueue.submit(task, result, priority);
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task, TaskPriority priority) {
    return taskQueue.submit(task, priority);
  }

  @Override
  public void schedule(Runnable task, long delayInMs, TaskPriority priority) {
    taskQueue.schedule(task, delayInMs, priority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, long delayInMs,
                                                 TaskPriority priority) {
    return taskQueue.submitScheduled(task, result, delayInMs, priority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs,
                                                 TaskPriority priority) {
    return taskQueue.submitScheduled(task, delayInMs, priority);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay,
                                     TaskPriority priority) {
    taskQueue.scheduleWithFixedDelay(task, initialDelay, recurringDelay, priority);
    
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period,
                                  TaskPriority priority) {
    taskQueue.scheduleAtFixedRate(task, initialDelay, period, priority);
  }

  @Override
  public TaskPriority getDefaultPriority() {
    return taskQueue.getDefaultPriority();
  }

  @Override
  public long getMaxWaitForLowPriority() {
    return taskQueue.getMaxWaitForLowPriority();
  }

  @Override
  public int getQueuedTaskCount(TaskPriority priority) {
    return taskQueue.getQueuedTaskCount(priority); // TODO - include parent scheduler
  }

  @Override
  public int getWaitingForExecutionTaskCount(TaskPriority priority) {
    return taskQueue.getWaitingForExecutionTaskCount(priority); // TODO - include parent scheduler
  }
  
  /**
   * This class serves to gain visibility into protected classes inside 
   * {@link AbstractPriorityScheduler}.  We do this rather than promoting their inner classes to 
   * avoid the javadoc pollution (and because right now this is a one off need).
   */
  protected class PriorityTaskQueue extends AbstractPriorityScheduler 
                                    implements Queue<RunnableRunnableContainer> {
    protected final QueueManager queueManager;
    
    protected PriorityTaskQueue(TaskPriority defaultPriority, long maxWaitForLowPriorityInMs) {
      super(defaultPriority);
      
      this.queueManager = new QueueManager(PrioritySchedulerLimiter.this::consumeAvailable, 
                                           maxWaitForLowPriorityInMs);
    }

    @Override
    public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay,
                                       TaskPriority priority) {
      // TODO
      throw new UnsupportedOperationException();
    }

    @Override
    public void scheduleAtFixedRate(Runnable task, long initialDelay, long period,
                                    TaskPriority priority) {
      // TODO
      throw new UnsupportedOperationException();
    }

    @Override
    protected OneTimeTaskWrapper doSchedule(Runnable task, long delayInMillis,
                                            TaskPriority priority) {
      throw new UnsupportedOperationException();
      // TODO - we need to wakeup and consume from the queue when ready
    }

    @Override
    protected QueueManager getQueueManager() {
      return queueManager;
    }

    @Override
    public boolean isShutdown() {
      return false;
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
      queueManager.clearQueue();
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
      return ! queueManager.hasTasksReadyToExecute();
    }

    @Override
    public int size() {
      /* Similar to isEmpty(), we need to report only the number of tasks currently waiting to 
       * execute. 
       */
      return getWaitingForExecutionTaskCount();
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
    public RunnableRunnableContainer peek() {
      return queueManager.getNextTask();
    }

    @Override
    public RunnableRunnableContainer element() {
      RunnableRunnableContainer result = peek();
      if (result == null) {
        throw new NoSuchElementException();
      }
      return result;
    }

    @Override
    public RunnableRunnableContainer poll() {
      while (true) {
        TaskWrapper nextTask = queueManager.getNextTask();
        if (nextTask != null) {
          // must get executeReference before time is checked
          short executeReference = nextTask.getExecuteReference();
          long taskDelay = nextTask.getScheduleDelay();
          if (taskDelay > 0) {
            if (taskDelay == Long.MAX_VALUE) {
              // concurrent modification, retry
              continue;
            } else {
              return null;  // we only return tasks that can execute
            }
          } else {
            if (nextTask.canExecute(executeReference)) {
              return nextTask;
            } else {
              // concurrent modification, retry
              continue;
            }
          }
        } else {
          return null;
        }
      }
    }

    @Override
    public RunnableRunnableContainer remove() {
      RunnableRunnableContainer result = poll();
      if (result == null) {
        throw new NoSuchElementException();
      }
      return result;
    }

    @Override
    public int getActiveTaskCount() {
      // This should not be called, we will just defer to parent scheduler
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
  
  protected class PriorityDelayedExecutionRunnable extends DelayedExecutionRunnable {
    private final TaskPriority priority;

    protected PriorityDelayedExecutionRunnable(Runnable runnable, TaskPriority priority) {
      super(runnable);
      
      this.priority = priority;
    }
    
    @Override
    public void run() {
      if (canRunTask()) {  // we can run in the thread we already have
        lrw.run();
      } else {
        addToQueue(lrw);  // TODO - execute with priority
      }
    }

    @Override
    public Runnable getContainedRunnable() {
      return lrw.getContainedRunnable();
    }
  }
}
