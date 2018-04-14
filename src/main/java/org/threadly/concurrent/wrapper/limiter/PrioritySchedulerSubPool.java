package org.threadly.concurrent.wrapper.limiter;

import java.util.concurrent.atomic.AtomicInteger;
import org.threadly.concurrent.AbstractPriorityScheduler;
import org.threadly.concurrent.SchedulerService;
import org.threadly.concurrent.TaskPriority;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;

/**
 * This sub-pool is a special type of limiter.  It is able to have expanded semantics than the pool 
 * it delegates to.  For example this pool provides {@link TaskPriority} capabilities even though 
 * the pool it runs on top of does not necessarily provide that.  In addition most status's returned 
 * do not consider the parent pools state (for example {@link #getActiveTaskCount()} does not 
 * reflect the active tasks in the parent pool).
 * <p>
 * Most importantly difference in this "sub-pool" vs "limiter" is the way task execution order is 
 * maintained in the delegate pool.  In a limiter tasks will need to queue individually against the 
 * other tasks the delegate pool needs to execute.  In this implementation the sub-pool basically 
 * gets CPU time and it will attempt to execute everything it needs to.  It will not return the 
 * thread to the delegate pool until there is nothing left to process.
 * <p>
 * There are two big reasons you might want to use this sub pool over a limiter.  As long as the 
 * above details are not problematic, this is a more efficient implementation.  Providing 
 * better load characteristics for submitting tasks, as well reducing the burden on the delegate 
 * pool.  In addition if you need concurrency limiting + priority capabilities, this or 
 * {@link SingleThreadSchedulerSubPool} are your only options.  If limiting to a single thread 
 * {@link SingleThreadSchedulerSubPool} is a higher performance option.
 * 
 * @since 5.16
 */
public class PrioritySchedulerSubPool extends AbstractPriorityScheduler {
  protected final DelegateExecutorWorkerPool workerPool;

  /**
   * Constructs a new sub-pool.
   * 
   * @param delegateScheduler The scheduler to perform task execution on
   * @param poolSize Thread pool size that should be maintained
   */
  public PrioritySchedulerSubPool(SchedulerService delegateScheduler, int poolSize) {
    this(delegateScheduler, poolSize, DEFAULT_PRIORITY, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS);
  }

  /**
   * Constructs a new sub-pool.  This provides the extra parameters to tune what tasks submitted 
   * without a priority will be scheduled as.  As well as the maximum wait for low priority tasks.
   * 
   * @param delegateScheduler The scheduler to perform task execution on
   * @param poolSize Thread pool size that should be maintained
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   */
  public PrioritySchedulerSubPool(SchedulerService delegateScheduler, int poolSize, 
                                  TaskPriority defaultPriority, long maxWaitForLowPriorityInMs) {
    this(new DelegateExecutorWorkerPool(delegateScheduler, poolSize, maxWaitForLowPriorityInMs), 
         defaultPriority);
  }
  
  /**
   * This constructor is designed for extending classes to be able to provide their own 
   * implementation of {@link DelegateExecutorWorkerPool}.  Ultimately all constructors will defer 
   * to this one.
   * 
   * @param workerPool WorkerPool to handle accepting tasks and providing them to a worker for execution
   * @param defaultPriority Default priority to store in case no priority is provided for tasks
   */
  protected PrioritySchedulerSubPool(DelegateExecutorWorkerPool workerPool, TaskPriority defaultPriority) {
    super(defaultPriority);
    
    this.workerPool = workerPool;
  }
  
  /**
   * Getter for the currently set max thread pool size.
   * 
   * @return current max pool size
   */
  public int getMaxPoolSize() {
    return workerPool.getMaxPoolSize();
  }
  
  /**
   * Change the set sub-pool size.
   * <p>
   * If the value is less than the current running workers, as workers finish they will exit 
   * rather than accept new tasks.  No currently running tasks will be interrupted, rather we 
   * will just wait for them to finish before killing the worker.
   * <p>
   * If this is an increase in the pool size, workers will be lazily started as needed till the 
   * new size is reached.  If there are tasks waiting for workers to run on, they immediately 
   * will be started.
   * 
   * @param newPoolSize New core pool size, must be at least one
   */
  public void setPoolSize(int newPoolSize) {
    workerPool.setPoolSize(newPoolSize);
  }

  /**
   * Adjust the pools size by a given delta.  If the provided delta would result in a pool size 
   * of zero or less, then a {@link IllegalStateException} will be thrown.
   * 
   * @param delta Delta to adjust the max pool size by
   */
  public void adjustPoolSize(int delta) {
    workerPool.adjustPoolSize(delta);
  }
  
  @Override
  public int getActiveTaskCount() {
    return workerPool.getCurrentPoolSize();
  }

  @Override
  public boolean isShutdown() {
    return workerPool.delegateScheduler.isShutdown();
  }

  @Override
  protected OneTimeTaskWrapper doSchedule(Runnable task, long delayInMillis, TaskPriority priority) {
    QueueSet queueSet = workerPool.queueManager.getQueueSet(priority);
    OneTimeTaskWrapper result;
    if (delayInMillis == 0) {
      result = new OneTimeTaskWrapper(task, queueSet.getExecuteQueue(), 
                                      Clock.lastKnownForwardProgressingMillis());
      queueSet.addExecute(result);
    } else {
      result = new OneTimeTaskWrapper(task, queueSet.getScheduleQueue(), 
                                      Clock.accurateForwardProgressingMillis() + 
                                        delayInMillis);
      queueSet.addScheduled(result);
    }
    return result;
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, 
                                     long recurringDelay, TaskPriority priority) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(initialDelay, "initialDelay");
    ArgumentVerifier.assertNotNegative(recurringDelay, "recurringDelay");
    if (priority == null) {
      priority = defaultPriority;
    }

    QueueSet queueSet = workerPool.queueManager.getQueueSet(priority);
    queueSet.addScheduled(new RecurringDelayTaskWrapper(task, queueSet, 
                                                        Clock.accurateForwardProgressingMillis() + 
                                                          initialDelay, 
                                                        recurringDelay));
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period, 
                                  TaskPriority priority) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(initialDelay, "initialDelay");
    ArgumentVerifier.assertGreaterThanZero(period, "period");
    if (priority == null) {
      priority = defaultPriority;
    }

    QueueSet queueSet = workerPool.queueManager.getQueueSet(priority);
    queueSet.addScheduled(new RecurringRateTaskWrapper(task, queueSet, 
                                                       Clock.accurateForwardProgressingMillis() + initialDelay, 
                                                       period));
  }

  @Override
  protected QueueManager getQueueManager() {
    return workerPool.queueManager;
  }
  
  /**
   * Class to manage the pool of workers that are running on a delegate pool.
   * 
   * @since 5.16 
   */
  protected static class DelegateExecutorWorkerPool implements QueueSetListener {
    protected final SchedulerService delegateScheduler;
    protected final QueueManager queueManager;
    protected final Object poolSizeChangeLock;
    protected final AtomicInteger currentPoolSize;
    private volatile int maxPoolSize;  // can only be changed when poolSizeChangeLock locked
    
    public DelegateExecutorWorkerPool(SchedulerService delegateScheduler, int poolSize, 
                                      long maxWaitForLowPriorityInMs) {
      ArgumentVerifier.assertNotNull(delegateScheduler, "delegateScheduler");
      ArgumentVerifier.assertGreaterThanZero(poolSize, "poolSize");
      
      poolSizeChangeLock = new Object();
      currentPoolSize = new AtomicInteger(0);
      
      this.delegateScheduler = delegateScheduler;
      queueManager = new QueueManager(this, maxWaitForLowPriorityInMs);
      this.maxPoolSize = poolSize;
    }

    /**
     * Getter for the currently set max worker pool size.
     * 
     * @return current max pool size
     */
    public int getMaxPoolSize() {
      return maxPoolSize;
    }

    /**
     * Change the set core pool size.  If the value is less than the current max pool size, the max 
     * pool size will also be updated to this value.
     * <p>
     * If this was a reduction from the previous value, this call will examine idle workers to see 
     * if they should be expired.  If this call reduced the max pool size, and the current running 
     * thread count is higher than the new max size, this call will NOT block till the pool is 
     * reduced.  Instead as those workers complete, they will clean up on their own.
     * 
     * @param newPoolSize New core pool size, must be at least one
     */
    public void setPoolSize(int newPoolSize) {
      ArgumentVerifier.assertGreaterThanZero(newPoolSize, "newPoolSize");
      
      if (newPoolSize == maxPoolSize) {
        // short cut the lock
        return;
      }
      
      int poolSizeIncrease;
      synchronized (poolSizeChangeLock) {
        poolSizeIncrease = newPoolSize - this.maxPoolSize;
        
        this.maxPoolSize = newPoolSize;
      }

      for (int i = 0; i < poolSizeIncrease; i++) {
        handleQueueUpdate(false);
      }
    }
    
    /**
     * Adjust the pools size by a given delta.  If the provided delta would result in a pool size 
     * of zero or less, then a {@link IllegalStateException} will be thrown.
     * 
     * @param delta Delta to adjust the max pool size by
     */
    public void adjustPoolSize(int delta) {
      if (delta == 0) {
        return;
      }
      
      synchronized (poolSizeChangeLock) {
        if (maxPoolSize + delta < 1) {
          throw new IllegalStateException(maxPoolSize + "" + delta + " must be at least 1");
        }
        this.maxPoolSize += delta;
      }
      
      for (int i = 0; i < delta; i++) {
        // now that pool size increased, start a worker so workers we can for the waiting tasks
        handleQueueUpdate(false);
      }
    }
    
    /**
     * Check for the current quantity of threads running in this pool (either active or idle).
     * 
     * @return current thread count
     */
    public int getCurrentPoolSize() {
      return currentPoolSize.get();
    }

    /**
     * Invoked when a worker becomes idle.  This will provide another task for that worker, or 
     * block until a task is either ready, or the worker should be shutdown (either because pool 
     * was shut down, or max pool size changed).
     * 
     * @return Task that is ready for immediate execution
     */
    public TaskWrapper workerIdle() {
      /* pool state checks, if any of these change we need a dummy task added to the queue to 
       * break out of the task polling loop below.  This is done as an optimization, to avoid 
       * needing to check these on every loop (since they rarely change)
       */
      while (true) {
        int casPoolSize;
        if ((casPoolSize = currentPoolSize.get()) > maxPoolSize) {
          if (currentPoolSize.compareAndSet(casPoolSize, casPoolSize - 1)) {
            return null;  // return null so worker will shutdown
          }
        } else {
          // pool state is consistent, we should keep running
          break;
        }
      }
      
      try {
        while (true) {
          TaskWrapper nextTask = queueManager.getNextTask();
          if (nextTask == null) {
            return null;  // let worker shutdown
          } else {
            /* TODO - right now this has a a deficiency where a recurring period task can cut in 
             * the queue line.  The condition would be as follows:
             * 
             * * Thread 1 gets task to run...task is behind execution schedule, likely due to large queue
             * * Thread 2 gets same task
             * * Thread 1 gets reference, executes, task execution completes
             * * Thread 2 now gets the reference, and execution check and time check pass fine
             * * End result is that task has executed twice (on expected schedule), the second 
             *     execution was unfair since it was done without respects to queue order and 
             *     other tasks which are also likely behind execution schedule in this example
             *     
             * This should be very rare, but is possible.  The only way I see to solve this right 
             * now is to introduce locking.
             * 
             * This is the same problem that can exist in the PriorityScheduler this class was based off of
             */
            // must get executeReference before time is checked
            short executeReference = nextTask.getExecuteReference();
            long taskDelay = nextTask.getScheduleDelay();
            if (taskDelay > 0) {
              if (taskDelay < Long.MAX_VALUE) {
                // TODO - is it best to schedule here, or at task submission?
                delegateScheduler.schedule(() -> handleQueueUpdate(true), taskDelay);
              }
              return null;  // let worker shutdown
            } else if (nextTask.canExecute(executeReference)) {
              return nextTask;
            }
          }
        } // end pollTask loop
      } finally {
        Thread.interrupted();  // reset interrupted status if set
      }
    }

    @Override
    public void handleQueueUpdate() {
      handleQueueUpdate(false);
    }
    
    protected void handleQueueUpdate(boolean canRunOnThread) {
      int casSize;
      while ((casSize = currentPoolSize.get()) < maxPoolSize) {
        if (currentPoolSize.compareAndSet(casSize, casSize + 1)) {
          // start a new worker for the next task
          if (canRunOnThread) {
            new Worker(this).run();
          } else {
            delegateScheduler.execute(new Worker(this));
          }
          break;
        } // else loop and retry logic
      }
    }
  }
  
  /**
   * Runnable which tries to poll tasks and execute them
   * 
   * @since 5.16
   */
  protected static class Worker implements Runnable {
    protected final DelegateExecutorWorkerPool workerPool;
    
    protected Worker(DelegateExecutorWorkerPool workerPool) {
      this.workerPool = workerPool;
    }
    
    @Override
    public void run() {
      try {
        TaskWrapper nextTask;
        while ((nextTask = workerPool.workerIdle()) != null) {
          nextTask.runTask();
        }
      } finally {
        workerPool.currentPoolSize.decrementAndGet();
      }
    }
  }
}
