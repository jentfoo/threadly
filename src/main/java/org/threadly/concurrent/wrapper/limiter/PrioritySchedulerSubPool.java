package org.threadly.concurrent.wrapper.limiter;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import org.threadly.concurrent.AbstractPriorityScheduler;
import org.threadly.concurrent.ReschedulingOperation;
import org.threadly.concurrent.SchedulerService;
import org.threadly.concurrent.TaskPriority;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;
import org.threadly.util.ExceptionUtils;

/**
 * Executor to run tasks, schedule tasks.  Unlike 
 * {@link java.util.concurrent.ScheduledThreadPoolExecutor} this scheduled executor's pool size 
 * can shrink if set with a lower value via {@link #setPoolSize(int)}.  It also has the benefit 
 * that you can provide "low priority" tasks.
 * <p>
 * These low priority tasks will delay their execution if there are other high priority tasks 
 * ready to run, as long as they have not exceeded their maximum wait time.  If they have exceeded 
 * their maximum wait time, and high priority tasks delay time is less than the low priority delay 
 * time, then those low priority tasks will be executed.  What this results in is a task which has 
 * lower priority, but which wont be starved from execution.
 * <p>
 * Most tasks provided into this pool will likely want to be "high priority", to more closely 
 * match the behavior of other thread pools.  That is why unless specified by the constructor, the 
 * default {@link TaskPriority} is High.
 * <p>
 * In all conditions, "low priority" tasks will never be starved.  This makes "low priority" tasks 
 * ideal which do regular cleanup, or in general anything that must run, but cares little if there 
 * is a 1, or 10 second gap in the execution time.  That amount of tolerance is adjustable by 
 * setting the {@code maxWaitForLowPriorityInMs} either in the constructor, or at runtime via 
 * {@link #setMaxWaitForLowPriority(long)}.
 * 
 * @since 5.16
 */
public class PrioritySchedulerSubPool extends AbstractPriorityScheduler {
  protected final DelegateExecutorWorkerPool workerPool;
  protected final QueueManager taskQueueManager;
  
  public PrioritySchedulerSubPool(SchedulerService delegateScheduler, int poolSize) {
    this(delegateScheduler, poolSize, DEFAULT_PRIORITY, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS);
  }

  /**
   * Constructs a new thread pool, though threads will be lazily started as it has tasks ready to 
   * run.  This provides the extra parameters to tune what tasks submitted without a priority 
   * will be scheduled as.  As well as the maximum wait for low priority tasks.
   * 
   * @param delegateScheduler The scheduler to perform task execution on
   * @param poolSize Thread pool size that should be maintained
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   */
  public PrioritySchedulerSubPool(SchedulerService delegateScheduler, int poolSize, 
                                  TaskPriority defaultPriority, long maxWaitForLowPriorityInMs) {
    super(defaultPriority);
    
    Queue<Runnable> readyToExecuteQueue = new ConcurrentLinkedQueue<>();
    this.workerPool = new DelegateExecutorWorkerPool(delegateScheduler, poolSize, readyToExecuteQueue);
    QueueChecker queueChecker = new QueueChecker(delegateScheduler, readyToExecuteQueue, workerPool);
    taskQueueManager = new QueueManager(queueChecker, maxWaitForLowPriorityInMs);
    queueChecker.setQueueManager(taskQueueManager);
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
   * Change the set thread pool size.
   * <p>
   * If the value is less than the current running threads, as threads finish they will exit 
   * rather than accept new tasks.  No currently running tasks will be interrupted, rather we 
   * will just wait for them to finish before killing the thread.
   * <p>
   * If this is an increase in the pool size, threads will be lazily started as needed till the 
   * new size is reached.  If there are tasks waiting for threads to run on, they immediately 
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
    throw new UnsupportedOperationException();  // TODO
  }
  
  @Override
  public int getQueuedTaskCount(TaskPriority priority) {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  protected OneTimeTaskWrapper doSchedule(Runnable task, long delayInMillis, TaskPriority priority) {
    QueueSet queueSet = taskQueueManager.getQueueSet(priority);
    OneTimeTaskWrapper result;
    if (delayInMillis == 0) {
      addToExecuteQueue(queueSet, 
                        (result = new OneTimeTaskWrapper(task, queueSet.getExecuteQueue(), 
                                                         Clock.lastKnownForwardProgressingMillis())));
    } else {
      addToScheduleQueue(queueSet, 
                         (result = new OneTimeTaskWrapper(task, queueSet.getScheduleQueue(), 
                                                          Clock.accurateForwardProgressingMillis() + 
                                                            delayInMillis)));
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

    QueueSet queueSet = taskQueueManager.getQueueSet(priority);
    addToScheduleQueue(queueSet, 
                       new RecurringDelayTaskWrapper(task, queueSet, 
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

    QueueSet queueSet = taskQueueManager.getQueueSet(priority);
    addToScheduleQueue(queueSet, 
                       new RecurringRateTaskWrapper(task, queueSet, 
                                                    Clock.accurateForwardProgressingMillis() + initialDelay, 
                                                    period));
  }
  
  /**
   * Adds the ready TaskWrapper to the correct execute queue.  Using the priority specified in the 
   * task, we pick the correct queue and add it.
   * <p>
   * If this is a scheduled or recurring task use {@link #addToScheduleQueue(TaskWrapper)}.
   * 
   * @param task {@link TaskWrapper} to queue for the scheduler
   */
  protected void addToExecuteQueue(QueueSet queueSet, OneTimeTaskWrapper task) {
    queueSet.addExecute(task);
  }
  
  /**
   * Adds the ready TaskWrapper to the correct schedule queue.  Using the priority specified in the 
   * task, we pick the correct queue and add it.
   * <p>
   * If this is just a single execution with no delay use {@link #addToExecuteQueue(OneTimeTaskWrapper)}.
   * 
   * @param task {@link TaskWrapper} to queue for the scheduler
   */
  protected void addToScheduleQueue(QueueSet queueSet, TaskWrapper task) {
    queueSet.addScheduled(task);
  }

  @Override
  protected QueueManager getQueueManager() {
    return taskQueueManager;
  }
  
  protected static class QueueChecker extends ReschedulingOperation implements QueueSetListener {
    private final Queue<Runnable> readyToExecutTasks;
    private final QueueSetListener workerPool;
    private QueueManager queueManager;
    
    protected QueueChecker(Executor executor, Queue<Runnable> readyToExecutTasks, 
                           QueueSetListener workerPool) {
      super(executor);
      
      this.readyToExecutTasks = readyToExecutTasks;
      this.workerPool = workerPool;
    }
    
    // TODO - fix awkward circular dependency
    public void setQueueManager(QueueManager queueManager) {
      this.queueManager = queueManager;
    }

    @Override
    public void handleQueueUpdate() {
      this.signalToRunImmediately(true);
    }

    @Override
    protected void run() {
      TaskWrapper nextTask;
      while ((nextTask = queueManager.getNextTask()) != null && nextTask.getScheduleDelay() <= 0) {
        nextTask.canExecute(nextTask.getExecuteReference());  // single threaded, so can just signal execution
        // TODO - handle race condition with removing tasks
        readyToExecutTasks.add(nextTask.getContainedRunnable());
        workerPool.handleQueueUpdate();
      }
    }
  }
  
  /**
   * Class to manage the pool of worker threads.  This class handles creating workers, storing 
   * them, and killing them once they are ready to expire.  It also handles finding the 
   * appropriate worker when a task is ready to be executed.
   * TODO - update
   * @since 5.16 
   */
  protected static class DelegateExecutorWorkerPool implements QueueSetListener {
    protected final Executor delegateExecutor;
    protected final Queue<Runnable> readyToExecuteTasks;  // set before any threads started
    protected final Object poolSizeChangeLock;
    protected final AtomicInteger currentPoolSize;
    private volatile int maxPoolSize;  // can only be changed when poolSizeChangeLock locked
    
    protected DelegateExecutorWorkerPool(Executor delegateExecutor, int poolSize, 
                                         Queue<Runnable> readyToExecuteTasks) {
      ArgumentVerifier.assertNotNull(delegateExecutor, "delegateExecutor");
      ArgumentVerifier.assertGreaterThanZero(poolSize, "poolSize");
      
      poolSizeChangeLock = new Object();
      currentPoolSize = new AtomicInteger(0);
      
      this.delegateExecutor = delegateExecutor;
      this.readyToExecuteTasks = readyToExecuteTasks;
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
      
      boolean poolSizeIncrease;
      synchronized (poolSizeChangeLock) {
        poolSizeIncrease = newPoolSize > this.maxPoolSize;
        
        this.maxPoolSize = newPoolSize;
      }
      
      if (poolSizeIncrease) {
        handleQueueUpdate();
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
      
      if (delta > 0) {
        // now that pool size increased, start a worker so workers we can for the waiting tasks
        handleQueueUpdate();
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

    @Override
    public void handleQueueUpdate() {
      int casSize;
      while ((casSize = currentPoolSize.get()) < maxPoolSize) {
        if (currentPoolSize.compareAndSet(casSize, casSize + 1)) {
          // start a new worker for the next task
          delegateExecutor.execute(new Worker(readyToExecuteTasks));
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
    protected final Queue<Runnable> taskQueue;
    
    protected Worker(Queue<Runnable> taskQueue) {
      this.taskQueue = taskQueue;
    }
    
    @Override
    public void run() {
      Runnable nextTask;
      while ((nextTask = taskQueue.poll()) != null) {
        ExceptionUtils.runRunnable(nextTask);
      }
    }
  }
}
