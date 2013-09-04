package org.threadly.concurrent;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureVirtualTask;
import org.threadly.concurrent.future.ListenableRunnableFuture;
import org.threadly.concurrent.lock.NativeLockFactory;
import org.threadly.concurrent.lock.StripedLock;
import org.threadly.concurrent.lock.VirtualLock;

/**
 * TaskDistributor is designed to take a multi-threaded pool
 * and add tasks with a given key such that those tasks will
 * be run single threaded for any given key.  The thread which
 * runs those tasks may be different each time, but no two tasks
 * with the same key will ever be run in parallel.
 * 
 * Because of that, it is recommended that the executor provided 
 * has as many possible threads as possible keys that could be 
 * provided to be run in parallel.  If this class is starved for 
 * threads some keys may continue to process new tasks, while
 * other keys could be starved.
 * 
 * @author jent - Mike Jensen
 */
public class TaskExecutorDistributor {
  protected static final int DEFAULT_THREAD_KEEPALIVE_TIME = 1000 * 10;
  protected static final int DEFAULT_LOCK_PARALISM = 10;
  protected static final float CONCURRENT_HASH_MAP_LOAD_FACTOR = (float)0.75;  // 0.75 is ConcurrentHashMap default
  protected static final int CONCURRENT_HASH_MAP_MAX_INITIAL_SIZE = 100;
  protected static final int CONCURRENT_HASH_MAP_MAX_CONCURRENCY_LEVEL = 100;
  
  protected final Executor executor;
  protected final StripedLock sLock;
  private final ConcurrentHashMap<Object, TaskQueueWorker> taskWorkers;
  
  /**
   * Constructor which creates executor based off provided values.
   * 
   * @param expectedParallism Expected number of keys that will be used in parallel
   * @param maxThreadCount Max thread count (limits the qty of keys which are handled in parallel)
   */
  public TaskExecutorDistributor(int expectedParallism, int maxThreadCount) {
    this(new PriorityScheduledExecutor(Math.min(expectedParallism, maxThreadCount), 
                                       maxThreadCount, 
                                       DEFAULT_THREAD_KEEPALIVE_TIME, 
                                       TaskPriority.High, 
                                       PriorityScheduledExecutor.DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS), 
         new StripedLock(expectedParallism, new NativeLockFactory()));
  }
  
  /**
   * Constructor to use a provided executor implementation for running tasks.
   * 
   * @param executor A multi-threaded executor to distribute tasks to.  
   *                 Ideally has as many possible threads as keys that 
   *                 will be used in parallel. 
   */
  public TaskExecutorDistributor(Executor executor) {
    this(DEFAULT_LOCK_PARALISM, executor);
  }
    
  /**
   * Constructor to use a provided executor implementation for running tasks.
   * 
   * @param expectedParallism level of expected qty of threads adding tasks in parallel
   * @param executor A multi-threaded executor to distribute tasks to.  
   *                 Ideally has as many possible threads as keys that 
   *                 will be used in parallel. 
   */
  public TaskExecutorDistributor(int expectedParallism, Executor executor) {
    this(executor, new StripedLock(expectedParallism, new NativeLockFactory()));
  }
  
  /**
   * Constructor to be used in unit tests.  This allows you to provide a StripedLock 
   * that provides a {@link org.threadly.test.concurrent.lock.TestableLockFactory} so 
   * that this class can be used with the 
   * {@link org.threadly.test.concurrent.TestablePriorityScheduler}.
   * 
   * @param executor executor to be used for task worker execution 
   * @param sLock lock to be used for controlling access to workers
   */
  public TaskExecutorDistributor(Executor executor, 
                                 StripedLock sLock) {
    if (executor == null) {
      throw new IllegalArgumentException("executor can not be null");
    } else if (sLock == null) {
      throw new IllegalArgumentException("striped lock must be provided");
    }
    
    this.executor = executor;
    this.sLock = sLock;
    int mapInitialSize = Math.min(sLock.getExpectedConcurrencyLevel(), 
                                  CONCURRENT_HASH_MAP_MAX_INITIAL_SIZE);
    int mapConcurrencyLevel = Math.min(sLock.getExpectedConcurrencyLevel(), 
                                       CONCURRENT_HASH_MAP_MAX_CONCURRENCY_LEVEL);
    this.taskWorkers = new ConcurrentHashMap<Object, TaskQueueWorker>(mapInitialSize,  
                                                                      CONCURRENT_HASH_MAP_LOAD_FACTOR, 
                                                                      mapConcurrencyLevel);
  }
  
  /**
   * Getter for the executor being used behind the scenes.
   * 
   * @return executor tasks are being distributed to
   */
  public Executor getExecutor() {
    return executor;
  }
  
  /**
   * Returns an executor implementation where all tasks submitted 
   * on this executor will run on the provided key.
   * 
   * @param threadKey object key where hashCode will be used to determine execution thread
   * @return executor which will only execute based on the provided key
   */
  public Executor getExecutorForKey(Object threadKey) {
    if (threadKey == null) {
      throw new IllegalArgumentException("Must provide thread key");
    }
    
    return new KeyBasedExecutor(threadKey);
  }
  
  /**
   * Will move any waiting tasks from a current key to a new execution key.
   * This is a one time move, if after this call additional tasks are executed 
   * under the original key, those will NOT be moved to the new key.
   * 
   * This will however make sure that at the point of returning from this call 
   * no tasks will be running under the original key.  With that said if tasks 
   * are scheduled under the original key during this call, they will start 
   * executing under the original key after this call returns.
   * 
   * If tasks are currently running under the newKey they will continue to run 
   * during this process.  We will _attempt_ to block the new key from running 
   * new tasks, but this is only a loose attempt.
   * 
   * @param originalKey key for original tasks to move from
   * @param newKey new key to move tasks to
   * @throws InterruptedException if thread is interrupted while waiting for tasks on original key to finish
   */
  public void moveCurrentTasks(Object originalKey, Object newKey) throws InterruptedException {
    if (originalKey == null) {
      throw new IllegalArgumentException("Must provide original key");
    } else if (newKey == null) {
      throw new IllegalArgumentException("Must provide new key");
    } else if (originalKey.hashCode() == newKey.hashCode()) {
      return; // no-op
    }
    
    VirtualLock originalLock = sLock.getLock(originalKey);
    synchronized (originalLock) {
      TaskQueueWorker originalWorker = taskWorkers.get(originalKey);
      if (originalWorker != null) {
        VirtualLock newLock = sLock.getLock(originalKey);
        synchronized (newLock) {
          boolean needToStartNewWorker = false;
          TaskQueueWorker newWorker = taskWorkers.get(newKey);
          if (newWorker == null) {
            newWorker = new TaskQueueWorker(newKey, newLock);
            taskWorkers.put(newKey, newWorker);
            needToStartNewWorker = true;
          }
          
          // move all waiting tasks from the original worker to the new one
          originalWorker.drainTo(newWorker);
          
          if (originalWorker.isStarted()) {
            /* wait for tasks to finish...
             * we hold the lock for the new worker during this time 
             * to prevent it from taking new tasks until the old worker has finished
             */
            while (! originalWorker.isFinished()) {
              /* wait on original worker lock so it can 
               * synchronize on it to mark itself as finished
               */
              originalLock.await();
            }
          }
          
          if (needToStartNewWorker) {
            executor.execute(newWorker);
          }
        }
      }
    }
  }
  
  /**
   * Provide a task to be run with a given thread key.
   * 
   * @param threadKey object key where hashCode will be used to determine execution thread
   * @param task Task to be executed.
   */
  public void addTask(Object threadKey, Runnable task) {
    if (threadKey == null) {
      throw new IllegalArgumentException("Must provide thread key");
    } else if (task == null) {
      throw new IllegalArgumentException("Must provide task");
    }
    
    VirtualLock agentLock = sLock.getLock(threadKey);
    synchronized (agentLock) {
      TaskQueueWorker worker = taskWorkers.get(threadKey);
      if (worker == null) {
        worker = new TaskQueueWorker(threadKey, agentLock, task);
        taskWorkers.put(threadKey, worker);
        executor.execute(worker);
      } else {
        worker.add(task);
      }
    }
  }
  
  /**
   * Submit a task to be run with a given thread key.
   * 
   * @param threadKey object key where hashCode will be used to determine execution thread
   * @param task Task to be executed.
   * @return Future to represent when the execution has occurred
   */
  public ListenableFuture<?> submitTask(Object threadKey, Runnable task) {
    return submitTask(threadKey, task, null);
  }
  
  /**
   * Submit a task to be run with a given thread key.
   * 
   * @param threadKey object key where hashCode will be used to determine execution thread
   * @param task Runnable to be executed.
   * @param result Result to be returned from future when task completes
   * @return Future to represent when the execution has occurred and provide the given result
   */
  public <T> ListenableFuture<T> submitTask(Object threadKey, Runnable task, 
                                            T result) {
    if (threadKey == null) {
      throw new IllegalArgumentException("Must provide thread key");
    } else if (task == null) {
      throw new IllegalArgumentException("Must provide task");
    }
    
    ListenableRunnableFuture<T> rf = new ListenableFutureVirtualTask<T>(task, result, 
                                                                        sLock.getLock(threadKey));
    
    addTask(threadKey, rf);
    
    return rf;
  }
  
  /**
   * Submit a callable to be run with a given thread key.
   * 
   * @param threadKey object key where hashCode will be used to determine execution thread
   * @param task Callable to be executed.
   * @return Future to represent when the execution has occurred and provide the result from the callable
   */
  public <T> ListenableFuture<T> submitTask(Object threadKey, Callable<T> task) {
    if (threadKey == null) {
      throw new IllegalArgumentException("Must provide thread key");
    } else if (task == null) {
      throw new IllegalArgumentException("Must provide task");
    }
    
    ListenableRunnableFuture<T> rf = new ListenableFutureVirtualTask<T>(task, 
                                                                        sLock.getLock(threadKey));
    
    addTask(threadKey, rf);
    
    return rf;
  }
  
  /**
   * Worker which will consume through a given queue of tasks.
   * Each key is represented by one worker at any given time.
   * 
   * @author jent - Mike Jensen
   */
  private class TaskQueueWorker extends VirtualRunnable {
    private final Object mapKey;
    private final VirtualLock agentLock;
    private LinkedList<Runnable> queue;
    private boolean started;
    private boolean finished;
    
    private TaskQueueWorker(Object mapKey, 
                            VirtualLock agentLock) {
      this.mapKey = mapKey;
      this.agentLock = agentLock;
      this.queue = new LinkedList<Runnable>();
      started = false;
      finished = false;
    }
    
    private TaskQueueWorker(Object mapKey, 
                            VirtualLock agentLock, 
                            Runnable firstTask) {
      this(mapKey, agentLock);
      
      queue.add(firstTask);
    }
    
    // should be locked around agentLock before calling
    private void add(Runnable task) {
      queue.addLast(task);
    }

    // should be locked around agentLock before calling
    private void addAll(List<Runnable> tasks) {
      queue.addAll(tasks);
    }

    // should be locked around agentLock and provided workers lock before calling
    private void drainTo(TaskQueueWorker worker) {
      worker.addAll(queue);
      queue.clear();
    }

    // should be locked around agentLock before calling
    private boolean isStarted() {
      return started;
    }
    
    private boolean isFinished() {
      return finished;
    }
    
    @Override
    public void run() {
      while (true) {
        List<Runnable> nextList;
        synchronized (agentLock) {
          started = true;
          nextList = queue;
          
          if (nextList.isEmpty()) {  // stop consuming tasks
            taskWorkers.remove(mapKey);
            finished = true;
            
            agentLock.signalAll();
            break;
          } else {  // prepare queue for future tasks
            queue = new LinkedList<Runnable>();
          }
        }
        
        Iterator<Runnable> it = nextList.iterator();
        while (it.hasNext()) {
          try {
            Runnable next = it.next();
            if (factory != null && next instanceof VirtualRunnable) {
              ((VirtualRunnable)next).run(factory);
            } else {
              next.run();
            }
          } catch (Throwable t) {
            UncaughtExceptionHandler ueh = Thread.getDefaultUncaughtExceptionHandler();
            if (ueh != null) {
              ueh.uncaughtException(Thread.currentThread(), t);
            } else {
              t.printStackTrace();
            }
          }
        }
      }
    }
  }
  
  /**
   * Simple executor implementation that runs on a given key.
   * 
   * @author jent - Mike Jensen
   */
  protected class KeyBasedExecutor implements Executor {
    protected final Object threadKey;
    
    protected KeyBasedExecutor(Object threadKey) {
      this.threadKey = threadKey;
    }
    
    @Override
    public void execute(Runnable command) {
      addTask(threadKey, command);
    }
  }
}
