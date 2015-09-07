package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.BlockingTestRunnable;
import org.threadly.ThreadlyTestUtil;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.util.ExceptionUtils;
import org.threadly.util.TestExceptionHandler;

@SuppressWarnings("javadoc")
public class KeyDistributedExecutorTest {
  private static final int PARALLEL_LEVEL = TEST_QTY;
  private static final int RUNNABLE_COUNT_PER_LEVEL = TEST_QTY * 2;
  
  private static PriorityScheduler scheduler;
  
  @BeforeClass
  public static void setupClass() {
    ThreadlyTestUtil.setIgnoreExceptionHandler();
    
    scheduler = new StrictPriorityScheduler(PARALLEL_LEVEL * 2);
  }
  
  @AfterClass
  public static void cleanupClass() {
    scheduler.shutdownNow();
    scheduler = null;
  }
  
  private KeyDistributedExecutor distributor;
  
  @Before
  public void setup() {
    distributor = new KeyDistributedExecutor(1, scheduler, Integer.MAX_VALUE, false);
  }
  
  @After
  public void cleanup() {
    distributor = null;
  }
  
  private static List<TDRunnable> populate(AddHandler ah) {
    final List<TDRunnable> runs = new ArrayList<TDRunnable>(PARALLEL_LEVEL * RUNNABLE_COUNT_PER_LEVEL);
    
    // use BTR to avoid execution till all are submitted
    List<BlockingTestRunnable> blockingRunnables = new ArrayList<BlockingTestRunnable>(PARALLEL_LEVEL);
    try {
      for (int i = 0; i < PARALLEL_LEVEL; i++) {
        BlockingTestRunnable btr = new BlockingTestRunnable();
        blockingRunnables.add(btr);
        ThreadContainer tc = new ThreadContainer();
        ah.addTDRunnable(tc, btr);
        TDRunnable previous = null;
        for (int j = 0; j < RUNNABLE_COUNT_PER_LEVEL; j++) {
          TDRunnable tr = new TDRunnable(tc, previous);
          runs.add(tr);
          ah.addTDRunnable(tc, tr);
          
          previous = tr;
        }
      }
    } finally {
      for (BlockingTestRunnable btr : blockingRunnables) {
        btr.unblock();
      }
    }
    
    return runs;
  }
  
  @SuppressWarnings("unused")
  @Test
  public void constructorTest() {
    // none should throw exception
    new KeyDistributedExecutor(scheduler);
    new KeyDistributedExecutor(scheduler, true);
    new KeyDistributedExecutor(scheduler, 1);
    new KeyDistributedExecutor(scheduler, 1, true);
    new KeyDistributedExecutor(1, scheduler);
    new KeyDistributedExecutor(1, scheduler, true);
    new KeyDistributedExecutor(1, scheduler, 1);
    new KeyDistributedExecutor(1, scheduler, 1, true);
  }
  
  @SuppressWarnings("unused")
  @Test
  public void constructorFail() {
    try {
      new KeyDistributedExecutor(1, null);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new KeyDistributedExecutor(-1, scheduler, 1, false);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new KeyDistributedExecutor(1, scheduler, -1, false);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void getExecutorTest() {
    assertTrue(scheduler == distributor.getExecutor());
  }
  
  @Test
  public void keyBasedSubmitterConsistentThreadTest() {
    List<TDRunnable> runs = populate(new AddHandler() {
      @Override
      public void addTDRunnable(Object key, Runnable tdr) {
        SubmitterExecutor keySubmitter = distributor.getSubmitterForKey(key);
        keySubmitter.submit(tdr);
      }
    });
    
    Iterator<TDRunnable> it = runs.iterator();
    while (it.hasNext()) {
      TDRunnable tr = it.next();
      tr.blockTillFinished(1000 * 20);
      assertEquals(1, tr.getRunCount()); // verify each only ran once
      assertTrue(tr.threadTracker.threadConsistent());  // verify that all threads for a given key ran in the same thread
      assertTrue(tr.previousRanFirst());  // verify runnables were run in order
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getSubmitterForKeyFail() {
    distributor.getSubmitterForKey(null);
  }
  
  @Test
  @SuppressWarnings("deprecation")
  public void addTaskFail() {
    try {
      distributor.addTask(null, new TestRunnable());
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      distributor.addTask(new Object(), null);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void executeFail() {
    try {
      distributor.execute(null, new TestRunnable());
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      distributor.execute(new Object(), null);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  @SuppressWarnings("deprecation")
  public void addTaskConsistentThreadTest() {
    List<TDRunnable> runs = populate(new AddHandler() {
      @Override
      public void addTDRunnable(Object key, Runnable tdr) {
        distributor.addTask(key, tdr);
      }
    });

    Iterator<TDRunnable> it = runs.iterator();
    while (it.hasNext()) {
      TDRunnable tr = it.next();
      tr.blockTillFinished(20 * 1000);
      assertEquals(1, tr.getRunCount()); // verify each only ran once
      assertTrue(tr.threadTracker.threadConsistent());  // verify that all threads for a given key ran in the same thread
      assertTrue(tr.previousRanFirst());  // verify runnables were run in order
    }
  }
  
  @Test
  public void executeConsistentThreadTest() {
    List<TDRunnable> runs = populate(new AddHandler() {
      @Override
      public void addTDRunnable(Object key, Runnable tdr) {
        distributor.execute(key, tdr);
      }
    });

    Iterator<TDRunnable> it = runs.iterator();
    while (it.hasNext()) {
      TDRunnable tr = it.next();
      tr.blockTillFinished(20 * 1000);
      assertEquals(1, tr.getRunCount()); // verify each only ran once
      assertTrue(tr.threadTracker.threadConsistent());  // verify that all threads for a given key ran in the same thread
      assertTrue(tr.previousRanFirst());  // verify runnables were run in order
    }
  }
  
  @Test
  @SuppressWarnings("deprecation")
  public void submitTaskRunnableFail() {
    try {
      distributor.submitTask(null, new TestRunnable());
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submitTask(new Object(), null, null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void submitRunnableFail() {
    try {
      distributor.submit(null, new TestRunnable());
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submit(new Object(), null, null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  @SuppressWarnings("deprecation")
  public void submitTaskRunnableConsistentThreadTest() {
    List<TDRunnable> runs = populate(new AddHandler() {
      @Override
      public void addTDRunnable(Object key, Runnable tdr) {
        distributor.submitTask(key, tdr);
      }
    });
    
    Iterator<TDRunnable> it = runs.iterator();
    while (it.hasNext()) {
      TDRunnable tr = it.next();
      tr.blockTillFinished(20 * 1000);
      assertEquals(1, tr.getRunCount()); // verify each only ran once
      assertTrue(tr.threadTracker.threadConsistent());  // verify that all threads for a given key ran in the same thread
      assertTrue(tr.previousRanFirst());  // verify runnables were run in order
    }
  }
  
  @Test
  public void submitRunnableConsistentThreadTest() {
    List<TDRunnable> runs = populate(new AddHandler() {
      @Override
      public void addTDRunnable(Object key, Runnable tdr) {
        distributor.submit(key, tdr);
      }
    });
    
    Iterator<TDRunnable> it = runs.iterator();
    while (it.hasNext()) {
      TDRunnable tr = it.next();
      tr.blockTillFinished(20 * 1000);
      assertEquals(1, tr.getRunCount()); // verify each only ran once
      assertTrue(tr.threadTracker.threadConsistent());  // verify that all threads for a given key ran in the same thread
      assertTrue(tr.previousRanFirst());  // verify runnables were run in order
    }
  }
  
  @Test
  @SuppressWarnings("deprecation")
  public void submitTaskCallableConsistentThreadTest() {
    List<TDCallable> runs = new ArrayList<TDCallable>(PARALLEL_LEVEL * RUNNABLE_COUNT_PER_LEVEL);
    
    // use BTR to avoid execution till all are submitted
    List<BlockingTestRunnable> blockingRunnables = new ArrayList<BlockingTestRunnable>(PARALLEL_LEVEL);
    try {
      for (int i = 0; i < PARALLEL_LEVEL; i++) {
        BlockingTestRunnable btr = new BlockingTestRunnable();
        blockingRunnables.add(btr);
        ThreadContainer tc = new ThreadContainer();
        distributor.submitTask(tc, btr);
        TDCallable previous = null;
        for (int j = 0; j < RUNNABLE_COUNT_PER_LEVEL; j++) {
          TDCallable tr = new TDCallable(tc, previous);
          runs.add(tr);
          distributor.submitTask(tc, tr);
          
          previous = tr;
        }
      }
    } finally {
      for (BlockingTestRunnable btr : blockingRunnables) {
        btr.unblock();
      }
    }
    
    Iterator<TDCallable> it = runs.iterator();
    while (it.hasNext()) {
      TDCallable tr = it.next();
      tr.blockTillFinished(20 * 1000);
      assertTrue(tr.threadTracker.threadConsistent());  // verify that all threads for a given key ran in the same thread
      assertTrue(tr.previousRanFirst());  // verify runnables were run in order
    }
  }
  
  @Test
  public void submitCallableConsistentThreadTest() {
    List<TDCallable> runs = new ArrayList<TDCallable>(PARALLEL_LEVEL * RUNNABLE_COUNT_PER_LEVEL);

    // use BTR to avoid execution till all are submitted
    List<BlockingTestRunnable> blockingRunnables = new ArrayList<BlockingTestRunnable>(PARALLEL_LEVEL);
    try {
      for (int i = 0; i < PARALLEL_LEVEL; i++) {
        BlockingTestRunnable btr = new BlockingTestRunnable();
        blockingRunnables.add(btr);
        ThreadContainer tc = new ThreadContainer();
        distributor.submit(tc, btr);
        TDCallable previous = null;
        for (int j = 0; j < RUNNABLE_COUNT_PER_LEVEL; j++) {
          TDCallable tr = new TDCallable(tc, previous);
          runs.add(tr);
          distributor.submit(tc, tr);
          
          previous = tr;
        }
      }
    } finally {
      for (BlockingTestRunnable btr : blockingRunnables) {
        btr.unblock();
      }
    }
    
    Iterator<TDCallable> it = runs.iterator();
    while (it.hasNext()) {
      TDCallable tr = it.next();
      tr.blockTillFinished(20 * 1000);
      assertTrue(tr.threadTracker.threadConsistent());  // verify that all threads for a given key ran in the same thread
      assertTrue(tr.previousRanFirst());  // verify runnables were run in order
    }
  }
  
  @Test
  @SuppressWarnings("deprecation")
  public void submitTaskCallableFail() {
    try {
      distributor.submitTask(null, new TestCallable());
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submitTask(new Object(), (Callable<Void>)null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void submitCallableFail() {
    try {
      distributor.submit(null, new TestCallable());
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submit(new Object(), (Callable<Void>)null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void executeStressTest() {
    final Object testLock = new Object();
    final int expectedCount = (PARALLEL_LEVEL * 2) * (RUNNABLE_COUNT_PER_LEVEL * 2);
    final List<TDRunnable> runs = new ArrayList<TDRunnable>(expectedCount);
    
    // we can't use populate here because we don't want to hold the agentLock
    
    scheduler.execute(new Runnable() {
      private final Map<Integer, ThreadContainer> containers = new HashMap<Integer, ThreadContainer>();
      private final Map<Integer, TDRunnable> previousRunnables = new HashMap<Integer, TDRunnable>();
      
      @Override
      public void run() {
        synchronized (testLock) {
          for (int i = 0; i < RUNNABLE_COUNT_PER_LEVEL * 2; i++) {
            for (int j = 0; j < PARALLEL_LEVEL * 2; j++) {
              ThreadContainer tc = containers.get(j);
              if (tc == null) {
                tc = new ThreadContainer();
                containers.put(j, tc);
              }
              
              TDRunnable tr = new TDRunnable(tc, previousRunnables.get(j)) {
                private boolean added = false;
                
                @Override
                public void handleRunFinish() {
                  if (! added) {
                    distributor.execute(threadTracker, this);
                    added = true;
                  }
                }
              };
              runs.add(tr);
              distributor.execute(tc, tr);
              previousRunnables.put(j, tr);
            }
          }
        }
      }
    });
    
    // block till ready to ensure other thread got testLock lock
    new TestCondition() {
      @Override
      public boolean get() {
        synchronized (testLock) {
          return runs.size() == expectedCount;
        }
      }
    }.blockTillTrue(20 * 1000, 100);

    synchronized (testLock) {
      Iterator<TDRunnable> it = runs.iterator();
      while (it.hasNext()) {
        TDRunnable tr = it.next();
        tr.blockTillFinished(20 * 1000, 2);
        assertEquals(2, tr.getRunCount()); // verify each only ran twice
        assertTrue(tr.previousRanFirst);  // verify runnables were run in order
        assertFalse(tr.ranConcurrently());  // verify that it never run in parallel
      }
    }
  }
  
  @Test
  public void taskExceptionTest() {
    Integer key = 1;
    TestExceptionHandler teh = new TestExceptionHandler();
    final RuntimeException testException = new RuntimeException();
    ExceptionUtils.setDefaultExceptionHandler(teh);
    TestRunnable exceptionRunnable = new TestRuntimeFailureRunnable(testException);
    TestRunnable followRunnable = new TestRunnable();
    distributor.execute(key, exceptionRunnable);
    distributor.execute(key, followRunnable);
    exceptionRunnable.blockTillFinished();
    followRunnable.blockTillStarted();  // verify that it ran despite the exception
    
    assertEquals(1, teh.getCallCount());
    assertEquals(testException, teh.getLastThrowable());
  }
  
  @Test
  public void limitExecutionPerCycleTest() {
    final AtomicInteger execCount = new AtomicInteger(0);
    KeyDistributedExecutor distributor = new KeyDistributedExecutor(1, new Executor() {
      @Override
      public void execute(Runnable command) {
        execCount.incrementAndGet();
        
        new Thread(command).start();
      }
    }, 1);
    
    BlockingTestRunnable btr = new BlockingTestRunnable();
    
    distributor.execute(this, btr);
    btr.blockTillStarted();
    
    // add second task while we know worker is active
    TestRunnable secondTask = new TestRunnable();
    distributor.execute(this, secondTask);
    
    assertEquals(1, distributor.taskWorkers.size());
    assertEquals(1, distributor.taskWorkers.get(this).queue.size());
    
    btr.unblock();
    
    secondTask.blockTillFinished();
    
    // verify worker execed out between task
    assertEquals(2, execCount.get());
  }
  
  @Test
  public void limitExecutionPerCycleStressTest() {
    PriorityScheduler scheduler = new StrictPriorityScheduler(3);
    final AtomicBoolean testComplete = new AtomicBoolean(false);
    try {
      final Integer key1 = 1;
      final Integer key2 = 2;
      Executor singleThreadedExecutor = scheduler.makeSubPool(1);
      final KeyDistributedExecutor distributor = new KeyDistributedExecutor(2, singleThreadedExecutor, 2);
      final AtomicInteger waitingTasks = new AtomicInteger();
      final AtomicReference<TestRunnable> lastTestRunnable = new AtomicReference<TestRunnable>();
      scheduler.execute(new Runnable() {  // execute thread to add for key 1
        @Override
        public void run() {
          while (! testComplete.get()) {
            TestRunnable next = new TestRunnable() {
              @Override
              public void handleRunStart() {
                waitingTasks.decrementAndGet();
                
                TestUtils.sleep(20);  // wait to make sure producer is faster than executor
              }
            };
            lastTestRunnable.set(next);
            waitingTasks.incrementAndGet();
            distributor.execute(key1, next);
          }
        }
      });
      
      // block till there is for sure a backup of key1 tasks
      new TestCondition() {
        @Override
        public boolean get() {
          return waitingTasks.get() > 10;
        }
      }.blockTillTrue();
      
      TestRunnable key2Runnable = new TestRunnable();
      distributor.execute(key2, key2Runnable);
      TestRunnable lastKey1Runnable = lastTestRunnable.get();
      key2Runnable.blockTillStarted();  // will throw exception if not started
      // verify it ran before the lastKey1Runnable
      assertFalse(lastKey1Runnable.ranOnce());
    } finally {
      testComplete.set(true);
      scheduler.shutdownNow();
    }
  }
  
  private static void getTaskQueueSizeSimpleTest(boolean accurateDistributor) {
    final Object taskKey = new Object();
    KeyDistributedExecutor kde = new KeyDistributedExecutor(new Executor() {
      @Override
      public void execute(Runnable command) {
        // kidding, don't actually execute, haha
      }
    }, accurateDistributor);
    
    assertEquals(0, kde.getTaskQueueSize(taskKey));
    
    kde.execute(taskKey, new TestRunnable());
    
    // should add as first task
    assertEquals(1, kde.getTaskQueueSize(taskKey));
    
    kde.execute(taskKey, new TestRunnable());
    
    // will now add into the queue
    assertEquals(2, kde.getTaskQueueSize(taskKey));
  }
  
  private static void getTaskQueueSizeThreadedTest(boolean accurateDistributor) {
    final Object taskKey = new Object();
    KeyDistributedExecutor kde = new KeyDistributedExecutor(scheduler, accurateDistributor);
    
    assertEquals(0, kde.getTaskQueueSize(taskKey));
    
    BlockingTestRunnable btr = new BlockingTestRunnable();
    kde.execute(taskKey, btr);
    
    // add more tasks while remaining blocked
    kde.execute(taskKey, new TestRunnable());
    kde.execute(taskKey, new TestRunnable());
    
    btr.blockTillStarted();
    
    assertEquals(2, kde.getTaskQueueSize(taskKey));
    
    btr.unblock();
  }
  
  @Test
  public void getTaskQueueSizeInaccurateTest() {
    getTaskQueueSizeSimpleTest(false);
    getTaskQueueSizeThreadedTest(false);
  }
  
  @Test
  public void getTaskQueueSizeAccurateTest() {
    getTaskQueueSizeSimpleTest(true);
    getTaskQueueSizeThreadedTest(true);
  }
  
  private static void getTaskQueueSizeMapSimpleTest(boolean accurateDistributor) {
    final Object taskKey = new Object();
    KeyDistributedExecutor kde = new KeyDistributedExecutor(new Executor() {
      @Override
      public void execute(Runnable command) {
        // kidding, don't actually execute, haha
      }
    }, accurateDistributor);
    
    Map<?, Integer> result = kde.getTaskQueueSizeMap();
    assertTrue(result.isEmpty());
    
    kde.execute(taskKey, new TestRunnable());
    
    // should add as first task
    result = kde.getTaskQueueSizeMap();
    assertEquals(1, result.size());
    assertEquals((Integer)1, result.get(taskKey));
    
    kde.execute(taskKey, new TestRunnable());
    
    // will now add into the queue
    result = kde.getTaskQueueSizeMap();
    assertEquals(1, result.size());
    assertEquals((Integer)2, result.get(taskKey));
  }
  
  private static void getTaskQueueSizeMapThreadedTest(boolean accurateDistributor) {
    final Object taskKey = new Object();
    KeyDistributedExecutor kde = new KeyDistributedExecutor(scheduler, accurateDistributor);

    Map<?, Integer> result = kde.getTaskQueueSizeMap();
    assertTrue(result.isEmpty());
    
    BlockingTestRunnable btr = new BlockingTestRunnable();
    kde.execute(taskKey, btr);
    
    // add more tasks while remaining blocked
    kde.execute(taskKey, new TestRunnable());
    kde.execute(taskKey, new TestRunnable());
    
    btr.blockTillStarted();
    
    result = kde.getTaskQueueSizeMap();
    assertEquals(1, result.size());
    assertEquals((Integer)2, result.get(taskKey));
    
    btr.unblock();
  }
  
  @Test
  public void getTaskQueueSizeMapInaccurateTest() {
    getTaskQueueSizeMapSimpleTest(false);
    getTaskQueueSizeMapThreadedTest(false);
  }
  
  @Test
  public void getTaskQueueSizeMapAccurateTest() {
    getTaskQueueSizeMapSimpleTest(true);
    getTaskQueueSizeMapThreadedTest(true);
  }
  
  protected static class TDRunnable extends TestRunnable {
    protected final TDRunnable previousRunnable;
    protected final ThreadContainer threadTracker;
    private volatile boolean previousRanFirst;
    private volatile boolean verifiedPrevious;
    
    protected TDRunnable(ThreadContainer threadTracker, 
                         TDRunnable previousRunnable) {
      this.threadTracker = threadTracker;
      this.previousRunnable = previousRunnable;
      previousRanFirst = false;
      verifiedPrevious = false;
    }
    
    @Override
    public void handleRunStart() {
      threadTracker.running();
      
      if (! verifiedPrevious) {
        if (previousRunnable != null) {
          previousRanFirst = previousRunnable.getRunCount() >= 1;
        } else {
          previousRanFirst = true;
        }
        
        verifiedPrevious = true;
      }
    }
    
    public boolean previousRanFirst() {
      return previousRanFirst;
    }
  }
  
  protected static class TDCallable extends TestCallable {
    protected final TDCallable previousRunnable;
    protected final ThreadContainer threadTracker;
    private volatile boolean previousRanFirst;
    private volatile boolean verifiedPrevious;
    
    protected TDCallable(ThreadContainer threadTracker, 
                         TDCallable previousRunnable) {
      this.threadTracker = threadTracker;
      this.previousRunnable = previousRunnable;
      previousRanFirst = false;
      verifiedPrevious = false;
    }
    
    @Override
    public void handleCallStart() {
      threadTracker.running();
      
      if (! verifiedPrevious) {
        if (previousRunnable != null) {
          previousRanFirst = previousRunnable.isDone();
        } else {
          previousRanFirst = true;
        }
        
        verifiedPrevious = true;
      }
    }
    
    public boolean previousRanFirst() {
      return previousRanFirst;
    }
  }
  
  protected static class ThreadContainer {
    private Thread runningThread = null;
    private boolean threadConsistent = true;
    
    public synchronized void running() {
      if (runningThread == null) {
        runningThread = Thread.currentThread();
      } else {
        threadConsistent = threadConsistent && runningThread == Thread.currentThread();
      }
    }
    
    public boolean threadConsistent() {
      return threadConsistent;
    }
  }
  
  private interface AddHandler {
    public void addTDRunnable(Object key, Runnable tdr);
  }
}
