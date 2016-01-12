package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.ThreadlyTestUtil;
import org.threadly.concurrent.KeyDistributedExecutorTest.TDCallable;
import org.threadly.concurrent.KeyDistributedExecutorTest.TDRunnable;
import org.threadly.concurrent.KeyDistributedExecutorTest.ThreadContainer;
import org.threadly.concurrent.lock.StripedLock;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestableScheduler;

@SuppressWarnings("javadoc")
public class KeyDistributedSchedulerTest {
  private static final int PARALLEL_LEVEL = TEST_QTY;
  private static final int RUNNABLE_COUNT_PER_LEVEL = TEST_QTY * 2;
  
  @BeforeClass
  public static void setupClass() {
    ThreadlyTestUtil.setIgnoreExceptionHandler();
  }
  
  private PriorityScheduler scheduler;
  private Lock agentLock;
  private KeyDistributedScheduler distributor;
  
  @Before
  public void setup() {
    scheduler = new StrictPriorityScheduler(PARALLEL_LEVEL * 2);
    StripedLock sLock = new StripedLock(1);
    agentLock = sLock.getLock(null);  // there should be only one lock
    distributor = new KeyDistributedScheduler(scheduler, sLock, Integer.MAX_VALUE, false);
  }
  
  @After
  public void cleanup() {
    scheduler.shutdownNow();
    scheduler = null;
    agentLock = null;
    distributor = null;
  }
  
  private List<TDRunnable> populate(AddHandler ah) {
    final List<TDRunnable> runs = new ArrayList<TDRunnable>(PARALLEL_LEVEL * RUNNABLE_COUNT_PER_LEVEL);
    
    // hold agent lock to prevent execution till ready
    agentLock.lock();
    try {
      for (int i = 0; i < PARALLEL_LEVEL; i++) {
        ThreadContainer tc = new ThreadContainer();
        TDRunnable previous = null;
        for (int j = 0; j < RUNNABLE_COUNT_PER_LEVEL; j++) {
          TDRunnable tr = new TDRunnable(tc, previous);
          runs.add(tr);
          ah.addTDRunnable(tc, tr);
          
          previous = tr;
        }
      }
    } finally {
      agentLock.unlock();
    }
    
    return runs;
  }
  
  @SuppressWarnings("unused")
  @Test
  public void constructorFail() {
    try {
      new KeyDistributedScheduler(1, null);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new KeyDistributedScheduler(scheduler, null, 
                                  Integer.MAX_VALUE, false);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @SuppressWarnings("unused")
  @Test
  public void constructorTest() {
    // none should throw exception
    new KeyDistributedScheduler(scheduler);
    new KeyDistributedScheduler(scheduler, true);
    new KeyDistributedScheduler(scheduler, 1);
    new KeyDistributedScheduler(scheduler, 1, true);
    new KeyDistributedScheduler(1, scheduler);
    new KeyDistributedScheduler(1, scheduler, true);
    new KeyDistributedScheduler(1, scheduler, 1);
    new KeyDistributedScheduler(1, scheduler, 1, true);
    StripedLock sLock = new StripedLock(1);
    new KeyDistributedScheduler(scheduler, sLock, 1, false);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getSubmitterSchedulerForKeyFail() {
    distributor.getSubmitterSchedulerForKey(null);
  }
  
  @Test
  public void getExecutorTest() {
    assertTrue(scheduler == distributor.getExecutor());
  }
  
  @Test
  @SuppressWarnings("deprecation")
  public void addTaskTest() {
    List<TDRunnable> runs = populate(new AddHandler() {
      @Override
      public void addTDRunnable(Object key, TDRunnable tdr) {
        distributor.addTask(key, tdr);
      }
    });
    
    Iterator<TDRunnable> it = runs.iterator();
    while (it.hasNext()) {
      TDRunnable tr = it.next();
      tr.blockTillFinished(1000);
      assertEquals(1, tr.getRunCount()); // verify each only ran once
      assertTrue(tr.threadTracker.threadConsistent());  // verify that all threads for a given key ran in the same thread
      assertTrue(tr.previousRanFirst());  // verify runnables were run in order
    }
  }
  
  @Test
  public void executeTest() {
    List<TDRunnable> runs = populate(new AddHandler() {
      @Override
      public void addTDRunnable(Object key, TDRunnable tdr) {
        distributor.execute(key, tdr);
      }
    });
    
    Iterator<TDRunnable> it = runs.iterator();
    while (it.hasNext()) {
      TDRunnable tr = it.next();
      tr.blockTillFinished(1000);
      assertEquals(1, tr.getRunCount()); // verify each only ran once
      assertTrue(tr.threadTracker.threadConsistent());  // verify that all threads for a given key ran in the same thread
      assertTrue(tr.previousRanFirst());  // verify runnables were run in order
    }
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
  public void submitTaskRunnableFail() {
    try {
      distributor.submitTask(null, new TestRunnable());
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submitTask(new Object(), null, null);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void submitRunnableFail() {
    try {
      distributor.submit(null, new TestRunnable());
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submit(new Object(), null, null);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  @SuppressWarnings("deprecation")
  public void submitTaskCallableConsistentThreadTest() {
    List<TDCallable> runs = new ArrayList<TDCallable>(PARALLEL_LEVEL * RUNNABLE_COUNT_PER_LEVEL);
    
    // hold agent lock to avoid execution till all are submitted
    agentLock.lock();
    try {
      for (int i = 0; i < PARALLEL_LEVEL; i++) {
        ThreadContainer tc = new ThreadContainer();
        TDCallable previous = null;
        for (int j = 0; j < RUNNABLE_COUNT_PER_LEVEL; j++) {
          TDCallable tr = new TDCallable(tc, previous);
          runs.add(tr);
          distributor.submitTask(tc, tr);
          
          previous = tr;
        }
      }
    } finally {
      agentLock.unlock();
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
    
    // hold agent lock to avoid execution till all are submitted
    agentLock.lock();
    try {
      for (int i = 0; i < PARALLEL_LEVEL; i++) {
        ThreadContainer tc = new ThreadContainer();
        TDCallable previous = null;
        for (int j = 0; j < RUNNABLE_COUNT_PER_LEVEL; j++) {
          TDCallable tr = new TDCallable(tc, previous);
          runs.add(tr);
          distributor.submit(tc, tr);
          
          previous = tr;
        }
      }
    } finally {
      agentLock.unlock();
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
      fail("Exception should have been thrown");
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
      fail("Exception should have been thrown");
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
  @SuppressWarnings("deprecation")
  public void scheduleTaskExecutionTest() {
    List<TDRunnable> runs = populate(new AddHandler() {
      @Override
      public void addTDRunnable(Object key, TDRunnable tdr) {
        distributor.scheduleTask(key, tdr, DELAY_TIME);
      }
    });
    
    Iterator<TDRunnable> it = runs.iterator();
    while (it.hasNext()) {
      TDRunnable tr = it.next();
      tr.blockTillFinished(1000);
      assertEquals(1, tr.getRunCount()); // verify each only ran once
      assertTrue(tr.getDelayTillFirstRun() >= DELAY_TIME);
    }
  }
  
  @Test
  public void scheduleExecutionTest() {
    List<TDRunnable> runs = populate(new AddHandler() {
      @Override
      public void addTDRunnable(Object key, TDRunnable tdr) {
        distributor.schedule(key, tdr, DELAY_TIME);
      }
    });
    
    Iterator<TDRunnable> it = runs.iterator();
    while (it.hasNext()) {
      TDRunnable tr = it.next();
      tr.blockTillFinished(1000);
      assertEquals(1, tr.getRunCount()); // verify each only ran once
      assertTrue(tr.getDelayTillFirstRun() >= DELAY_TIME);
    }
  }
  
  @Test
  @SuppressWarnings("deprecation")
  public void scheduleTaskExecutionFail() {
    try {
      distributor.scheduleTask(new Object(), null, 1000);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.scheduleTask(new Object(), new TestRunnable(), -1);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.scheduleTask(null, new TestRunnable(), 100);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void scheduleExecutionFail() {
    try {
      distributor.schedule(new Object(), null, 1000);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.schedule(new Object(), new TestRunnable(), -1);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.schedule(null, new TestRunnable(), 100);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  @SuppressWarnings("deprecation")
  public void submitScheduledTaskRunnableFail() {
    try {
      distributor.submitScheduledTask(new Object(), (Runnable)null, 1000);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submitScheduledTask(new Object(), new TestRunnable(), -1);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submitScheduledTask(null, new TestRunnable(), 100);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void submitScheduledRunnableFail() {
    try {
      distributor.submitScheduled(new Object(), (Runnable)null, 1000);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submitScheduled(new Object(), new TestRunnable(), -1);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submitScheduled(null, new TestRunnable(), 100);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  @SuppressWarnings("deprecation")
  public void submitScheduledTaskCallableFail() {
    try {
      distributor.submitScheduledTask(new Object(), (Callable<?>)null, 1000);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submitScheduledTask(new Object(), new TestCallable(), -1);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submitScheduledTask(null, new TestCallable(), 100);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void submitScheduledCallableFail() {
    try {
      distributor.submitScheduled(new Object(), (Callable<?>)null, 1000);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submitScheduled(new Object(), new TestCallable(), -1);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submitScheduled(null, new TestCallable(), 100);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void recurringExecutionTest() {
    List<TDRunnable> runs = populate(new AddHandler() {
      int initialDelay = 0;
      @Override
      public void addTDRunnable(Object key, TDRunnable tdr) {
        distributor.scheduleTaskWithFixedDelay(key, tdr, initialDelay++, DELAY_TIME);
      }
    });
    
    Iterator<TDRunnable> it = runs.iterator();
    while (it.hasNext()) {
      TDRunnable tr = it.next();
      assertTrue(tr.getDelayTillRun(2) >= DELAY_TIME);
      tr.blockTillFinished(10 * 1000, 3);
      assertFalse(tr.ranConcurrently());  // verify that it never run in parallel
    }
  }
  
  @Test
  public void recurringExecutionFail() {
    try {
      distributor.scheduleTaskWithFixedDelay(new Object(), null, 1000, 100);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.scheduleTaskWithFixedDelay(new Object(), new TestRunnable(), -1, 100);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.scheduleTaskWithFixedDelay(new Object(), new TestRunnable(), 100, -1);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.scheduleTaskWithFixedDelay(null, new TestRunnable(), 100, 100);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void removeRunnableTest() {
    TestableScheduler scheduler = new TestableScheduler();
    KeyDistributedScheduler distributor = new KeyDistributedScheduler(scheduler);
    TestRunnable scheduleRunnable = new TestRunnable();
    TestRunnable submitScheduledRunnable = new TestRunnable();
    TestRunnable scheduleWithFixedDelayRunnable = new TestRunnable();
    
    distributor.schedule(scheduleRunnable, scheduleRunnable, 10);
    distributor.submitScheduled(submitScheduledRunnable, submitScheduledRunnable, 10);
    distributor.scheduleTaskWithFixedDelay(scheduleWithFixedDelayRunnable, scheduleWithFixedDelayRunnable, 10, 10);
    
    assertTrue(scheduler.remove(scheduleRunnable));
    assertTrue(scheduler.remove(submitScheduledRunnable));
    assertTrue(scheduler.remove(scheduleWithFixedDelayRunnable));
  }
  
  @Test
  public void removeCallableTest() {
    TestableScheduler scheduler = new TestableScheduler();
    KeyDistributedScheduler distributor = new KeyDistributedScheduler(scheduler);
    TestCallable submitScheduledCallable = new TestCallable();
    
    distributor.submitScheduled(submitScheduledCallable, submitScheduledCallable, 10);
    
    assertTrue(scheduler.remove(submitScheduledCallable));
  }
  
  private interface AddHandler {
    public void addTDRunnable(Object key, TDRunnable tdr);
  }
}
