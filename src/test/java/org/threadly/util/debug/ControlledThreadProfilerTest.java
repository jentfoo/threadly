package org.threadly.util.debug;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.CentralThreadlyPool;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.util.ExceptionUtils;
import org.threadly.util.debug.Profiler.ThreadSample;

@SuppressWarnings("javadoc")
public class ControlledThreadProfilerTest extends ProfilerTest {
  private static final int WAIT_TIME_FOR_COLLECTION = 50;
  
  private ControlledThreadProfiler ctProfiler;
  
  @Before
  @Override
  public void setup() {
    ctProfiler = new ControlledThreadProfiler(POLL_INTERVAL, (p) -> startFutureResultSupplier.get());
    profiler = ctProfiler;
    startFutureResultSupplier = profiler::dump;
  }
  
  @Override
  protected void profilingExecutor(Executor executor) {
    AtomicReference<Thread> tAR = new AtomicReference<>();
    executor.execute(() -> tAR.set(Thread.currentThread()));
    new TestCondition(() -> tAR.get() != null).blockTillTrue();
    ctProfiler.addProfiledThread(tAR.get());
  }
  
  @Test
  @Override
  public void constructorTest() {
    int testPollInterval = Profiler.DEFAULT_POLL_INTERVAL_IN_MILLIS * 10;
    ControlledThreadProfiler p;
    
    p = new ControlledThreadProfiler();
    assertNotNull(p.controledThreadStore.threadTraces);
    assertTrue(p.controledThreadStore.threadTraces.isEmpty());
    assertEquals(Profiler.DEFAULT_POLL_INTERVAL_IN_MILLIS, p.controledThreadStore.pollIntervalInMs);
    assertNull(p.controledThreadStore.collectorThread.get());
    assertNull(p.controledThreadStore.dumpingThread);
    assertNotNull(p.startStopLock);
    assertTrue(p.controledThreadStore.profiledThreads.isEmpty());
    
    p = new ControlledThreadProfiler(testPollInterval);
    assertNotNull(p.controledThreadStore.threadTraces);
    assertTrue(p.controledThreadStore.threadTraces.isEmpty());
    assertEquals(testPollInterval, p.controledThreadStore.pollIntervalInMs);
    assertNull(p.controledThreadStore.collectorThread.get());
    assertNull(p.controledThreadStore.dumpingThread);
    assertNotNull(p.startStopLock);
    assertTrue(p.controledThreadStore.profiledThreads.isEmpty());
  }
  
  @Test
  public void getProfileThreadsIteratorEmptyTest() {
    Iterator<?> it = profiler.pStore.getProfileThreadsIterator();
    
    assertNotNull(it);
    assertFalse(it.hasNext());
  }
  
  @Test
  @Override
  public void getProfileThreadsIteratorTest() {
    ctProfiler.addProfiledThread(Thread.currentThread());
    Iterator<? extends ThreadSample> it = profiler.pStore.getProfileThreadsIterator();
    
    assertNotNull(it);
    assertTrue(it.hasNext());
    assertTrue(it.next().getThread() == Thread.currentThread());
    // should only have the one added thread
    assertFalse(it.hasNext());
  }
  
  @Test
  @Override
  public void profileThreadsIteratorRemoveFail() {
    // not relevant for this class
  }
  
  @Override
  @SuppressWarnings("unused")
  @Test (expected = IllegalArgumentException.class)
  public void constructorFail() {
    new ControlledThreadProfiler(-1);
  }
  
  @Test
  public void addProfiledThreadTest() {
    assertTrue(ctProfiler.controledThreadStore.profiledThreads.isEmpty());
    
    Thread currentThread = Thread.currentThread();
    ctProfiler.addProfiledThread(currentThread);
    
    assertEquals(1, ctProfiler.controledThreadStore.profiledThreads.size());
  }
  
  @Test
  public void addProfiledThreadDuplicateThreadTest() {
    assertEquals(0, ctProfiler.controledThreadStore.profiledThreads.size());
    
    Thread currentThread = Thread.currentThread();
    ctProfiler.addProfiledThread(currentThread);
    
    assertEquals(1, ctProfiler.controledThreadStore.profiledThreads.size());
    
    ctProfiler.addProfiledThread(currentThread);
    // nothing should have changed
    assertEquals(1, ctProfiler.controledThreadStore.profiledThreads.size());
  }
  
  @Test
  public void addNullProfiledThreadTest() {
    assertTrue(ctProfiler.controledThreadStore.profiledThreads.isEmpty());
    
    ctProfiler.addProfiledThread(null);
    
    assertTrue(ctProfiler.controledThreadStore.profiledThreads.isEmpty());
  }
  
  @Test
  public void removeProfiledThreadTest() {
    assertTrue(ctProfiler.controledThreadStore.profiledThreads.isEmpty());
    
    Thread thread1 = new Thread();
    Thread thread2 = new Thread();
    ctProfiler.addProfiledThread(thread1);
    ctProfiler.addProfiledThread(thread2);
    
    assertEquals(2, ctProfiler.controledThreadStore.profiledThreads.size());
    
    assertTrue(ctProfiler.removeProfiledThread(thread1));
    
    assertEquals(1, ctProfiler.controledThreadStore.profiledThreads.size());
  }
  
  @Test
  @Override
  public void startWithoutExecutorTest() {
    ctProfiler.addProfiledThread(Thread.currentThread());
    
    super.startWithoutExecutorTest();
  }
  
  @Test
  @Override
  public void startWithExecutorTest() {
    ctProfiler.addProfiledThread(Thread.currentThread());
    
    super.startWithExecutorTest();
  }
  
  @Test
  @Override
  public void startWithSameThreadExecutorTest() throws InterruptedException, TimeoutException {
    try {
      CentralThreadlyPool.lowPriorityPool()
                         .submit(() -> ctProfiler.addProfiledThread(Thread.currentThread())).get();
    } catch (ExecutionException e) {
      throw ExceptionUtils.makeRuntime(e.getCause());
    }
    
    super.startWithSameThreadExecutorTest();
  }
  
  @Test
  @Override
  public void startWithSameThreadExecutorAndTimeoutTest() {
    try {
      CentralThreadlyPool.lowPriorityPool()
                         .submit(() -> ctProfiler.addProfiledThread(Thread.currentThread())).get();
    } catch (ExecutionException e) {
      throw ExceptionUtils.makeRuntime(e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    
    super.startWithSameThreadExecutorAndTimeoutTest();
  }
  
  @Test
  public void getProfiledThreadCountTest() {
    int testThreadCount = 10;
    assertEquals(0, ctProfiler.getProfiledThreadCount());
    
    List<Thread> addedThreads = new ArrayList<>(testThreadCount);
    for (int i = 0; i < testThreadCount; i++) {
      Thread t = new Thread();
      addedThreads.add(t);
      ctProfiler.addProfiledThread(t);
      assertEquals(i + 1, ctProfiler.getProfiledThreadCount());
    }
    
    Iterator<Thread> it = addedThreads.iterator();
    int removedCount = 0;
    while (it.hasNext()) {
      Thread t = it.next();
      ctProfiler.removeProfiledThread(t);
      removedCount++;
      assertEquals(testThreadCount - removedCount, ctProfiler.getProfiledThreadCount());
    }
  }
  @Test
  @Override
  public void resetTest() {
    ctProfiler.addProfiledThread(Thread.currentThread());
    
    super.resetTest();
  }
  
  @Test
  public void dumpStoppedStringEmptyTest() {
    profiler.start();
    
    TestUtils.sleep(WAIT_TIME_FOR_COLLECTION);
    
    profiler.stop();
    
    String resultStr = profiler.dump();
    
    // no dump since no threads set to track
    assertEquals(0, resultStr.length());
  }
  
  @Test
  @Override
  public void dumpStoppedStringTest() {
    ctProfiler.addProfiledThread(Thread.currentThread());
    
    super.dumpStoppedStringTest();
  }
  
  @Test
  public void dumpStoppedOutputStreamEmptyTest() {
    profiler.start();
    
    TestUtils.sleep(WAIT_TIME_FOR_COLLECTION);
    
    profiler.stop();
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    profiler.dump(out);
    
    String resultStr = out.toString();
    
    // no dump since no threads set to track
    assertEquals(0, resultStr.length());
  }
  
  @Test
  @Override
  public void dumpStoppedOutputStreamTest() {
    ctProfiler.addProfiledThread(Thread.currentThread());
    
    super.dumpStoppedOutputStreamTest();
  }
  
  @Test
  public void dumpStringEmptyTest() {
    profiler.start();
    
    TestUtils.sleep(WAIT_TIME_FOR_COLLECTION);
    
    String resultStr = profiler.dump();
    
    // no dump since no threads set to track
    assertEquals(0, resultStr.length());
  }
  
  @Test
  @Override
  public void dumpStringTest() {
    ctProfiler.addProfiledThread(Thread.currentThread());
    
    super.dumpStringTest();
  }
  
  @Test
  public void dumpOutputStreamEmptyTest() {
    profiler.start();
    
    TestUtils.sleep(WAIT_TIME_FOR_COLLECTION);
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    profiler.dump(out);
    
    String resultStr = out.toString();
    
    // no dump since no threads set to track
    assertEquals(0, resultStr.length());
  }
  
  @Test
  @Override
  public void dumpOutputStreamTest() {
    ctProfiler.addProfiledThread(Thread.currentThread());
    
    super.dumpOutputStreamTest();
  }
  
  @Test
  @Override
  public void dumpStringOnlySummaryTest() {
    ctProfiler.addProfiledThread(Thread.currentThread());
    
    super.dumpStringOnlySummaryTest();
  }
  
  @Test
  @Override
  public void idlePrioritySchedulerTest() {
    // ignored because we can't easily access both threads in the scheduler
  }
  
  @Test
  @Override
  public void idlePrioritySchedulerWithExceptionHandlerTest() {
    // ignored because we can't easily access both threads in the scheduler
  }
}
