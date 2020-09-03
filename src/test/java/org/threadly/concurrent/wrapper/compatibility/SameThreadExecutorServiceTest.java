package org.threadly.concurrent.wrapper.compatibility;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterExecutorInterfaceTest;
import org.threadly.concurrent.wrapper.SubmitterExecutorAdapter;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.util.StringUtils;

@SuppressWarnings("javadoc")
public class SameThreadExecutorServiceTest extends SubmitterExecutorInterfaceTest {
  private SameThreadExecutorService executor;
  
  @BeforeClass
  public static void classSetup() {
    setIgnoreExceptionHandler();
  }
  
  @Before
  @SuppressWarnings("deprecation")
  public void setup() {
    executor = new SameThreadExecutorService();
  }
  
  @After
  public void cleanup() {
    executor = null;
  }
  
  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new ExecutorFactory();
  }
  
  @Test
  @Override
  public void executeTest() {
    TestRunnable tr = new TestRunnable();
    executor.execute(tr);
    
    assertTrue(tr.ranOnce());
    assertTrue(tr.executedThread == Thread.currentThread());
    
    super.executeTest();
  }
  
  @Test
  @Override
  public void submitRunnableTest() throws InterruptedException, ExecutionException {
    TestRunnable tr = new TestRunnable();
    Future<?> future = executor.submit(tr);
    
    assertTrue(tr.ranOnce());
    assertTrue(tr.executedThread == Thread.currentThread());
    assertTrue(future.isDone());
    assertTrue(future.get() == null);

    super.submitRunnableTest();
  }
  
  @Test
  @Override
  public void submitRunnableWithResultTest() throws InterruptedException, ExecutionException {
    Object result = new Object();
    TestRunnable tr = new TestRunnable();
    Future<?> future = executor.submit(tr, result);
    
    assertTrue(tr.ranOnce());
    assertTrue(tr.executedThread == Thread.currentThread());
    assertTrue(future.isDone());
    assertTrue(future.get() == result);
    
    super.submitRunnableWithResultTest();
  }
  
  @Test
  public void staticInstanceTest() {
    assertNotNull(SameThreadExecutorService.instance());
  }
  
  @Test
  public void invokeAllTest() throws InterruptedException, ExecutionException {
    String result1 = StringUtils.makeRandomString(8);
    String result2 = StringUtils.makeRandomString(8);
    List<Callable<String>> tasks = new ArrayList<>();
    tasks.add(() -> result1);
    tasks.add(() -> result2);
    
    List<Future<String>> futures = executor.invokeAll(tasks);
    
    assertEquals(tasks.size(), futures.size());
    assertEquals(result1, futures.get(0).get());
    assertEquals(result2, futures.get(1).get());
  }
  
  @Test
  public void invokeAllTimeoutTest() {
    List<Callable<Void>> tasks = new ArrayList<>();
    tasks.add(() -> {
      TestUtils.blockTillClockAdvances();
      TestUtils.blockTillClockAdvances();
      return null;
    });
    tasks.add(() -> {
      return null;
    });
    
    List<Future<Void>> futures = executor.invokeAll(tasks, 1, TimeUnit.MILLISECONDS);
    
    assertEquals(tasks.size(), futures.size());
    assertTrue(futures.get(1).isCancelled());
  }
  
  @Test
  public void invokeAnyFirstResultTest() throws ExecutionException {
    String result = StringUtils.makeRandomString(8);
    AtomicBoolean secondRan = new AtomicBoolean(false);
    List<Callable<String>> tasks = new ArrayList<>();
    tasks.add(() -> result);
    tasks.add(() -> { secondRan.set(true); return null; });
    
    String invokeResult = executor.invokeAny(tasks);
    
    assertEquals(result, invokeResult);
    assertFalse(secondRan.get());
  }
  
  @Test
  public void invokeAnySecondResultTest() throws ExecutionException {
    String result = StringUtils.makeRandomString(8);
    List<Callable<String>> tasks = new ArrayList<>();
    tasks.add(() -> { throw new RuntimeException(); });
    tasks.add(() -> result);
    
    String invokeResult = executor.invokeAny(tasks);

    assertEquals(result, invokeResult);
  }
  
  @Test
  public void invokeAnyTimeoutTest() {
    List<Callable<Void>> tasks = new ArrayList<>();
    tasks.add(() -> {
      TestUtils.blockTillClockAdvances();
      TestUtils.blockTillClockAdvances();
      throw new RuntimeException();
    });
    tasks.add(() -> {
      return null;
    });
    

    try {
      executor.invokeAny(tasks, 1, TimeUnit.MILLISECONDS);
      fail("Exception expected");
    } catch (ExecutionException e) {
      fail("Unexpected failure: " + e);
    } catch (TimeoutException e) {
      // expected
    }
  }
  
  @Test
  public void isShutdownTest() {
    assertFalse(executor.isShutdown());
  }
  
  @Test
  public void isTerminatedTest() {
    assertFalse(executor.isTerminated());
  }
  
  @Test (expected = UnsupportedOperationException.class)
  public void shutdownFail() {
    executor.shutdown();
    fail("Exception should have thrown");
  }
  
  @Test (expected = UnsupportedOperationException.class)
  public void shutdownNowFail() {
    executor.shutdownNow();
    fail("Exception should have thrown");
  }
  
  @Test (expected = UnsupportedOperationException.class)
  public void awaitTerminationFail() {
    executor.awaitTermination(10, TimeUnit.SECONDS);
    fail("Exception should have thrown");
  }
  
  private static class TestRunnable extends org.threadly.test.concurrent.TestRunnable {
    private Thread executedThread = null;
    
    @Override
    public void handleRunStart() {
      executedThread = Thread.currentThread();
    }
  }
  
  private static class ExecutorFactory implements SubmitterExecutorFactory {
    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      return SubmitterExecutorAdapter.adaptExecutor(new SameThreadExecutorService());
    }

    @Override
    public void shutdown() {
      // ignored
    }
  }
}
