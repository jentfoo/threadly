package org.threadly.test.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Test;
import org.threadly.concurrent.SingleThreadScheduler;
import org.threadly.concurrent.UncheckedInterruptedException;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class TestUtilTest {
  @After
  public void cleanup() {
    Thread.interrupted(); // reset interrupted status
  }
  
  @Test
  public void sleepTest() {
    long start = Clock.accurateForwardProgressingMillis();
    TestUtils.sleep(DELAY_TIME);
    long end = Clock.accurateForwardProgressingMillis();
    assertTrue(end - start >= (DELAY_TIME - ALLOWED_VARIANCE));
  }
  
  @Test
  public void sleepInterruptedTest() {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    ListenableFuture<?> interruptFuture = null;
    try {
      final AtomicBoolean aboutToSleep = new AtomicBoolean(false);
      final Thread testThread = Thread.currentThread();
      interruptFuture = sts.submit(new Runnable() {
        @Override
        public void run() {
          while (! aboutToSleep.get()) {
            // spin
          }
          TestUtils.sleep(DELAY_TIME);
          
          testThread.interrupt();
        }
      });
      
      aboutToSleep.set(true);
      TestUtils.sleep(1000 * 20);
      fail("Exception should have thrown");
    } catch (UncheckedInterruptedException e) {
      // expected
      assertTrue(Thread.interrupted());
    } finally {
      sts.shutdownNow();
      if (interruptFuture != null) {
        interruptFuture.cancel(true);
      }
    }
  }
  
  @Test
  public void blockTillClockAdvancesTest() {
    long before = Clock.accurateTimeMillis();
    TestUtils.blockTillClockAdvances();
    assertTrue(Clock.lastKnownTimeMillis() != before);
  }
}
