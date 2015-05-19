package org.threadly.concurrent.limiter;

import org.threadly.concurrent.NoThreadScheduler;

/**
 * <p>An implementation which has the same goals as {@link RateLimiterExecutor} but providing a 
 * blocking design.  Instead of scheduling tasks which will execute at a given rate, this limiter 
 * will block on {@link #acquire(int)} until they are able to be run for the given rate.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.0.0
 */
public class BlockingRateLimiter {
  private final NoThreadScheduler scheduler;
  private final RateLimiterExecutor rateLimiter;
  
  public BlockingRateLimiter(int permitsPerSecond) {
    scheduler = new NoThreadScheduler();
    rateLimiter = new RateLimiterExecutor(scheduler, permitsPerSecond);
  }
  
  public void acquire() throws InterruptedException {
    acquire(1);
  }
  
  public void acquire(int permits) throws InterruptedException {
    rateLimiter.execute(permits, new Runnable() {
      @Override
      public void run() {
        // no-op
      }
    });
    
    scheduler.blockingTick(null);
  }
}
