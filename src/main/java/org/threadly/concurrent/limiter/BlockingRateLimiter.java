package org.threadly.concurrent.limiter;

import org.threadly.concurrent.NoThreadScheduler;

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
