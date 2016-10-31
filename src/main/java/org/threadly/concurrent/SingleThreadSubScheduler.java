package org.threadly.concurrent;

import java.util.concurrent.Executor;

public class SingleThreadSubScheduler extends AbstractSubmitterScheduler {// TODO - make priority scheduler
  protected final NoThreadScheduler noThreadScheduler;
  protected final TickTask tickTask;
  
  public SingleThreadSubScheduler(Executor delegateScheduler) {
    this.noThreadScheduler = new NoThreadScheduler();
    this.tickTask = new TickTask(delegateScheduler);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay) {
    noThreadScheduler.scheduleWithFixedDelay(task, initialDelay, recurringDelay);
    tickTask.signalToRun();
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period) {
    noThreadScheduler.scheduleAtFixedRate(task, initialDelay, period);
    tickTask.signalToRun();
  }

  @Override
  protected void doSchedule(Runnable task, long delayInMillis) {
    noThreadScheduler.doSchedule(task, delayInMillis);
    tickTask.signalToRun();
  } 
  
  public int getQueuedTaskCount() {
    return noThreadScheduler.getQueuedTaskCount();
  }
  
  private class TickTask extends ReschedulingOperation {
    protected TickTask(Executor delegateScheduler) {
      super(delegateScheduler, 0);
    }

    @Override
    protected void run() {
      noThreadScheduler.tick(null);
    }
  }
}
