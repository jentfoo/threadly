package org.threadly.concurrent;

public class SingleThreadSubScheduler extends AbstractSubmitterScheduler {// TODO - make priority scheduler
  private final SubmitterScheduler delegateScheduler;
  private final NoThreadScheduler noThreadScheduler;
  private final TickTask tickTask;
  
  public SingleThreadSubScheduler(SubmitterScheduler delegateScheduler) {
    this.delegateScheduler = delegateScheduler;
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
  
  private class TickTask extends ReschedulingOperation {
    protected TickTask(SubmitterScheduler delegateScheduler) {
      super(delegateScheduler, 0);
    }

    @Override
    protected void run() {
      noThreadScheduler.tick(null);
    }
  }
}
