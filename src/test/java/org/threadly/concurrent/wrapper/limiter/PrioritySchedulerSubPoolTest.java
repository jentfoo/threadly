package org.threadly.concurrent.wrapper.limiter;

import org.threadly.concurrent.AbstractPriorityScheduler;
import org.threadly.concurrent.AbstractPrioritySchedulerTest;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;

@SuppressWarnings("javadoc")
public class PrioritySchedulerSubPoolTest extends AbstractPrioritySchedulerTest {
  @Override
  protected AbstractPrioritySchedulerFactory getAbstractPrioritySchedulerFactory() {
    return new PrioritySchedulerSubPoolFactory();
  }
  
  @Override
  protected boolean isSingleThreaded() {
    return false;
  }

  protected static class PrioritySchedulerSubPoolFactory implements AbstractPrioritySchedulerFactory {
    private final PrioritySchedulerFactory schedulerFactory = new PrioritySchedulerFactory();
    
    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }
    
    @Override
    public AbstractPriorityScheduler makeAbstractPriorityScheduler(int poolSize, 
                                                                   TaskPriority defaultPriority, 
                                                                   long maxWaitForLowPriority) {
      return new PrioritySchedulerSubPool(schedulerFactory.makeAbstractPriorityScheduler(poolSize), 
                                          poolSize, defaultPriority, maxWaitForLowPriority);
    }
    
    @Override
    public AbstractPriorityScheduler makeAbstractPriorityScheduler(int poolSize) {
      return new PrioritySchedulerSubPool(schedulerFactory.makeAbstractPriorityScheduler(poolSize), poolSize);
    }
  }
}
