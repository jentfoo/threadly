package org.threadly.concurrent;

import java.util.concurrent.RejectedExecutionException;

/**
 * <p>Interface to be invoked when a pool can not accept a task for any reason.  In general pools 
 * in threadly will never reject a task.  However there are wrappers and other behavior modifiers 
 * which do things like queue limits and may need to reject a task.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.8.0
 */
public interface RejectedExecutionHandler {
  /**
   * Typically the default handler for most pool implementations.  This handler will only throw a 
   * {@link RejectedExecutionException} to indicate the failure in accepting the task.
   */
  public static final RejectedExecutionHandler THROW_REJECTED_EXECUTION_EXCEPTION = new RejectedExecutionHandler() {
    @Override
    public void handleRejectedTask(Runnable task) {
      throw new RejectedExecutionException("Could not execute task: " + task);
    }
  };
  
  /**
   * Handle the task that was unable to be accepted by a pool.  This function may simply swallow 
   * the task, log, queue in a different way, or throw an exception.  Note that this task may not 
   * be the original task submitted, but an instance of 
   * {@link org.threadly.concurrent.future.ListenableFutureTask} or something similar to convert 
   * Callables or handle other future needs.  In any case the comparison of tasks should be 
   * possible using {@link ContainerHelper}.
   * 
   * @param task Task which could not be submitted to the pool
   */
  public void handleRejectedTask(Runnable task);
}
