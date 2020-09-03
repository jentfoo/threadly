package org.threadly.concurrent.wrapper.compatibility;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.SettableListenableFuture;
import org.threadly.util.Clock;

/**
 * Similar to {@link SameThreadSubmitterExecutor} except implements {@link ExecutorService}.  This
 * will run all provided tasks immediately in the same thread that is invoking into it.  This is 
 * different from calling the runnable directly only in that no exceptions will propagate out.  If 
 * an exception is thrown and the function does NOT return a {@link Future}, the thrown exception 
 * will be provided to {@link org.threadly.util.ExceptionUtils#handleException(Throwable)} to be 
 * handled.  Otherwise thrown exceptions will be represented by their returned {@link Future}.
 * 
 * @since 6.5
 */
public class SameThreadExecutorService implements ExecutorService {
  private static final SameThreadExecutorService INSTANCE;
  
  static {
    INSTANCE = new SameThreadExecutorService();
  }
  
  /**
   * Call to get a default instance of the {@link SameThreadExecutorService}.  Because there is 
   * no saved or shared state, the same instance can be reused as much as desired.
   * 
   * @return a static instance of SameThreadExecutorService
   */
  public static SameThreadExecutorService instance() {
    return INSTANCE;
  }
  
  SameThreadExecutorService() {
    // prevent construction, force instance()
  }
  
  @Override
  public void execute(Runnable task) {
    SameThreadSubmitterExecutor.instance().execute(task);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return SameThreadSubmitterExecutor.instance().submit(task);
  }

  @Override
  public Future<?> submit(Runnable task) {
    return SameThreadSubmitterExecutor.instance().submit(task);
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return SameThreadSubmitterExecutor.instance().submit(task, result);
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> c) {
    List<Future<T>> result = new ArrayList<>(c.size());
    // added futures from SameThreadSubmitterExecutor are all completed
    c.forEach((t) -> result.add(SameThreadSubmitterExecutor.instance().submit(t)));
    return result;
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> c, long time, TimeUnit unit) {
    long timeoutMillis;
    long startTime;
    if (time > 0 && time != Long.MAX_VALUE) {
      timeoutMillis = unit.toMillis(time);
      startTime = Clock.accurateForwardProgressingMillis();
    } else {
      timeoutMillis = startTime = -1;
    }
    
    List<Future<T>> result = new ArrayList<>(c.size());
    SettableListenableFuture<T> canceledSLF = null;
    for (Callable<T> task : c) {
      if (timeoutMillis > -1 && Clock.lastKnownForwardProgressingMillis() - startTime > timeoutMillis) {
        if (canceledSLF == null) {
          canceledSLF = new SettableListenableFuture<>();
          canceledSLF.cancel(false);
        }
        result.add(canceledSLF);
      } else {
        result.add(SameThreadSubmitterExecutor.instance().submit(task));
      }
    }
    return result;
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> c) throws ExecutionException {
    try {
      return invokeAny(c, 0, null);
    } catch (TimeoutException e) {
      // not possible with no timeout
      throw new RuntimeException(e);
    }
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> c, long time, TimeUnit unit) throws ExecutionException,
                                                                                               TimeoutException {
    long timeoutMillis;
    long startTime;
    if (time > 0 && time != Long.MAX_VALUE) {
      timeoutMillis = unit.toMillis(time);
      startTime = Clock.accurateForwardProgressingMillis();
    } else {
      timeoutMillis = startTime = -1;
    }
    
    Throwable firstFailure = null;
    for (Callable<T> task : c) {
      if (timeoutMillis > -1 && Clock.lastKnownForwardProgressingMillis() - startTime > timeoutMillis) {
        TimeoutException ex = new TimeoutException("Timeout, first failure as cause");
        ex.initCause(firstFailure);
        throw ex;
      }
      
      ListenableFuture<T> lf = SameThreadSubmitterExecutor.instance().submit(task);
      try {
        if (lf.getFailure() == null) {
          return lf.get();
        } else if (firstFailure == null) {
          firstFailure = lf.getFailure();
        }
      } catch (ExecutionException | InterruptedException e) {
        // not possible
        throw new RuntimeException(e);
      }
    }
    
    if (firstFailure == null) {
      throw new IllegalArgumentException("No tasks");
    }
    throw new ExecutionException(firstFailure);
  }

  @Override
  public boolean isShutdown() {
    return false;
  }

  @Override
  public boolean isTerminated() {
    return false;
  }

  @Override
  public void shutdown() {
    throw new UnsupportedOperationException("Static instance, can't be shutdown");
  }

  @Override
  public List<Runnable> shutdownNow() {
    throw new UnsupportedOperationException("Static instance, can't be shutdown");
  }

  @Override
  public boolean awaitTermination(long time, TimeUnit unit) {
    throw new UnsupportedOperationException("Will never terminate");
  }
}
