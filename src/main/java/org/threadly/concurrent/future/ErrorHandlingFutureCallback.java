package org.threadly.concurrent.future;

/**
 * Implementation of {@link FutureCallback} where if there is an exception thrown while handling 
 * the result it also will be provided to {@link #handleFailure(Throwable)} rather than it being 
 * sent to the uncaught exception handler.
 * 
 * @since 5.41
 * @param <T> Type of result handled by callback
 */
public abstract class ErrorHandlingFutureCallback<T> implements FutureCallback<T> {
  @Override
  public final void handleResult(T result) {
    try {
      handleResultOrThrow(result);
    } catch (Throwable t) {
      handleFailure(t);
    }
  }
  
  /**
   * Implement this as an alternative to the normal {@link #handleResult(Object)}.
   * 
   * @param result Result that was provided from the future
   * @throws Exception Exception which may be thrown while handling the result
   */
  public abstract void handleResultOrThrow(T result) throws Exception;
}
