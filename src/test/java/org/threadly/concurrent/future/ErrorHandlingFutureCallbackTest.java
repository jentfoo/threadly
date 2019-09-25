package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class ErrorHandlingFutureCallbackTest {
  @Test
  public void normalResultTest() {
    Object result = new Object();
    TestErrorHandlingFutureCallback fc = new TestErrorHandlingFutureCallback(false);
    
    fc.handleResult(result);
    
    assertTrue(result == fc.getLastResult());
  }
  
  @Test
  public void normalFailureTest() {
    Exception failure = new Exception();
    TestErrorHandlingFutureCallback fc = new TestErrorHandlingFutureCallback(true);
    
    fc.handleFailure(failure);
    
    assertTrue(failure == fc.getLastFailure());
  }
  
  @Test
  public void failureHandlingResultTest() {
    TestErrorHandlingFutureCallback fc = new TestErrorHandlingFutureCallback(true);
    
    fc.handleResult(null);
    
    assertNotNull(fc.getLastFailure());
  }
  
  public class TestErrorHandlingFutureCallback extends ErrorHandlingFutureCallback<Object> {
    private final boolean throwOnResult;
    private Object lastResult = null;
    private Throwable lastFailure = null;
    
    public TestErrorHandlingFutureCallback(boolean throwOnResult) {
      this.throwOnResult = throwOnResult;
    }
    
    public Object getLastResult() {
      return lastResult;
    }
    
    public Throwable getLastFailure() {
      return lastFailure;
    }

    @Override
    public void handleResultOrThrow(Object result) throws Exception {
      if (throwOnResult) {
        throw new Exception();
      }
      lastResult = result;
    }

    @Override
    public void handleFailure(Throwable t) {
      lastFailure = t;
    }
  }
}
