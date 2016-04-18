package org.threadly.util.pair;

import static org.junit.Assert.*;

import java.util.concurrent.Callable;

import org.junit.Test;
import org.threadly.util.StringUtils;

@SuppressWarnings("javadoc")
public class LazyPairTest extends PairTest {
  @Override
  protected <T> LazyPair<T, T> makePair(final T left, final T right) {
    return new LazyPair<T, T>(new Callable<T>() {
      @Override
      public T call() {
        return left;
      }
    }, new Callable<T>() {
      @Override
      public T call() {
        return right;
      }
    });
  }
  
  @Test
  @Override
  public void toStringTest() {
    String leftStr = StringUtils.makeRandomString(10);
    String rightStr = StringUtils.makeRandomString(10);
    
    Pair<String, String> p = makePair(leftStr, rightStr);
    String result = p.toString();
    
    // should not contain either field yet
    assertFalse(result.contains(leftStr));
    assertFalse(result.contains(rightStr));
    
    // populate fields
    p.getLeft(); p.getRight();
    result = p.toString();
    
    assertTrue(result.contains(leftStr));
    assertTrue(result.contains(rightStr));
    assertTrue(result.indexOf(leftStr) < result.indexOf(rightStr));
  }
}
