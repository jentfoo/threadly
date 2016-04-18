package org.threadly.util.pair;

import static org.junit.Assert.*;

import org.junit.Test;
import org.threadly.util.StringUtils;

@SuppressWarnings("javadoc")
public class MutablePairTest extends PairTest {
  protected <T> MutablePair<T, T> makePair() {
    return makePair(null, null);
  }
  
  @Override
  protected <T> MutablePair<T, T> makePair(T left, T right) {
    return new MutablePair<T, T>(left, right);
  }
  
  @Test
  public void setAndGetLeftTest() {
    String strValue = StringUtils.makeRandomString(5);
    MutablePair<String, String> p = makePair();
    assertNull(p.getLeft());
    p.setLeft(strValue);
    assertEquals(strValue, p.getLeft());
  }
  
  @Test
  public void setAndGetRightTest() {
    String strValue = StringUtils.makeRandomString(5);
    MutablePair<String, String> p = makePair();
    assertNull(p.getRight());
    p.setRight(strValue);
    assertEquals(strValue, p.getRight());
  }
}
