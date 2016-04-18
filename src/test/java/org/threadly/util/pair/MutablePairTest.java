package org.threadly.util.pair;

import static org.junit.Assert.*;

import org.junit.Test;
import org.threadly.util.StringUtils;

@SuppressWarnings("javadoc")
public class MutablePairTest extends PairTest {
  @Override
  protected <T> MutablePair<T, T> makePair(T left, T right) {
    return new MutablePair<T, T>(left, right);
  }
  
  @Test
  public void setAndGetLeftTest() {
    String strValue = StringUtils.makeRandomString(5);
    MutablePair<String, String> p = new MutablePair<String, String>();
    p.setLeft(strValue);
    assertEquals(strValue, p.getLeft());
  }
  
  @Test
  public void setAndGetRightTest() {
    String strValue = StringUtils.makeRandomString(5);
    MutablePair<String, String> p = new MutablePair<String, String>();
    p.setRight(strValue);
    assertEquals(strValue, p.getRight());
  }
}
