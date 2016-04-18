package org.threadly.util.pair;

@SuppressWarnings("javadoc")
public class VolatilePairTest extends MutablePairTest {
  @Override
  protected <T> VolatilePair<T, T> makePair(T left, T right) {
    return new VolatilePair<T, T>(left, right);
  }
}
