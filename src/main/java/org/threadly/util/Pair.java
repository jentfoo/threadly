package org.threadly.util;

/**
 * <p>A simple tuple implementation (every library needs one, right?).  This is designed to be a 
 * minimal and light weight pair holder.</p>
 * 
 * @deprecated Moved to {@link org.threadly.util.pair.Pair}
 * 
 * @author jent - Mike Jensen
 * @since 4.4.0
 * @param <L> Type of 'left' object to be held
 * @param <R> Type of 'right' object to be held
 */
@Deprecated
public class Pair<L, R> extends org.threadly.util.pair.Pair<L, R> {
  /**
   * Constructs a new pair, providing the left and right objects to be held.
   * 
   * @param left Left reference
   * @param right Right reference
   */
  public Pair(L left, R right) {
    super(left, right);
  }
}
