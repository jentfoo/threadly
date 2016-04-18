package org.threadly.util.pair;

/**
 * <p>A special type of {@link Pair} which allows the stored references to be updated after 
 * creation.  The stored references are {@code volatile} so values set from different threads 
 * will be able to be read without synchronization.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.4.0
 * @param <L> Type of 'left' object to be held
 * @param <R> Type of 'right' object to be held
 */
public class VolatilePair<L, R> extends MutablePair<L, R> {
  protected volatile L left;
  protected volatile R right;
  
  /**
   * Constructs a new mutable pair with the left and right references defaulted to be {@code null}.
   */
  public VolatilePair() {
    super(null, null);
  }

  /**
   * Constructs a new mutable pair, providing the left and right objects to be held.
   * 
   * @param left Left reference
   * @param right Right reference
   */
  public VolatilePair(L left, R right) {
    super(null, null);
    
    this.left = left;
    this.right = right;
  }
  
  @Override
  public void setLeft(L left) {
    this.left = left;
  }

  @Override
  public void setRight(R right) {
    this.right = right;
  }
  
  @Override
  public L getLeft() {
    return left;
  }
  
  @Override
  public R getRight() {
    return right;
  }
}
