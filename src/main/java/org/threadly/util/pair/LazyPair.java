package org.threadly.util.pair;

import java.util.concurrent.Callable;

import org.threadly.util.ExceptionUtils;

/**
 * <p>An implementation of {@link Pair} where the contents are only generated when they are 
 * requested through {@link #getLeft()} or {@link #getRight()}.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.4.0
 * @param <L> Type of 'left' object to be held
 * @param <R> Type of 'right' object to be held
 */
public class LazyPair<L, R> extends Pair<L, R> {
  protected volatile Callable<L> leftSupplier;
  protected volatile Callable<R> rightSupplier;
  protected volatile L left;
  protected volatile R right;
  
  /**
   * Construct a new lazy set pair.  If {@code null} is provided for either the left or right 
   * supplier, then {@link #getLeft()} and {@link #getRight()} respectively will forever return 
   * {@code null}.  Otherwise the provided supplier will be invoked once (if no exception is 
   * thrown) when needed to generate the value.  Multiple requests for the same value will block 
   * until the value is generated.  If an exception is thrown during generation it will be 
   * propogated to {@link #getLeft()} or 
   * {@link #getRight()}, and then on the next attempt to 
   * 
   * @param leftSupplier Supplier for the left side value
   * @param rightSupplier Supplier for the right side value
   */
  public LazyPair(Callable<L> leftSupplier, Callable<R> rightSupplier) {
    super(null, null);
    
    this.leftSupplier = leftSupplier;
    this.rightSupplier = rightSupplier;
  }
  
  @Override
  public L getLeft() {
    Callable<L> leftSupplier = this.leftSupplier;
    if (leftSupplier == null) {
      return left;
    }
    synchronized (leftSupplier) {
      if (this.leftSupplier == null) {
        return left;
      }
      try {
        left = leftSupplier.call();
      } catch (Exception e) {
        throw ExceptionUtils.makeRuntime(e);
      }
      this.leftSupplier = null;
    }
    return left;
  }

  @Override
  public R getRight() {
    Callable<R> rightSupplier = this.rightSupplier;
    if (rightSupplier == null) {
      return right;
    }
    synchronized (rightSupplier) {
      if (this.rightSupplier == null) {
        return right;
      }
      try {
        right = rightSupplier.call();
      } catch (Exception e) {
        throw ExceptionUtils.makeRuntime(e);
      }
      this.rightSupplier = null;
    }
    return right;
  }
  
  @Override
  public String toString() {
    return Pair.class.getSimpleName() + '[' + (leftSupplier == null ? left : "NOT_GENERATED") + 
             ',' + (rightSupplier == null ? right : "NOT_GENERATED") + ']';
  }
}
