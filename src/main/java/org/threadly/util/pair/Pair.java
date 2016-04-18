package org.threadly.util.pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * <p>A simple tuple implementation (every library needs one, right?).  This is designed to be a 
 * minimal and light weight pair holder.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.4.0
 * @param <L> Type of 'left' object to be held
 * @param <R> Type of 'right' object to be held
 */
public class Pair<L, R> {
  private static final short LEFT_PRIME = 13;
  private static final short RIGHT_PRIME = 31;
  
  // TODO - in threadly 5.0.0 make an easy way to apply a functional to either left or right
  
  /**
   * Collect all the non-null left references into a new List.  A simple implementation which 
   * iterates over a source collection and collects all non-null left references into a new list 
   * that can be manipulated or referenced.
   *  
   * @param <T> Type of object held as pair's left reference
   * @param source Source collection of pairs
   * @return New list that contains non-null left references
   */
  public static <T> List<T> collectLeft(Collection<? extends Pair<? extends T, ?>> source) {
    List<T> result = new ArrayList<T>(source.size());
    for (Pair<? extends T, ?> p : source) {
      T left = p.getLeft();
      if (left != null) {
        result.add(left);
      }
    }
    return result;
  }

  /**
   * Collect all the non-null right references into a new List.  A simple implementation which 
   * iterates over a source collection and collects all non-null right references into a new list 
   * that can be manipulated or referenced.
   *  
   * @param <T> Type of object held as pair's right reference
   * @param source Source collection of pairs
   * @return New list that contains non-null right references
   */
  public static <T> List<T> collectRight(Collection<? extends Pair<?, ? extends T>> source) {
    List<T> result = new ArrayList<T>(source.size());
    for (Pair<?, ? extends T> p : source) {
      T right = p.getRight();
      if (right != null) {
        result.add(right);
      }
    }
    return result;
  }
  
  /**
   * Simple search to see if a collection of pairs contains a given left value.  It is assumed 
   * that the iterator will not return any {@code null} elements.
   * 
   * @param search Iterable to search over
   * @param value Value to be searching for from left elements
   * @return {@code true} if the value is found as a left element from the iterable provided
   */
  public static boolean containsLeft(Iterable<? extends Pair<?, ?>> search, Object value) {
    for (Pair<?, ?> p : search) {
      Object left = p.getLeft();
      if (left == null) {
        if (value == null) {
          return true;
        }
      } else if (left.equals(value)) {
        return true;
      }
    }
    
    return false;
  }

  /**
   * Simple search to see if a collection of pairs contains a given right value.  It is assumed 
   * that the iterator will not return any {@code null} elements.
   * 
   * @param search Iterable to search over
   * @param value Value to be searching for from right elements
   * @return {@code true} if the value is found as a right element from the iterable provided
   */
  public static boolean containsRight(Iterable<? extends Pair<?, ?>> search, Object value) {
    for (Pair<?, ?> p : search) {
      Object right = p.getRight();
      if (right == null) {
        if (value == null) {
          return true;
        }
      } else if (right.equals(value)) {
        return true;
      }
    }
    
    return false;
  }
  
  /**
   * Get the right side of a pair by searching for a matching left side.  This iterates over the 
   * provided source, and once the first pair with a left that matches (via 
   * {@link Object#equals(Object)}), the right side is returned.  If no match is found, 
   * {@code null} will be returned.  Although the implementer must be aware that since nulls can 
   * be kept kept inside pairs, that does not strictly indicate a match failure.
   * 
   * @param <T> Type of object held as pair's right reference
   * @param search Iteratable to search through looking for a match
   * @param left Object to be looking searching for as a left reference
   * @return Corresponding right reference or {@code null} if none is found
   */
  public static <T> T getRightFromLeft(Iterable<? extends Pair<?, ? extends T>> search, Object left) {
    for (Pair<?, ? extends T> p : search) {
      Object leftVal = p.getLeft();
      if (leftVal == null) {
        if (left == null) {
          return p.getRight();
        }
      } else if (leftVal.equals(left)) {
        return p.getRight();
      }
    }
    
    return null;
  }
  
  /**
   * Get the left side of a pair by searching for a matching right side.  This iterates over the 
   * provided source, and once the first pair with a right that matches (via 
   * {@link Object#equals(Object)}), the left side is returned.  If no match is found, 
   * {@code null} will be returned.  Although the implementer must be aware that since nulls can 
   * be kept kept inside pairs, that does not strictly indicate a match failure.
   * 
   * @param <T> Type of object held as pair's left reference
   * @param search Iteratable to search through looking for a match
   * @param right Object to be looking searching for as a left reference
   * @return Corresponding left reference or {@code null} if none is found
   */
  public static <T> T getLeftFromRight(Iterable<? extends Pair<? extends T, ?>> search, Object right) {
    for (Pair<? extends T, ?> p : search) {
      Object rightVal = p.getRight();
      if (rightVal == null) {
        if (right == null) {
          return p.getLeft();
        }
      } else if (rightVal.equals(right)) {
        return p.getLeft();
      }
    }
    
    return null;
  }
  
  // not final so extending classes can mutate
  protected L left;
  protected R right;
  
  /**
   * Constructs a new pair, providing the left and right objects to be held.
   * 
   * @param left Left reference
   * @param right Right reference
   */
  public Pair(L left, R right) {
    this.left = left;
    this.right = right;
  }
  
  /**
   * Getter to get the left reference stored in the pair.
   * 
   * @return Left reference
   */
  public L getLeft() {
    return left;
  }

  /**
   * Getter to get the right reference stored in the pair.
   * 
   * @return Right reference
   */
  public R getRight() {
    return right;
  }
  
  @Override
  public String toString() {
    return Pair.class.getSimpleName() + '[' + getLeft() + ',' + getRight() + ']';
  }
  
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o instanceof Pair) {
      Pair<?, ?> p = (Pair<?, ?>)o;
      // lazily set in if condition
      L left; R right;
      if (! ((left = getLeft()) == p.getLeft() || 
          (left != null && left.equals(p.getLeft())))) {
        return false;
      } else if (! ((right = getRight()) == p.getRight() || 
                 (right != null && right.equals(p.getRight())))) {
        return false;
      } else {
        return true;
      }
    } else {
      return false;
    }
  }
  
  @Override
  public int hashCode() {
    L left = getLeft();
    R right = getRight();
    int leftHash = left == null ? LEFT_PRIME : left.hashCode();
    int rightHash = right == null ? RIGHT_PRIME : right.hashCode();
    return leftHash ^ rightHash;
  }
}
