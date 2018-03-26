package org.threadly.concurrent;

/**
 * A class for wrapping {@link InterruptedException} that is not checked.  In addition to the 
 * blocking not needing to be declared, this will ensure that the interrupted status of the thread 
 * is set on construction (since it is naturally cleared when caught).
 */
public class UncheckedInterruptedException extends RuntimeException {
  private static final long serialVersionUID = -4828682927356336855L;

  /**
   * Construct a new {@link UncheckedInterruptedException} without a source exception.  This should 
   * be used if the threads interrupted status is being checked directly.  Constructing this will 
   * set the invoking threads status to be interrupted if it's not already.
   */
  public UncheckedInterruptedException() {
    this(null);
  }
  
  /**
   * Constructs a new {@link UncheckedInterruptedException} with a source 
   * {@link InterruptedException} to wrap.    Constructing this will set the invoking threads 
   * status to be interrupted if it's not already.
   * 
   * @param e Exception to wrap as cause
   */
  public UncheckedInterruptedException(InterruptedException e) {
    super(e);
    
    Thread.currentThread().interrupt();
  }
}
