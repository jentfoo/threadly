package org.threadly.concurrent.event;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executor;

class AbstractListenerHelper<T> {
  protected final Object listenersLock = new Object();
  protected Map<T, Executor> listeners = null;

  /**
   * Return a collection of the currently subscribed listener instances.  This returned collection 
   * can NOT be modified.
   * 
   * @return A non-null collection of currently subscribed listeners
   */
  public Collection<T> getSubscribedListeners() {
    synchronized (listenersLock) {
      if (listeners == null) {
        return Collections.emptyList();
      } else {
        return Collections.unmodifiableList(new ArrayList<T>(listeners.keySet()));
      }
    }
  }
  
  /**
   * Removes all listeners currently registered. 
   */
  public void clearListeners() {
    synchronized (listenersLock) {
      listeners = null;
    }
  }
  
  /**
   * Returns how many listeners were added, and will be ran on the next invocation of them.
   * 
   * @return number of listeners registered to be called
   */
  public int registeredListenerCount() {
    synchronized (listenersLock) {
      return listeners == null ? 0 : listeners.size();
    }
  }
}
