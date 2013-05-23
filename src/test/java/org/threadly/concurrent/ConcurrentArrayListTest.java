package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.lock.NativeLock;
import org.threadly.concurrent.lock.VirtualLock;

@SuppressWarnings("javadoc")
public class ConcurrentArrayListTest {
  private static final int TEST_QTY = 10;
  
  private ConcurrentArrayList<String> testList;
  
  @Before
  public void setup() {
    testList = new ConcurrentArrayList<String>();
  }
  
  @After
  public void tearDown() {
    testList = null;
  }
  
  @Test
  public void getModificationLockTest() {
    VirtualLock testLock = new NativeLock();
    ConcurrentArrayList<String> testList = new ConcurrentArrayList<String>(testLock);
    
    assertTrue(testLock == testList.getModificationLock());
  }
  
  @Test
  public void setFrontPaddingTest() {
    testList.setFrontPadding(1);
    assertEquals(testList.getFrontPadding(), 1);
    
    // make some modifications
    testList.add("foo");
    testList.add("bar");
    testList.remove(0);
    
    assertEquals(testList.getFrontPadding(), 1);
  }

  @Test (expected = IllegalArgumentException.class)
  public void setFrontPaddingFail() {
    testList.setFrontPadding(-1);
  }
  
  @Test
  public void setRearPaddingTest() {
    testList.setRearPadding(1);
    assertEquals(testList.getRearPadding(), 1);
    
    // make some modifications
    testList.add("foo");
    testList.add("bar");
    testList.remove(0);
    
    assertEquals(testList.getRearPadding(), 1);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setRearPaddingFail() {
    testList.setRearPadding(-1);
  }

  @Test
  public void sizeTest() {
    for (int i = 0; i < TEST_QTY; i++) {
      assertEquals(testList.size(), i);
      testList.add("testStr");
      assertEquals(testList.size(), i + 1);
    }

    for (int i = TEST_QTY; i >= 0; i--) {
      assertEquals(testList.size(), i);
      if (i != 0) {
        testList.removeFirst();
      }
    }
  }

  @Test
  public void isEmptyTest() {
    assertTrue(testList.isEmpty());
    testList.add("foo");
    assertFalse(testList.isEmpty());
    testList.add("foo");
    assertFalse(testList.isEmpty());
    testList.removeFirst();
    assertFalse(testList.isEmpty());
    testList.removeFirst();
    assertTrue(testList.isEmpty());
  }
  
  @Test
  public void getTest() {
    List<String> comparisionList = new ArrayList<String>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      comparisionList.add(str);
      testList.add(str);
      assertEquals(testList.get(i), str);
    }
    for (int i = 0; i < TEST_QTY; i++) {
      assertEquals(testList.get(i), comparisionList.get(i));
    }
  }
}