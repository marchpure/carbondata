package org.apache.carbondata.core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

public final class UnsafeAccess {

  private static final Logger LOG = LoggerFactory.getLogger(UnsafeAccess.class);

  public static final Unsafe theUnsafe;
  /** The offset to the first element in a byte array. */
  public static final long BYTE_ARRAY_BASE_OFFSET;

  // This number limits the number of bytes to copy per call to Unsafe's
  // copyMemory method. A limit is imposed to allow for safepoint polling
  // during a large copy
  static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;

  public static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder()
      .equals(ByteOrder.LITTLE_ENDIAN);

  static {
    theUnsafe = (Unsafe) AccessController.doPrivileged(new PrivilegedAction<Object>() {
      @Override
      public Object run() {
        try {
          Field f = Unsafe.class.getDeclaredField("theUnsafe");
          f.setAccessible(true);
          return f.get(null);
        } catch (Throwable e) {
          LOG.warn("sun.misc.Unsafe is not accessible", e);
        }
        return null;
      }
    });

    if (theUnsafe != null) {
      BYTE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);
    } else{
      BYTE_ARRAY_BASE_OFFSET = -1;
    }
  }

  private UnsafeAccess(){}

  /**
   * Copies specified number of bytes from given offset of {@code src} ByteBuffer to the
   * {@code dest} array.
   *
   * @param src
   * @param srcOffset
   * @param dest
   * @param destOffset
   * @param length
   */
  public static void copy(ByteBuffer src, int srcOffset, byte[] dest, int destOffset,
      int length) {
    long srcAddress = srcOffset;
    Object srcBase = null;
    if (src.isDirect()) {
      srcAddress = srcAddress + ((DirectBuffer) src).address();
    } else {
      srcAddress = srcAddress + BYTE_ARRAY_BASE_OFFSET + src.arrayOffset();
      srcBase = src.array();
    }
    long destAddress = destOffset + BYTE_ARRAY_BASE_OFFSET;
    unsafeCopy(srcBase, srcAddress, dest, destAddress, length);
  }

  /**
   * Copies specified number of bytes from given offset of {@code src} buffer into the {@code dest}
   * buffer.
   *
   * @param src
   * @param srcOffset
   * @param dest
   * @param destOffset
   * @param length
   */
  public static void copy(ByteBuffer src, int srcOffset, ByteBuffer dest, int destOffset,
      int length) {
    long srcAddress, destAddress;
    Object srcBase = null, destBase = null;
    if (src.isDirect()) {
      srcAddress = srcOffset + ((DirectBuffer) src).address();
    } else {
      srcAddress = (long) srcOffset +  src.arrayOffset() + BYTE_ARRAY_BASE_OFFSET;
      srcBase = src.array();
    }
    if (dest.isDirect()) {
      destAddress = destOffset + ((DirectBuffer) dest).address();
    } else {
      destAddress = destOffset + BYTE_ARRAY_BASE_OFFSET + dest.arrayOffset();
      destBase = dest.array();
    }
    unsafeCopy(srcBase, srcAddress, destBase, destAddress, length);
  }

  private static void unsafeCopy(Object src, long srcAddr, Object dst, long destAddr, long len) {
    while (len > 0) {
      long size = (len > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : len;
      theUnsafe.copyMemory(src, srcAddr, dst, destAddr, len);
      len -= size;
      srcAddr += size;
      destAddr += size;
    }
  }
}
