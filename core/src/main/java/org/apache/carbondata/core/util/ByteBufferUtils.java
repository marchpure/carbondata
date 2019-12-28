/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.core.util;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import sun.nio.ch.DirectBuffer;

/**
 * Utility functions for working with byte buffers, such as reading/writing
 * variable-length long numbers.
 */
public final class ByteBufferUtils {
  static final Field address, capacity;
  static {
    try {
      address = Buffer.class.getDeclaredField("address");
      address.setAccessible(true);
      capacity = Buffer.class.getDeclaredField("capacity");
      capacity.setAccessible(true);
    } catch (NoSuchFieldException e) {
      throw new AssertionError(e);
    }
  }

  private ByteBufferUtils() {
  }

  static abstract class Comparer {
    abstract int compareTo(ByteBuffer buf1, int o1, int l1, ByteBuffer buf2, int o2, int l2);
  }

  static class ComparerHolder {
    static final String UNSAFE_COMPARER_NAME = ComparerHolder.class.getName() + "$UnsafeComparer";

    static final Comparer BEST_COMPARER = getBestComparer();

    static Comparer getBestComparer() {
      try {
        Class<?> theClass = Class.forName(UNSAFE_COMPARER_NAME);
        @SuppressWarnings("unchecked")
        Comparer comparer = (Comparer) theClass.getConstructor().newInstance();
        return comparer;
      } catch (Throwable t) { // ensure we really catch *everything*
        return PureJavaComparer.INSTANCE;
      }
    }

    static final class PureJavaComparer extends Comparer {
      static final PureJavaComparer INSTANCE = new PureJavaComparer();

      private PureJavaComparer() {}

      @Override
      public int compareTo(ByteBuffer buf1, int o1, int l1, ByteBuffer buf2, int o2, int l2) {
        int end1 = o1 + l1;
        int end2 = o2 + l2;
        for (int i = o1, j = o2; i < end1 && j < end2; i++, j++) {
          int a = buf1.get(i) & 0xFF;
          int b = buf2.get(j) & 0xFF;
          if (a != b) {
            return a - b;
          }
        }
        return l1 - l2;
      }
    }

    static final class UnsafeComparer extends Comparer {

      public UnsafeComparer() {}

      @Override
      public int compareTo(ByteBuffer buf1, int o1, int l1, ByteBuffer buf2, int o2, int l2) {
        long offset1Adj, offset2Adj;
        Object refObj1 = null, refObj2 = null;
        if (buf1.isDirect()) {
          offset1Adj = o1 + ((DirectBuffer) buf1).address();
        } else {
          offset1Adj = o1 + buf1.arrayOffset() + UnsafeAccess.BYTE_ARRAY_BASE_OFFSET;
          refObj1 = buf1.array();
        }
        if (buf2.isDirect()) {
          offset2Adj = o2 + ((DirectBuffer) buf2).address();
        } else {
          offset2Adj = o2 + buf2.arrayOffset() + UnsafeAccess.BYTE_ARRAY_BASE_OFFSET;
          refObj2 = buf2.array();
        }
        return compareToUnsafe(refObj1, offset1Adj, l1, refObj2, offset2Adj, l2);
      }
    }
  }

  public static int compareTo(ByteBuffer buf1, ByteBuffer buf2) {
    return ComparerHolder.BEST_COMPARER.compareTo(buf1, 0, buf1.limit(),
        buf2, 0, buf2.limit());
  }

  public static int compareTo(ByteBuffer buf1, int o1, int l1, ByteBuffer buf2, int o2, int l2) {
    return ComparerHolder.BEST_COMPARER.compareTo(buf1, o1, l1, buf2, o2, l2);
  }

  private static int compareToUnsafe(Object obj1, long o1, int l1, Object obj2, long o2, int l2) {
    final int stride = 8;
    final int minLength = Math.min(l1, l2);
    int strideLimit = minLength & ~(stride - 1);
    int i;

    /*
     * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a time is no slower than
     * comparing 4 bytes at a time even on 32-bit. On the other hand, it is substantially faster on
     * 64-bit.
     */
    for (i = 0; i < strideLimit; i += stride) {
      long lw = UnsafeAccess.theUnsafe.getLong(obj1, o1 + (long) i);
      long rw = UnsafeAccess.theUnsafe.getLong(obj2, o2 + (long) i);
      if (lw != rw) {
        if (!UnsafeAccess.LITTLE_ENDIAN) {
          return ((lw + Long.MIN_VALUE) < (rw + Long.MIN_VALUE)) ? -1 : 1;
        }
        /*
         * We want to compare only the first index where left[index] != right[index]. This
         * corresponds to the least significant nonzero byte in lw ^ rw, since lw and rw are
         * little-endian. Long.numberOfTrailingZeros(diff) tells us the least significant
         * nonzero bit, and zeroing out the first three bits of L.nTZ gives us the shift to get
         * that least significant nonzero byte. This comparison logic is based on UnsignedBytes
         * from guava v21
         */
        int n = Long.numberOfTrailingZeros(lw ^ rw) & ~0x7;
        return ((int) ((lw >>> n) & 0xFF)) - ((int) ((rw >>> n) & 0xFF));
      }
    }

    // The epilogue to cover the last (minLength % stride) elements.
    for (; i < minLength; i++) {
      int il = (UnsafeAccess.theUnsafe.getByte(obj1, o1 + i) & 0xFF);
      int ir = (UnsafeAccess.theUnsafe.getByte(obj2, o2 + i) & 0xFF);
      if (il != ir) {
        return il - ir;
      }
    }
    return l1 - l2;
  }

  public static ByteBuffer allocate(int capacity, boolean useDirect) {
    return useDirect ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity);
  }

  public static ByteBuffer ensureLength(ByteBuffer byteBuffer, int limit) {
    if (byteBuffer.capacity() < limit) {
        byteBuffer = byteBuffer.isDirect() ? ByteBuffer.allocateDirect(limit)
            : ByteBuffer.allocate(limit);
        return byteBuffer;
    }
    return byteBuffer;
  }

  public static ByteBuffer wrapAddress(long addr, int length, boolean useDirect) {
    ByteBuffer bb = useDirect ?
        ByteBuffer.allocateDirect(0).order(ByteOrder.nativeOrder()):
        ByteBuffer.allocate(0).order(ByteOrder.nativeOrder());
    try {
      address.setLong(bb, addr);
      capacity.setInt(bb, length);
      bb.clear();
    } catch (IllegalAccessException e) {
      throw new AssertionError(e);
    }
    return bb;
  }

  /**
   * Copies the specified range of the specified bytebuffer into a new array.
   * @param original the buffer from which the copy has to happen
   * @param from the starting index
   * @param to the ending index
   * @return a byte[] created out of the copy
   */
  public static byte[] copyOfRange(ByteBuffer original, int from, int to) {
    int newLength = to - from;
    if (newLength < 0) throw new IllegalArgumentException(from + " > " + to);
    byte[] copy = new byte[newLength];
    ByteBufferUtils.copyFromBufferToArray(copy, original, from, 0, newLength);
    return copy;
  }

  /**
   * Copies specified number of bytes from given offset of 'in' ByteBuffer to
   * the array. This doesn't affact the position of buffer.
   * @param out
   * @param in
   * @param sourceOffset
   * @param destinationOffset
   * @param length
   */
  public static void copyFromBufferToArray(byte[] out, ByteBuffer in, int sourceOffset,
      int destinationOffset, int length) {
    if (in.hasArray()) {
      System.arraycopy(in.array(), sourceOffset + in.arrayOffset(),
          out, destinationOffset, length);
    } else {
      UnsafeAccess.copy(in, sourceOffset, out, destinationOffset, length);
    }
  }

  /**
   * Copy one buffer's whole data to another. Write starts at the current position of 'out' buffer.
   * Note : This will advance the position marker of {@code out} and also change the position maker
   * for {@code in}.
   * @param in source buffer
   * @param out destination buffer
   */
  public static void copyFromBufferToBuffer(ByteBuffer in, ByteBuffer out) {
    if (in.hasArray() && out.hasArray()) {
      int length = in.remaining();
      System.arraycopy(in.array(), in.arrayOffset(), out.array(), out.arrayOffset(), length);
      out.position(out.position() + length);
      in.position(in.limit());
    } else {
      int length = in.remaining();
      UnsafeAccess.copy(in, in.position(), out, out.position(), length);
      out.position(out.position() + length);
      in.position(in.limit());
    }
  }
}
