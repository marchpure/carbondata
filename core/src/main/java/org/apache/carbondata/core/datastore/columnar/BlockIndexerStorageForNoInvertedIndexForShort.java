/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.core.datastore.columnar;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.util.ByteBufferUtils;
import org.apache.carbondata.core.util.ByteUtil;

/**
 * Below class will be used to for no inverted index
 */
public class BlockIndexerStorageForNoInvertedIndexForShort extends BlockIndexerStorage<byte[][]> {

  /**
   * column data
   */
  private byte[][] dataPage;

  private ColumnPage dataColumnPage;

  public ByteBuffer dataPageByteBuffer;

  private short[] dataRlePage;

  public BlockIndexerStorageForNoInvertedIndexForShort(byte[][] dataPage, boolean applyRLE) {
    this.dataPage = dataPage;
    if (applyRLE) {
      List<byte[]> actualDataList = new ArrayList<>();
      for (int i = 0; i < dataPage.length; i++) {
        actualDataList.add(dataPage[i]);
      }
      rleEncodeOnData(actualDataList);
    }
  }

  public BlockIndexerStorageForNoInvertedIndexForShort(ColumnPage dataColumnPage, boolean applyRLE) {
    this.dataColumnPage = dataColumnPage;
    if (applyRLE) {
      rleEncodeOnData();
    } else {
      this.dataPageByteBuffer = dataColumnPage.getFlattedByteBufferPage();
    }
  }

  private void rleEncodeOnData() {
    ByteBuffer prvKey = dataColumnPage.getByteBufferRow(0);
    List<Integer> list = new ArrayList<>(dataColumnPage.getPageSize() / 2);
    int totalLength = 0;
    list.add(0);
    totalLength += prvKey.limit();
    short counter = 1;
    short start = 0;
    List<Short> map = new ArrayList<>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    for (int i = 1; i < dataColumnPage.getPageSize(); i++) {
      ByteBuffer currentKey = dataColumnPage.getByteBufferRow(i);
      if (ByteBufferUtils.compareTo(prvKey, currentKey) != 0) {
        prvKey = currentKey;
        list.add(i);
        totalLength += currentKey.limit();
        map.add(start);
        map.add(counter);
        start += counter;
        counter = 1;
        continue;
      }
      counter++;
    }
    map.add(start);
    map.add(counter);
    // if rle is index size is more than 70% then rle wont give any benefit
    // so better to avoid rle index and write data as it is
    boolean useRle = (((list.size() + map.size()) * 100) / dataColumnPage.getPageSize()) < 70;
    if (useRle) {
      this.dataPageByteBuffer = convertToDataPageByteBuffer(list, totalLength);
      dataRlePage = convertToArray(map);
    } else {
      this.dataPageByteBuffer = dataColumnPage.getFlattedByteBufferPage();
      dataRlePage = new short[0];
    }
  }


  private void rleEncodeOnData(List<byte[]> actualDataList) {
    byte[] prvKey = actualDataList.get(0);
    List<byte[]> list = new ArrayList<>(actualDataList.size() / 2);
    list.add(actualDataList.get(0));
    short counter = 1;
    short start = 0;
    List<Short> map = new ArrayList<>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    for (int i = 1; i < actualDataList.size(); i++) {
      if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(prvKey, actualDataList.get(i)) != 0) {
        prvKey = actualDataList.get(i);
        list.add(actualDataList.get(i));
        map.add(start);
        map.add(counter);
        start += counter;
        counter = 1;
        continue;
      }
      counter++;
    }
    map.add(start);
    map.add(counter);
    // if rle is index size is more than 70% then rle wont give any benefit
    // so better to avoid rle index and write data as it is
    boolean useRle = (((list.size() + map.size()) * 100) / actualDataList.size()) < 70;
    if (useRle) {
      this.dataPage = convertToDataPage(list);
      dataRlePage = convertToArray(map);
    } else {
      this.dataPage = convertToDataPage(actualDataList);
      dataRlePage = new short[0];
    }
  }

  private byte[][] convertToDataPage(List<byte[]> list) {
    byte[][] shortArray = new byte[list.size()][];
    for (int i = 0; i < shortArray.length; i++) {
      shortArray[i] = list.get(i);
    }
    return shortArray;
  }

  private ByteBuffer convertToDataPageByteBuffer(List<Integer> list, int totalLength) {
    ByteBuffer rleDataPageByteBuffer = ByteBufferUtils.allocate(totalLength, false);
    for (int i = 0; i < list.size(); i++) {
      ByteBufferUtils.copyFromBufferToBuffer(dataColumnPage.getByteBufferRow(list.get(i)),
          rleDataPageByteBuffer);
    }
    return rleDataPageByteBuffer;
  }


  public short[] getDataRlePage() {
    return dataRlePage;
  }

  public int getDataRlePageLengthInBytes() {
    if (dataRlePage != null) {
      return dataRlePage.length * 2;
    } else {
      return 0;
    }
  }

  /**
   * no use
   *
   * @return
   */
  public short[] getRowIdPage() {
    return new short[0];
  }

  public int getRowIdPageLengthInBytes() {
    return 0;
  }

  /**
   * no use
   *
   * @return
   */
  public short[] getRowIdRlePage() {
    return new short[0];
  }

  public int getRowIdRlePageLengthInBytes() {
    return 0;
  }

  /**
   * @return the dataPage
   */
  public byte[][] getDataPage() {
    return dataPage;
  }

}
