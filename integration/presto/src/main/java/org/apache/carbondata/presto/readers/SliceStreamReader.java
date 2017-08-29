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

package org.apache.carbondata.presto.readers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryChunksWrapper;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.SliceArrayBlock;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;

/**
 * This class reads the String data and convert it into Slice Block
 */
public class SliceStreamReader extends AbstractStreamReader {


  private boolean isDictionary;

  private Dictionary dictionaryData;

  private SliceArrayBlock dictionaryBlock;

  public SliceStreamReader() {}

  public SliceStreamReader(boolean isDictionary, Dictionary dictionaryData) {
    this.isDictionary = isDictionary;
    this.dictionaryData = dictionaryData;
    this.dictionaryBlock = createSliceArrayBlock();
  }

  /**
   * Function to create the Slice Block
   * @param type
   * @return
   * @throws IOException
   */
  public Block readBlock(Type type)
      throws IOException
  {
    int numberOfRows = 0;
    BlockBuilder builder = null;
    if(isVectorReader) {
      numberOfRows = batchSize;
      builder = type.createBlockBuilder(new BlockBuilderStatus(), numberOfRows);
      if (columnVector != null) {
        if(isDictionary) {
          int[] values = new int[numberOfRows];
          for (int i = 0; i < numberOfRows; i++) {
            if (!columnVector.isNullAt(i)) {
              values[i] = columnVector.getInt(i);
            }
          }
          Block block = new DictionaryBlock(batchSize, dictionaryBlock, values);

          return block;
        } else {
          for (int i = 0; i < numberOfRows; i++) {
            if (columnVector.isNullAt(i)) {
              builder.appendNull();
            } else {
              type.writeSlice(builder, utf8Slice(columnVector.getUTF8String(i).toString()));
            }
          }
        }
      }
    } else {
      numberOfRows = streamData.length;
      builder = type.createBlockBuilder(new BlockBuilderStatus(), numberOfRows);
      if (streamData != null) {
        for(int i = 0; i < numberOfRows ; i++ ){
          type.writeSlice(builder, utf8Slice(streamData[i].toString()));
        }
      }
    }

    return builder.build();

  }

  /**
   * Function to get the DictionaryValues in Slice
   * @return
   */
  private Slice[] getDictionarySlice() {
    List<Slice> sliceArray = new ArrayList<>();
    if(dictionaryData != null){
      DictionaryChunksWrapper chunks = dictionaryData.getDictionaryChunks();
      sliceArray = new ArrayList<Slice>(chunks.getSize() + 1);
      sliceArray.add( Slices.EMPTY_SLICE );
      long count = 0;
      while(chunks.hasNext()){
        byte[] value = chunks.next();
        // Since in Carbondata the first value is for null so in case of null it should return null
        if ( count == 0) {
          sliceArray.add(null);
        } else {
          sliceArray.add(wrappedBuffer(value, 0, value.length));
        }
        count++;
      }
    }
    return  sliceArray.toArray(new Slice[sliceArray.size()]);
  }

  /**
   * Function to Create the SliceArrayBlock that is required in the DictionaryBlock
   * @return
   */
  private SliceArrayBlock createSliceArrayBlock() {
    Slice[] sliceArray = getDictionarySlice();
    return new SliceArrayBlock(sliceArray.length, sliceArray, true);
  }

}
