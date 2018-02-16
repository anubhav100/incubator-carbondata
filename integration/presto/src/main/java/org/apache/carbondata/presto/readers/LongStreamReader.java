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

import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.constants.CarbonCommonConstants;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.type.Type;

public class LongStreamReader extends AbstractStreamReader {

  private boolean isDictionary;
  private Dictionary dictionary;

  public LongStreamReader() {

  }

  public LongStreamReader(boolean isDictionary, Dictionary dictionary) {
    this.isDictionary = isDictionary;
    this.dictionary = dictionary;
  }

  public Block readBlock(Type type) throws IOException {
    int numberOfRows;
    BlockBuilder builder;
    if (isVectorReader) {
      numberOfRows = batchSize;
      if (columnVector != null) {
        if (isDictionary) {
          return populateDictionaryVector(type, numberOfRows);
        }
        else {
          return new LongArrayBlock(batchSize, columnVector.getIsNullVector(), columnVector.getLongData());
        }
      }
    } else {
      numberOfRows = streamData.length;
      builder = type.createBlockBuilder(new BlockBuilderStatus(), numberOfRows);
      if (streamData != null) {
        for (int i = 0; i < numberOfRows; i++) {
          type.writeLong(builder, (Long) streamData[i]);
        }
      }
      return builder.build();
    }
    return null;
  }

  private Block populateDictionaryVector(Type type, int numberOfRows) {

    BlockBuilder builder =  type.createBlockBuilder(new BlockBuilderStatus(), numberOfRows);
    for (int i = 0; i < numberOfRows; i++) {
      int dictKey = (int) columnVector.getData(i);
      String dictionaryValue =dictionary.getDictionaryValueForKey(dictKey);
      if (dictionaryValue.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
        builder.appendNull();
      } else {
        Long longValue = parseLong(dictionaryValue);
        if(longValue!=null) {
          type.writeLong(builder,longValue);
        }
        else{
          builder.appendNull();
        }
      }
    }
    return builder.build();
  }
  private Long parseLong(String rawValue) {
    try {
      return Long.parseLong(rawValue);
    } catch (NumberFormatException numberFormatException) {
      return null;
    }
  }
}