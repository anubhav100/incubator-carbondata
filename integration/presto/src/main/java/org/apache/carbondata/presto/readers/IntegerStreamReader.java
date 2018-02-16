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
import com.facebook.presto.spi.block.IntArrayBlock;
import com.facebook.presto.spi.type.Type;

public class IntegerStreamReader extends AbstractStreamReader {

  private Dictionary dictionary;
  private boolean isDictionary;

  public IntegerStreamReader() {

  }

  public IntegerStreamReader(boolean isDictionary, Dictionary dictionary) {
    this.dictionary = dictionary;
    this.isDictionary = isDictionary;
  }

  public Block readBlock(Type type) throws IOException {
    int numberOfRows;
    BlockBuilder builder;
    if (isVectorReader) {
      numberOfRows = batchSize;
      if (columnVector != null) {
        if (isDictionary) {
          return populateDictionaryVector(type, numberOfRows);
        } else {
          return  new IntArrayBlock(batchSize, columnVector.getIsNullVector(), columnVector.getInts());
        }
      }
    } else {
      numberOfRows = streamData.length;
      builder = type.createBlockBuilder(new BlockBuilderStatus(), numberOfRows);
      if (streamData != null) {
        for (int i = 0; i < numberOfRows; i++) {
          type.writeLong(builder, ((Integer) streamData[i]).longValue());
        }
      }
      return builder.build();
    }
    return null;
  }

  private Block populateDictionaryVector(Type type, int numberOfRows) {
    BlockBuilder builder = type.createBlockBuilder(new BlockBuilderStatus(), numberOfRows);
    for (int i = 0; i < numberOfRows; i++) {
      int dictKey = (int) columnVector.getData(i);
      String dictionaryValue = dictionary.getDictionaryValueForKey(dictKey);
      if (dictionaryValue.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
        builder.appendNull();
      } else {
        Integer intValue = parseInteger(dictionaryValue);
        if (intValue != null) {
          type.writeLong(builder, intValue);
        } else {
          builder.appendNull();
        }
      }
    }
    return builder.build();
  }
  private Integer parseInteger(String rawValue) {
    try {
      return Integer.parseInt(rawValue);
    } catch (NumberFormatException numberFormatException) {
      return null;
    }
  }
}
