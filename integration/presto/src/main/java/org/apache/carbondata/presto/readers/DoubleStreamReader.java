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
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.DataTypeUtil;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.IntArrayBlock;
import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.type.Type;
import com.google.common.primitives.Longs;
import org.weakref.jmx.internal.guava.primitives.Doubles;

/**
 * Class for Reading the Double value and setting it in Block
 */
public class DoubleStreamReader extends AbstractStreamReader {

  private boolean isDictionary;
  private Dictionary dictionary;

  public DoubleStreamReader() {

  }

  public DoubleStreamReader(boolean isDictionary, Dictionary dictionary) {
    this.isDictionary = isDictionary;
    this.dictionary = dictionary;
  }

  /**
   * Create the DoubleType Block
   *
   * @param type
   * @return
   * @throws IOException
   */
  public Block readBlock(Type type) throws IOException {
    int numberOfRows;
    BlockBuilder builder;
    if (isVectorReader) {
      numberOfRows = batchSize;
      builder = type.createBlockBuilder(new BlockBuilderStatus(), numberOfRows);
      if (columnVector != null) {
        if (isDictionary) {
          populateDictionaryVector(type, numberOfRows, builder);
        } else {
          if (columnVector.anyNullsSet()) {
            handleNullInVector(type, numberOfRows, builder);
          } else {
            populateVector(type, numberOfRows, builder);
          }
         return  new LongArrayBlock(batchSize, columnVector.getIsNullVector(), Longs.toArray(
             Doubles.asList(columnVector.getDoubleData())));

        }
      }
    } else {
      numberOfRows = streamData.length;
      builder = type.createBlockBuilder(new BlockBuilderStatus(), numberOfRows);
      if (streamData != null) {
        for (int i = 0; i < numberOfRows; i++) {
          type.writeDouble(builder, (Double) streamData[i]);
        }
      }
    }

    return builder.build();
  }

  private void handleNullInVector(Type type, int numberOfRows, BlockBuilder builder) {
    for (int i = 0; i < numberOfRows; i++) {
      if (columnVector.isNullAt(i)) {
        builder.appendNull();
      } else {
        type.writeDouble(builder, (Double) columnVector.getData(i));
      }
    }
  }

  private void populateVector(Type type, int numberOfRows, BlockBuilder builder) {
    for (int i = 0; i < numberOfRows; i++) {
      type.writeDouble(builder, (Double) columnVector.getData(i));
    }
  }

  private void populateDictionaryVector(Type type, int numberOfRows, BlockBuilder builder) {
    for (int i = 0; i < numberOfRows; i++) {
      int dictKey = (int) columnVector.getData(i);
      String dictionaryValue = dictionary.getDictionaryValueForKey(dictKey);
      if (dictionaryValue.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
        builder.appendNull();
      } else {
        Double doubleValue = parseDouble(dictionaryValue);
        if (doubleValue != null) {
          type.writeDouble(builder, doubleValue);
        } else {
          builder.appendNull();
        }
      }
    }
  }
  private Double parseDouble(String rawValue) {
    try {
      return Double.valueOf(rawValue);
    } catch (NumberFormatException numberFormatException) {
      return null;
    }
  }
}