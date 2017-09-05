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

import org.apache.carbondata.core.cache.dictionary.ColumnDictionaryInfo;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.presto.CarbondataColumnHandle;

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import org.junit.Test;

import static junit.framework.TestCase.assertNotNull;

public class StreamReadersTest {
  private CarbondataColumnHandle carbondataColumnHandle;
  private Type type;

  @Test public void testReadBlock() {
    carbondataColumnHandle =
        new CarbondataColumnHandle("connectorId", "id", BigintType.BIGINT, 0, 3, 1, true, 1, "int",
            true, 5, 4);
    type = carbondataColumnHandle.getColumnType();
    assertNotNull(StreamReaders.createStreamReader(type, new ColumnDictionaryInfo(DataType.LONG)));
    carbondataColumnHandle =
        new CarbondataColumnHandle("connectorId", "id", DoubleType.DOUBLE, 0, 3, 1, true, 1, "int",
            true, 5, 4);
    type = carbondataColumnHandle.getColumnType();
    assertNotNull(
        StreamReaders.createStreamReader(type, new ColumnDictionaryInfo(DataType.DOUBLE)));
    carbondataColumnHandle =
        new CarbondataColumnHandle("connectorId", "id", DecimalType.createDecimalType(), 0, 3, 1,
            true, 1, "int", true, 5, 4);
    type = carbondataColumnHandle.getColumnType();
    assertNotNull(
        StreamReaders.createStreamReader(type, new ColumnDictionaryInfo(DataType.DECIMAL)));
    carbondataColumnHandle =
        new CarbondataColumnHandle("connectorId", "id", VarcharType.VARCHAR, 0, 3, 1, true, 1,
            "int", true, 5, 4);
    type = carbondataColumnHandle.getColumnType();
    assertNotNull(
        StreamReaders.createStreamReader(type, new ColumnDictionaryInfo(DataType.STRING)));
    carbondataColumnHandle =
        new CarbondataColumnHandle("connectorId", "id", IntegerType.INTEGER, 0, 3, 1, true, 1,
            "int", true, 5, 4);
    type = carbondataColumnHandle.getColumnType();
    assertNotNull(StreamReaders.createStreamReader(type, new ColumnDictionaryInfo(DataType.INT)));
    carbondataColumnHandle =
        new CarbondataColumnHandle("connectorId", "id", VarcharType.VARCHAR, 0, 3, 1, true, 1,
            "int", true, 5, 4);
    type = carbondataColumnHandle.getColumnType();
    assertNotNull(StreamReaders.createStreamReader(type, null));

  }
}
