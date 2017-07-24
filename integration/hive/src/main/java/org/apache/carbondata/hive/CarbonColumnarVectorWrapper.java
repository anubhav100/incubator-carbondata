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

package org.apache.carbondata.hive;

import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import scala.util.control.Exception;

public class CarbonColumnarVectorWrapper implements CarbonColumnVector {


  private boolean[] filteredRows;

  private int counter;

  private ColumnVector columnVector;

  private boolean filteredRowsExist;

  private BytesColumnVector bytesColumnVector;

  private LongColumnVector longColumnVector;

  private DoubleColumnVector doubleColumnVector;

  private String vectorType;

  CarbonColumnarVectorWrapper(ColumnVector columnVector, boolean[] filteredRows)
      throws HiveException {

    this.columnVector = columnVector;
    initilizedVector();

    if(columnVector instanceof BytesColumnVector) {
      bytesColumnVector = (BytesColumnVector) columnVector;
      this.filteredRows = filteredRows;

    }
    if(columnVector instanceof LongColumnVector) {
      longColumnVector = (LongColumnVector) columnVector;
      this.filteredRows = filteredRows;

    }
    if(columnVector instanceof DoubleColumnVector){
      doubleColumnVector = (DoubleColumnVector) columnVector;
      this.filteredRows = filteredRows;

    }
    else {
      throw new HiveException("Unsupported Vector Type In Hive");
    }
  }

private void initilizedVector(){
  this.columnVector.init();
}

  @Override public void putBoolean(int rowId, boolean value) {
    if (!filteredRows[rowId]) {
       // columnVector.putBoolean(counter++, value);
    }
  }

  @Override public void putFloat(int rowId, float value) {
    if (!filteredRows[rowId]) {
     // columnVector.putFloat(counter++, value);
    }
  }

  @Override public void putShort(int rowId, short value) {
    if (!filteredRows[rowId]) {
     // columnVector.putShort(counter++, value);
    }
  }

  @Override public void putShorts(int rowId, int count, short value) {
    if (filteredRowsExist) {
      for (int i = 0; i < count; i++) {
        if (!filteredRows[rowId]) {
        //  columnVector.putShort(counter++, value);
        }
        rowId++;
      }
    } else {
      //columnVector.putShorts(rowId, count, value);
    }
  }

  @Override public void putInt(int rowId, int value) {
    if (!filteredRows[rowId]) {
      //columnVector.putInt(counter++, value);
    }
  }

  @Override public void putInts(int rowId, int count, int value) {
    if (filteredRowsExist) {
      for (int i = 0; i < count; i++) {
        if (!filteredRows[rowId]) {
        //  columnVector.putInt(counter++, value);
        }
        rowId++;
      }
    } else {
      //columnVector.putInts(rowId, count, value);
    }
  }

  @Override public void putLong(int rowId, long value) {
    if (!filteredRows[rowId]) {
      //columnVector.putLong(counter++, value);
    }
  }

  @Override public void putLongs(int rowId, int count, long value) {
    if (filteredRowsExist) {
      for (int i = 0; i < count; i++) {
        if (!filteredRows[rowId]) {
         // columnVector.putLong(counter++, value);
        }
        rowId++;
      }
    } else {
      //columnVector.putLongs(rowId, count, value);
    }
  }

  @Override public void putDecimal(int rowId, Decimal value, int precision) {
    if (!filteredRows[rowId]) {
      //columnVector.putDecimal(counter++, value, precision);
    }
  }

  @Override public void putDecimals(int rowId, int count, Decimal value, int precision) {
    for (int i = 0; i < count; i++) {
      if (!filteredRows[rowId]) {
        //columnVector.putDecimal(counter++, value, precision);
      }
      rowId++;
    }
  }

  @Override public void putDouble(int rowId, double value) {
    if (!filteredRows[rowId]) {
      //columnVector.putDouble(counter++, value);
    }
  }

  @Override public void putDoubles(int rowId, int count, double value) {
    if (filteredRowsExist) {
      for (int i = 0; i < count; i++) {
        if (!filteredRows[rowId]) {
          //columnVector.putDouble(counter++, value);
        }
        rowId++;
      }
    } else {
      //columnVector.putDoubles(rowId, count, value);
    }
  }

  @Override public void putBytes(int rowId, byte[] value) {
    if (!filteredRows[rowId]) {
     // columnVector.putByteArray(counter++, value);
    }
  }

  @Override public void putBytes(int rowId, int count, byte[] value) {
    for (int i = 0; i < count; i++) {
      if (!filteredRows[rowId]) {
        //columnVector.putByteArray(counter++, value);
      }
      rowId++;
    }
  }

  @Override public void putBytes(int rowId, int offset, int length, byte[] value) {
    if (!filteredRows[rowId]) {
      //columnVector.putByteArray(counter++, value, offset, length);
    }
  }

  @Override public void putNull(int rowId) {
    if (!filteredRows[rowId]) {
     // columnVector.putNull(counter++);
    }
  }

  @Override public void putNulls(int rowId, int count) {
    if (filteredRowsExist) {
      for (int i = 0; i < count; i++) {
        if (!filteredRows[rowId]) {
         // columnVector.putNull(counter++);
        }
        rowId++;
      }
    } else {
      //columnVector.putNulls(rowId, count);
    }
  }

  @Override public boolean isNull(int rowId) {
    return columnVector.isNull[rowId];
  }

  @Override public void putObject(int rowId, Object obj) {
    //TODO handle complex types
  }

  @Override public Object getData(int rowId) {
    //TODO handle complex types
    return null;
  }

  @Override public void reset() {
    counter = 0;
    filteredRowsExist = false;
  }

  @Override public DataType getType() {
    return null;
  }

  @Override public void setFilteredRowsExist(boolean filteredRowsExist) {
    this.filteredRowsExist = filteredRowsExist;
  }
}
