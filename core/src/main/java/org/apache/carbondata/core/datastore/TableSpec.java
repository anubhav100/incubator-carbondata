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

package org.apache.carbondata.core.datastore;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.Writable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;

public class TableSpec {

  // column spec for each dimension and measure
  private DimensionSpec[] dimensionSpec;
  private MeasureSpec[] measureSpec;

  // number of simple dimensions
  private int numSimpleDimensions;

  public TableSpec(List<CarbonDimension> dimensions, List<CarbonMeasure> measures) {
    // first calculate total number of columnar field considering column group and complex column
    numSimpleDimensions = 0;
    for (CarbonDimension dimension : dimensions) {
      if (dimension.isColumnar()) {
        if (!dimension.isComplex()) {
          numSimpleDimensions++;
        }
      } else {
        throw new UnsupportedOperationException("column group is not supported");
      }
    }
    dimensionSpec = new DimensionSpec[dimensions.size()];
    measureSpec = new MeasureSpec[measures.size()];
    addDimensions(dimensions);
    addMeasures(measures);
  }

  private void addDimensions(List<CarbonDimension> dimensions) {
    int dimIndex = 0;
    for (int i = 0; i < dimensions.size(); i++) {
      CarbonDimension dimension = dimensions.get(i);
      if (dimension.isColumnar()) {
        if (dimension.isComplex()) {
          DimensionSpec spec = new DimensionSpec(ColumnType.COMPLEX, dimension);
          dimensionSpec[dimIndex++] = spec;
        } else if (dimension.getDataType() == DataType.TIMESTAMP && !dimension
            .isDirectDictionaryEncoding()) {
          DimensionSpec spec = new DimensionSpec(ColumnType.PLAIN_VALUE, dimension);
          dimensionSpec[dimIndex++] = spec;
        } else if (dimension.isDirectDictionaryEncoding()) {
          DimensionSpec spec = new DimensionSpec(ColumnType.DIRECT_DICTIONARY, dimension);
          dimensionSpec[dimIndex++] = spec;
        } else if (dimension.isGlobalDictionaryEncoding()) {
          DimensionSpec spec = new DimensionSpec(ColumnType.GLOBAL_DICTIONARY, dimension);
          dimensionSpec[dimIndex++] = spec;
        } else {
          DimensionSpec spec = new DimensionSpec(ColumnType.PLAIN_VALUE, dimension);
          dimensionSpec[dimIndex++] = spec;
        }
      }
    }
  }

  private void addMeasures(List<CarbonMeasure> measures) {
    for (int i = 0; i < measures.size(); i++) {
      CarbonMeasure measure = measures.get(i);
      measureSpec[i] = new MeasureSpec(measure.getColName(), measure.getDataType(), measure
          .getScale(), measure.getPrecision());
    }
  }

  public DimensionSpec getDimensionSpec(int dimensionIndex) {
    return dimensionSpec[dimensionIndex];
  }

  public MeasureSpec getMeasureSpec(int measureIndex) {
    return measureSpec[measureIndex];
  }

  public int getNumSimpleDimensions() {
    return numSimpleDimensions;
  }

  public int getNumDimensions() {
    return dimensionSpec.length;
  }

  /**
   * return number of measures
   */
  public int getNumMeasures() {
    return measureSpec.length;
  }

  public static class ColumnSpec implements Writable {
    // field name of this column
    private String fieldName;

    // data type of this column
    private DataType schemaDataType;

    // dimension type of this dimension
    private ColumnType columnType;

    // scale and precision is for decimal column only
    // TODO: make DataType a class instead of enum
    private int scale;
    private int precision;

    public ColumnSpec() {
    }

    public ColumnSpec(String fieldName, DataType schemaDataType, ColumnType columnType) {
      this(fieldName, schemaDataType, columnType, 0, 0);
    }

    public ColumnSpec(String fieldName, DataType schemaDataType, ColumnType columnType,
        int scale, int precision) {
      this.fieldName = fieldName;
      this.schemaDataType = schemaDataType;
      this.columnType = columnType;
      this.scale = scale;
      this.precision = precision;
    }

    public DataType getSchemaDataType() {
      return schemaDataType;
    }

    public String getFieldName() {
      return fieldName;
    }

    public ColumnType getColumnType() {
      return columnType;
    }

    public int getScale() {
      return scale;
    }

    public int getPrecision() {
      return precision;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(fieldName);
      out.writeByte(schemaDataType.ordinal());
      out.writeByte(columnType.ordinal());
      out.writeInt(scale);
      out.writeInt(precision);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.fieldName = in.readUTF();
      this.schemaDataType = DataType.valueOf(in.readByte());
      this.columnType = ColumnType.valueOf(in.readByte());
      this.scale = in.readInt();
      this.precision = in.readInt();
    }
  }

  public class DimensionSpec extends ColumnSpec implements Writable {

    // indicate whether this dimension is in sort column
    private boolean inSortColumns;

    // indicate whether this dimension need to do inverted index
    private boolean doInvertedIndex;

    DimensionSpec(ColumnType columnType, CarbonDimension dimension) {
      super(dimension.getColName(), dimension.getDataType(), columnType, 0, 0);
      this.inSortColumns = dimension.isSortColumn();
      this.doInvertedIndex = dimension.isUseInvertedIndex();
    }

    public boolean isInSortColumns() {
      return inSortColumns;
    }

    public boolean isDoInvertedIndex() {
      return doInvertedIndex;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
    }
  }

  public class MeasureSpec extends ColumnSpec implements Writable {

    MeasureSpec(String fieldName, DataType dataType, int scale, int precision) {
      super(fieldName, dataType, ColumnType.MEASURE, scale, precision);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
    }
  }
}