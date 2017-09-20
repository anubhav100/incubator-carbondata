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

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.iterator.AbstractDetailQueryResultIterator;
import org.apache.carbondata.core.scan.result.iterator.ChunkRowIterator;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonRecordReader;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.sql.execution.vectorized.ColumnarBatch;

class CarbonHiveRecordReader extends CarbonRecordReader<ArrayWritable>
    implements org.apache.hadoop.mapred.RecordReader<Void, ArrayWritable> {

  private ArrayWritable valueObj = null;
  private long recordReaderCounter = 0;
  private int[] columnIds;
  private  CarbonVectorizedRecordReader vectorReader;
  private ColumnarBatch columnarBatch;
  private int batchSize;

  public CarbonHiveRecordReader(QueryModel queryModel, CarbonDictionaryDecodeReadSupport<ArrayWritable> readSupport,
      InputSplit inputSplit, JobConf jobConf) throws IOException, QueryExecutionException {

    super(queryModel, readSupport);
    readSupport.initialize(queryModel.getProjectionColumns(), queryModel.getAbsoluteTableIdentifier());
    CarbonIterator iterator = queryExecutor.execute(queryModel);
    vectorReader =
        new CarbonVectorizedRecordReader(queryExecutor, queryModel,
            (AbstractDetailQueryResultIterator) iterator);
    readColumns(jobConf);
  }

  private void readColumns(Configuration conf) throws IOException {

    List<TypeInfo> columnTypes;
    // Get column names and sort order
    final String colIds = conf.get("hive.io.file.readcolumn.ids");
    final String columnTypeProperty = conf.get(serdeConstants.LIST_COLUMN_TYPES);

    if (columnTypeProperty.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }

    if (valueObj == null) {
      valueObj = new ArrayWritable(Writable.class, new Writable[columnTypes.size()]);
    }

    if (!colIds.equals("")) {
      String[] arraySelectedColId = colIds.split(",");
      columnIds = new int[arraySelectedColId.length];
      int columnId = 0;
      for (int j = 0; j < arraySelectedColId.length; j++) {
        columnId = Integer.parseInt(arraySelectedColId[j]);
        columnIds[j] = columnId;
      }
    }

  }

  @Override public boolean next(Void aVoid, ArrayWritable value) throws IOException {
    boolean isData = true;
    try {
      if (vectorReader.nextKeyValue()) {
        Object vectorBatch = vectorReader.getCurrentValue();
        if (vectorBatch != null && vectorBatch instanceof ColumnarBatch) {
          columnarBatch = (ColumnarBatch) vectorBatch;
          batchSize = columnarBatch.numRows();
          if (batchSize == 0) {
            close();
            isData = false;
          } else {
            CarbonDictionaryDecodeReadSupport carbonDictionaryDecodeReadSupport = (CarbonDictionaryDecodeReadSupport) readSupport;
            valueObj.set((Writable[])carbonDictionaryDecodeReadSupport.readBatches(columnarBatch,columnIds.length));
            isData = true;
          }

        } else {
          throw new Exception("Unable to get the Carbon Batch");
        }
      }
      else{
        isData= false;
      }
    }catch (Exception e) {
      e.printStackTrace();
    }
return isData;

  }

  @Override public Void createKey() {
    return null;
  }

  @Override public ArrayWritable createValue() {
    return valueObj;
  }

  @Override public long getPos() throws IOException {
    return recordReaderCounter;
  }

  @Override public float getProgress() throws IOException {
    return 0;
  }


}