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
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.model.CarbonQueryPlan;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.hadoop.CarbonInputFormat;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Job;

/**
 * Vectorized input format for Parquet files
 */
public class VectorizedCarbonInputFormat extends CarbonInputFormat<VectorizedRowBatch>
    implements VectorizedInputFormatInterface, InputFormat<NullWritable, VectorizedRowBatch>,
    CombineHiveInputFormat.AvoidSplitCombination {

  private static final Log LOG = LogFactory.getLog(VectorizedCarbonInputFormat.class);
  private static final String CARBON_TABLE = "mapreduce.input.carboninputformat.table";

  @Override public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
    org.apache.hadoop.mapreduce.JobContext jobContext = Job.getInstance(jobConf);
    List<org.apache.hadoop.mapreduce.InputSplit> splitList = super.getSplits(jobContext);
    InputSplit[] splits = new InputSplit[splitList.size()];
    CarbonInputSplit split;
    for (int i = 0; i < splitList.size(); i++) {
      split = (CarbonInputSplit) splitList.get(i);
      splits[i] = new CarbonHiveInputSplit(split.getSegmentId(), split.getPath(), split.getStart(),
          split.getLength(), split.getLocations(), split.getNumberOfBlocklets(), split.getVersion(),
          split.getBlockStorageIdMap());
    }
    return splits;
  }

  @Override public RecordReader<NullWritable, VectorizedRowBatch> getRecordReader(InputSplit split,
      JobConf jobConf, Reporter reporter) throws IOException {
    try {
      QueryModel queryModel = getQueryModel(jobConf);
      queryModel.setVectorReader(true);
      return  new CarbonHiveVectorizedReader(queryModel, split, jobConf);
    } catch (final InterruptedException e) {
      throw new RuntimeException("Cannot create a VectorizedParquetRecordReader", e);
    }
  }



  /**
   *
   * @param configuration
   * @return
   * @throws IOException
   */
  private QueryModel getQueryModel(Configuration configuration) throws IOException {
    CarbonTable carbonTable = getCarbonTable(configuration);
    // getting the table absoluteTableIdentifier from the carbonTable
    // to avoid unnecessary deserialization

    AbsoluteTableIdentifier identifier = carbonTable.getAbsoluteTableIdentifier();

    String projection = getProjection(configuration, carbonTable,
        identifier.getCarbonTableIdentifier().getTableName());
    CarbonQueryPlan queryPlan = CarbonInputFormatUtil.createQueryPlan(carbonTable, projection);
    QueryModel queryModel = QueryModel.createModel(identifier, queryPlan, carbonTable);
    // set the filter to the query model in order to filter blocklet before scan
    Expression filter = getFilterPredicates(configuration);
    CarbonInputFormatUtil.processFilterExpression(filter, carbonTable);
    FilterResolverIntf filterIntf = CarbonInputFormatUtil.resolveFilter(filter, identifier);
    queryModel.setFilterExpressionResolverTree(filterIntf);

    return queryModel;
  }

  /**
   * Return the Projection for the CarbonQuery.
   *
   * @param configuration
   * @param carbonTable
   * @param tableName
   * @return
   */
  private String getProjection(Configuration configuration, CarbonTable carbonTable,
      String tableName) {
    // query plan includes projection column
    String projection = getColumnProjection(configuration);
    if (projection == null) {
      projection = configuration.get("hive.io.file.readcolumn.names");
    }
    List<CarbonColumn> carbonColumns = carbonTable.getCreateOrderColumn(tableName);
    List<String> carbonColumnNames = new ArrayList<>();
    StringBuilder allColumns = new StringBuilder();
    StringBuilder projectionColumns = new StringBuilder();
    for (CarbonColumn column : carbonColumns) {
      carbonColumnNames.add(column.getColName());
      allColumns.append(column.getColName()).append(",");
    }

    if (!projection.equals("")) {
      String[] columnNames = projection.split(",");
      //verify that the columns parsed by Hive exist in the table
      for (String col : columnNames) {
        //show columns command will return these data
        if (carbonColumnNames.contains(col)) {
          projectionColumns.append(col + ",");
        }
      }
      return projectionColumns.substring(0, projectionColumns.lastIndexOf(","));
    } else {
      return allColumns.substring(0, allColumns.lastIndexOf(","));
    }
  }

  @Override public boolean shouldSkipCombine(Path path, Configuration conf) throws IOException {
    return true;
  }
}
