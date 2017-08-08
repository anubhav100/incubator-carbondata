package org.apache.carbondata.presto.scan.executor.impl;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.datastore.block.AbstractIndex;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.stats.QueryStatisticsRecorder;


class CarbonQueryExecutorProperties {
  /**
   * this will hold the information about the dictionary dimension
   * which to
   */
  Map<String, Dictionary> columnToDictionayMapping;

  /**
   * Measure datatypes
   */
  DataType[] measureDataTypes;
  /**
   * all the complex dimension which is on filter
   */
  Set<CarbonDimension> complexFilterDimension;

  Set<CarbonMeasure> filterMeasures;
  /**
   * to record the query execution details phase wise
   */
  QueryStatisticsRecorder queryStatisticsRecorder;
  /**
   * executor service to execute the query
   */
  ExecutorService executorService;
  /**
   * list of blocks in which query will be executed
   */
  List dataBlocks;
}
