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

package org.apache.carbondata.presto;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanExpression;
import org.apache.carbondata.core.scan.expression.conditional.InExpression;
import org.apache.carbondata.core.scan.expression.conditional.LessThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.LessThanExpression;
import org.apache.carbondata.core.scan.expression.conditional.ListExpression;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.expression.logical.OrExpression;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * PrestoFilterUtil create the carbonData Expression from the presto-domain
 */
public class PrestoFilterUtil {

  private static Map<Integer, Expression> filterMap = new HashMap<>();

  private static DataType Spi2CarbondataTypeMapper(CarbondataColumnHandle carbondataColumnHandle) {
    Type colType = carbondataColumnHandle.getColumnType();
    if (colType == BooleanType.BOOLEAN) return DataType.BOOLEAN;
    else if (colType == SmallintType.SMALLINT) return DataType.SHORT;
    else if (colType == IntegerType.INTEGER) return DataType.INT;
    else if (colType == BigintType.BIGINT) return DataType.LONG;
    else if (colType == DoubleType.DOUBLE) return DataType.DOUBLE;
    else if (colType == VarcharType.VARCHAR) return DataType.STRING;
    else if (colType == DateType.DATE) return DataType.DATE;
    else if (colType == TimestampType.TIMESTAMP) return DataType.TIMESTAMP;
    else if (colType.equals(DecimalType.createDecimalType(carbondataColumnHandle.getPrecision(),
        carbondataColumnHandle.getScale()))) return DataType.DECIMAL;
    else return DataType.STRING;
  }

  /**
   * Convert presto-TupleDomain predication into Carbon scan express condition
   *
   * @param originalConstraint presto-TupleDomain
   * @return
   */
  static Expression parseFilterExpression(TupleDomain<ColumnHandle> originalConstraint) {
    ImmutableList.Builder<Expression> filters = ImmutableList.builder();

    Domain domain;

    for (ColumnHandle c : originalConstraint.getDomains().get().keySet()) {

      // Build ColumnExpression for Expression(Carbondata)
      CarbondataColumnHandle cdch = (CarbondataColumnHandle) c;
      Type type = cdch.getColumnType();

      DataType coltype = Spi2CarbondataTypeMapper(cdch);
      Expression colExpression = new ColumnExpression(cdch.getColumnName(), coltype);

      domain = originalConstraint.getDomains().get().get(c);
      checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");

      List<Object> singleValues = new ArrayList<>();
      List<Expression> disjuncts = new ArrayList<>();
      for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
        if (range.isSingleValue()) {
          Object value = ConvertDataByType(range.getLow().getValue(), type);
          singleValues.add(value);
        } else {
          List<Expression> rangeConjuncts = new ArrayList<>();
          if (!range.getLow().isLowerUnbounded()) {
            Object value = ConvertDataByType(range.getLow().getValue(), type);
            switch (range.getLow().getBound()) {
              case ABOVE:
                if (type == TimestampType.TIMESTAMP) {
                  //todo not now
                } else {
                  GreaterThanExpression greater = new GreaterThanExpression(colExpression,
                      new LiteralExpression(value, coltype));
                  rangeConjuncts.add(greater);
                }
                break;
              case EXACTLY:
                GreaterThanEqualToExpression greater =
                    new GreaterThanEqualToExpression(colExpression,
                        new LiteralExpression(value, coltype));
                rangeConjuncts.add(greater);
                break;
              case BELOW:
                throw new IllegalArgumentException("Low marker should never use BELOW bound");
              default:
                throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
            }
          }
          if (!range.getHigh().isUpperUnbounded()) {
            Object value = ConvertDataByType(range.getHigh().getValue(), type);
            switch (range.getHigh().getBound()) {
              case ABOVE:
                throw new IllegalArgumentException("High marker should never use ABOVE bound");
              case EXACTLY:
                LessThanEqualToExpression less = new LessThanEqualToExpression(colExpression,
                    new LiteralExpression(value, coltype));
                rangeConjuncts.add(less);
                break;
              case BELOW:
                LessThanExpression less2 =
                    new LessThanExpression(colExpression, new LiteralExpression(value, coltype));
                rangeConjuncts.add(less2);
                break;
              default:
                throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
            }
          }
          disjuncts.addAll(rangeConjuncts);
        }
      }
      if (singleValues.size() == 1) {
        Expression ex;
        if (coltype.equals(DataType.STRING)) {
          ex = new EqualToExpression(colExpression,
              new LiteralExpression(singleValues.get(0), coltype));
        } else if (coltype.equals(DataType.TIMESTAMP) || coltype.equals(DataType.DATE)) {
          Long value = (Long) singleValues.get(0);
          ex = new EqualToExpression(colExpression, new LiteralExpression(value, coltype));
        } else ex = new EqualToExpression(colExpression,
            new LiteralExpression(singleValues.get(0), coltype));
        filters.add(ex);
      } else if (singleValues.size() > 1) {
        ListExpression candidates = null;
        List<Expression> exs = singleValues.stream()
            .map((a) -> new LiteralExpression(ConvertDataByType(a, type), coltype))
            .collect(Collectors.toList());
        candidates = new ListExpression(exs);

        filters.add(new InExpression(colExpression, candidates));
      } else if (disjuncts.size() > 0) {
        if (disjuncts.size() > 1) {
          Expression finalFilters = new AndExpression(disjuncts.get(0), disjuncts.get(1));
          if (disjuncts.size() > 2) {
            for (int i = 2; i < disjuncts.size(); i++) {
              filters.add(new AndExpression(finalFilters, disjuncts.get(i)));
            }
          } else {
            filters.add(finalFilters);
          }
        } else if (disjuncts.size() == 1) filters.add(disjuncts.get(0));
      }
    }

    Expression finalFilters;
    List<Expression> tmp = filters.build();
    if (tmp.size() > 1) {
      finalFilters = new AndExpression(tmp.get(0), tmp.get(1));
      if (tmp.size() > 2) {
        for (int i = 2; i < tmp.size(); i++) {
          finalFilters = new AndExpression(finalFilters, tmp.get(i));
        }
      }
    } else if (tmp.size() == 1) finalFilters = tmp.get(0);
    else return null;
    return finalFilters;
  }

  private static Object ConvertDataByType(Object rawdata, Type type) {
    if (type.equals(IntegerType.INTEGER)) return Integer.valueOf(rawdata.toString());
    // new Integer((rawdata.toString()));
    else if (type.equals(BigintType.BIGINT)) return rawdata;
    else if (type.equals(VarcharType.VARCHAR)) {
      if (rawdata instanceof Slice) {
        return ((Slice) rawdata).toStringUtf8();
      } else {
        return rawdata;
      }

    } else if (type.equals(BooleanType.BOOLEAN)) return rawdata;
    else if (type.equals(DateType.DATE)) {
      Calendar c = Calendar.getInstance();
      c.setTime(new Date(0));
      c.add(Calendar.DAY_OF_YEAR, ((Long) rawdata).intValue());
      Date date = c.getTime();
      return date.getTime() * 1000;
    }
    else if (type instanceof DecimalType) {
      if(rawdata instanceof  Double) {
        return new BigDecimal((Double) rawdata);
      } else if (rawdata instanceof  Long) {
        return new BigDecimal(new BigInteger(String.valueOf(rawdata)),
            ((DecimalType) type).getScale());
      }
    }

    return rawdata;
  }

  /**
   * get the filters from key
   */
  static Expression getFilters(Integer key) {
    return filterMap.get(key);
  }

  static void setFilter(Integer tableId, Expression filter) {
    filterMap.put(tableId, filter);
  }
}