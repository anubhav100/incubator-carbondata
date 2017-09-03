package org.apache.carbondata.presto.readers;

import java.io.IOException;
import java.util.Arrays;

import org.apache.carbondata.core.cache.dictionary.ColumnDictionaryInfo;
import org.apache.carbondata.core.metadata.datatype.DataType;

import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.Type;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.StructField;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertNotNull;
import static org.apache.spark.sql.types.DataTypes.DoubleType;

public class SliceStreamReaderTest {
  private static SliceStreamReader sliceStreamReader;
  private static Object[] objects;
  private static ColumnVector columnVector;
  private static Type type;

  @BeforeClass public static void setUp() {
    sliceStreamReader = new SliceStreamReader();
    StructField structField = new StructField("column1", DoubleType, false, null);
    columnVector = ColumnVector.allocate(5, structField.dataType(), MemoryMode.ON_HEAP);
    boolean[] filteredRows = new boolean[2];
    Arrays.fill(filteredRows, false);
    sliceStreamReader.setVector(columnVector);
    objects = new Object[] { 'a' };
    sliceStreamReader.setStreamData(objects);
    type = CharType.createCharType(1000L);

  }

  @Test public void testReadBlockWithVectorReader() throws IOException {
    sliceStreamReader.setVectorReader(true);
    sliceStreamReader.readBlock(type);
    assertNotNull(sliceStreamReader);
  }

  @Test public void testReadBlockWithVectorReaderAndDict() throws IOException {
    sliceStreamReader = new SliceStreamReader(true, new ColumnDictionaryInfo(DataType.STRING));
    StructField structField = new StructField("column1", DoubleType, false, null);
    columnVector = ColumnVector.allocate(5, structField.dataType(), MemoryMode.ON_HEAP);
    boolean[] filteredRows = new boolean[2];
    Arrays.fill(filteredRows, false);
    sliceStreamReader.setVector(columnVector);

    sliceStreamReader.setVectorReader(true);
    sliceStreamReader.readBlock(type);
    assertNotNull(sliceStreamReader);
  }

  @Test public void testReadBlockWithOutVectorReaderAndWithDict() throws IOException {
    sliceStreamReader = new SliceStreamReader(true, new ColumnDictionaryInfo(DataType.STRING));
    StructField structField = new StructField("column1", DoubleType, false, null);
    columnVector = ColumnVector.allocate(5, structField.dataType(), MemoryMode.ON_HEAP);
    boolean[] filteredRows = new boolean[2];
    Arrays.fill(filteredRows, false);
    sliceStreamReader.setVector(columnVector);

    sliceStreamReader.setVectorReader(false);
    sliceStreamReader.setStreamData(new Object[] { "1,2" });
    sliceStreamReader.readBlock(type);
    assertNotNull(sliceStreamReader);
  }
}
