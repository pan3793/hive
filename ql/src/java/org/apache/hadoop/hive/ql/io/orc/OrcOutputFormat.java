/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.StatsProvidingRecordWriter;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde.OrcSerdeRow;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

/**
 * A Hive OutputFormat for ORC files.
 */
public class OrcOutputFormat extends FileOutputFormat<NullWritable, OrcSerdeRow>
    implements HiveOutputFormat<NullWritable, OrcSerdeRow> {

  private static final Logger LOG = LoggerFactory.getLogger(OrcOutputFormat.class);

  private static class OrcRecordWriter
      implements RecordWriter<NullWritable, OrcSerdeRow>,
                 StatsProvidingRecordWriter {
    private Writer writer = null;
    private final Path path;
    private final OrcFile.WriterOptions options;
    private final SerDeStats stats;

    OrcRecordWriter(Path path, OrcFile.WriterOptions options) {
      this.path = path;
      this.options = options;
      this.stats = new SerDeStats();
    }

    @Override
    public void write(NullWritable nullWritable,
                      OrcSerdeRow row) throws IOException {
      if (writer == null) {
        options.inspector(row.getInspector());
        writer = OrcFile.createWriter(path, options);
      }
      writer.addRow(row.getRow());
    }

    @Override
    public void write(Writable row) throws IOException {
      OrcSerdeRow serdeRow = (OrcSerdeRow) row;
      if (writer == null) {
        options.inspector(serdeRow.getInspector());
        writer = OrcFile.createWriter(path, options);
      }
      writer.addRow(serdeRow.getRow());
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      close(true);
    }

    @Override
    public void close(boolean b) throws IOException {
      if (writer == null) {
        // we are closing a file without writing any data in it
        FileSystem fs = options.getFileSystem() == null ?
            path.getFileSystem(options.getConfiguration()) : options.getFileSystem();
        fs.createNewFile(path);
        return;
      }
      writer.close();
    }

    @Override
    public SerDeStats getStats() {
      stats.setRawDataSize(null == writer ? 0 : writer.getRawDataSize());
      stats.setRowCount(null == writer ? 0 : writer.getNumberOfRows());
      return stats;
    }
  }

  private OrcFile.WriterOptions getOptions(JobConf conf, Properties props) {
    OrcFile.WriterOptions result = OrcFile.writerOptions(props, conf);
    if (props != null) {
      final String columnNameProperty =
          props.getProperty(IOConstants.COLUMNS);
      final String columnTypeProperty =
          props.getProperty(IOConstants.COLUMNS_TYPES);
      if (columnNameProperty != null &&
          !columnNameProperty.isEmpty() &&
          columnTypeProperty != null &&
          !columnTypeProperty.isEmpty()) {
        List<String> columnNames;
        List<TypeInfo> columnTypes;
        final String columnNameDelimiter = props.containsKey(serdeConstants.COLUMN_NAME_DELIMITER) ? props
            .getProperty(serdeConstants.COLUMN_NAME_DELIMITER) : String.valueOf(SerDeUtils.COMMA);
        if (columnNameProperty.length() == 0) {
          columnNames = new ArrayList<String>();
        } else {
          columnNames = Arrays.asList(columnNameProperty.split(columnNameDelimiter));
        }

        if (columnTypeProperty.length() == 0) {
          columnTypes = new ArrayList<TypeInfo>();
        } else {
          columnTypes =
              TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
        }

        TypeDescription schema = TypeDescription.createStruct();
        for (int i = 0; i < columnNames.size(); ++i) {
          schema.addField(columnNames.get(i),
              OrcInputFormat.convertTypeInfo(columnTypes.get(i)));
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("ORC schema = " + schema);
        }
        result.setSchema(schema);
      }
    }
    return result;
  }

  @Override
  public RecordWriter<NullWritable, OrcSerdeRow>
  getRecordWriter(FileSystem fileSystem, JobConf conf, String name,
                  Progressable reporter) throws IOException {
    return new
        OrcRecordWriter(new Path(name), getOptions(conf, null));
  }


  public StatsProvidingRecordWriter
     getHiveRecordWriter(JobConf conf,
                         Path path,
                         Class<? extends Writable> valueClass,
                         boolean isCompressed,
                         Properties tableProperties,
                         Progressable reporter) throws IOException {
    return new OrcRecordWriter(path, getOptions(conf, tableProperties));
  }


}
