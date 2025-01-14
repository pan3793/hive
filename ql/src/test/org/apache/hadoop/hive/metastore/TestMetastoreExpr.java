/*
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

package org.apache.hadoop.hive.metastore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Stack;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;

import com.google.common.collect.Lists;


import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

/**
 * Tests hive metastore expression support. This should be moved in metastore module
 * as soon as we are able to use ql from metastore server (requires splitting metastore
 * server and client).
 * This is a "companion" test to test to TestHiveMetaStore#testPartitionFilter; thus,
 * it doesn't test all the edge cases of the filter (if classes were merged, perhaps the
 * filter test could be rolled into it); assumption is that they use the same path in SQL/JDO.
 */
public class TestMetastoreExpr {
  protected static HiveMetaStoreClient client;

  @After
  public void tearDown() throws Exception {
    try {

      client.close();
    } catch (Throwable e) {
      System.err.println("Unable to close metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw new Exception(e);
    }
  }

  @Before
  public void setUp() throws Exception {

    try {
      client = new HiveMetaStoreClient(new HiveConf(this.getClass()));
    } catch (Throwable e) {
      System.err.println("Unable to open the metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw new Exception(e);
    }
  }

  private static void silentDropDatabase(String dbName) throws TException {
    try {
      for (String tableName : client.getTables(dbName, "*")) {
        client.dropTable(dbName, tableName);
      }
      client.dropDatabase(dbName);
    } catch (NoSuchObjectException|MetaException ignore) {
    } catch (InvalidOperationException ignore) {
    }
  }

  @Test
  public void testPartitionExpr() throws Exception {
    String dbName = "filterdb";
    String tblName = "filtertbl";

    silentDropDatabase(dbName);
    Database db = new Database();
    db.setName(dbName);
    client.createDatabase(db);

    ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
    cols.add(new FieldSchema("c1", serdeConstants.STRING_TYPE_NAME, ""));
    cols.add(new FieldSchema("c2", serdeConstants.INT_TYPE_NAME, ""));
    ArrayList<FieldSchema> partCols = Lists.newArrayList(
        new FieldSchema("p1", serdeConstants.STRING_TYPE_NAME, ""),
        new FieldSchema("p2", serdeConstants.INT_TYPE_NAME, ""));

    Table tbl = new Table();
    tbl.setDbName(dbName);
    tbl.setTableName(tblName);
    addSd(cols, tbl);

    tbl.setPartitionKeys(partCols);
    client.createTable(tbl);
    tbl = client.getTable(dbName, tblName);

    addPartition(client, tbl, Lists.newArrayList("p11", "32"), "part1");
    addPartition(client, tbl, Lists.newArrayList("p12", "32"), "part2");
    addPartition(client, tbl, Lists.newArrayList("p13", "31"), "part3");
    addPartition(client, tbl, Lists.newArrayList("p14", "-33"), "part4");

    ExprBuilder e = new ExprBuilder(tblName);

    checkExpr(3, dbName, tblName, e.val(0).intCol("p2").pred(">", 2).build(), tbl);
    checkExpr(3, dbName, tblName, e.intCol("p2").val(0).pred("<", 2).build(), tbl);
    checkExpr(1, dbName, tblName, e.intCol("p2").val(0).pred(">", 2).build(), tbl);
    checkExpr(2, dbName, tblName, e.val(31).intCol("p2").pred("<=", 2).build(), tbl);
    checkExpr(3, dbName, tblName, e.val("p11").strCol("p1").pred(">", 2).build(), tbl);
    checkExpr(1, dbName, tblName, e.val("p11").strCol("p1").pred(">", 2)
        .intCol("p2").val(31).pred("<", 2).pred("and", 2).build(), tbl);
    checkExpr(3, dbName, tblName,
        e.val(32).val(31).intCol("p2").val(false).pred("between", 4).build(), tbl);

    addPartition(client, tbl, Lists.newArrayList("__HIVE_DEFAULT_PARTITION__", "36"), "part5");
    addPartition(client, tbl, Lists.newArrayList("p16", "__HIVE_DEFAULT_PARTITION__"), "part6");

    // Apply isnull and instr (not supported by pushdown) via name filtering.
    checkExpr(5, dbName, tblName, e.val("p").strCol("p1")
        .fn("instr", TypeInfoFactory.intTypeInfo, 2).val(0).pred("<=", 2).build(), tbl);
    checkExpr(1, dbName, tblName, e.intCol("p2").pred("isnull", 1).build(), tbl);
    checkExpr(1, dbName, tblName, e.val("__HIVE_DEFAULT_PARTITION__").intCol("p2").pred("=", 2).build(), tbl);
    checkExpr(5, dbName, tblName, e.intCol("p1").pred("isnotnull", 1).build(), tbl);
    checkExpr(5, dbName, tblName, e.val("__HIVE_DEFAULT_PARTITION__").strCol("p1").pred("!=", 2).build(), tbl);

    // Cannot deserialize => throw the specific exception.
    try {
      client.listPartitionsByExpr(dbName, tblName,
          new byte[] { 'f', 'o', 'o' }, null, (short)-1, new ArrayList<Partition>());
      fail("Should have thrown IncompatibleMetastoreException");
    } catch (IMetaStoreClient.IncompatibleMetastoreException ignore) {
    }

    // Invalid expression => throw some exception, but not incompatible metastore.
    try {
      checkExpr(-1, dbName, tblName, e.val(31).intCol("p3").pred(">", 2).build(), tbl);
      fail("Should have thrown");
    } catch (IMetaStoreClient.IncompatibleMetastoreException ignore) {
      fail("Should not have thrown IncompatibleMetastoreException");
    } catch (Exception ignore) {
    }
  }

  public void checkExpr(int numParts,
      String dbName, String tblName, ExprNodeGenericFuncDesc expr, Table t) throws Exception {
    List<Partition> parts = new ArrayList<Partition>();
    client.listPartitionsByExpr(dbName, tblName,
        SerializationUtilities.serializeObjectWithTypeInformation(expr), null, (short)-1, parts);
    assertEquals("Partition check failed: " + expr.getExprString(), numParts, parts.size());

    // check with partition spec as well
    PartitionsByExprRequest req = new PartitionsByExprRequest(dbName, tblName,
        ByteBuffer.wrap(SerializationUtilities.serializeObjectWithTypeInformation(expr)));
    req.setMaxParts((short)-1);
    req.setId(t.getId());

    List<PartitionSpec> partSpec = new ArrayList<>();
    client.listPartitionsSpecByExpr(req, partSpec);
    int partSpecSize = 0;
    if(!partSpec.isEmpty()) {
      partSpecSize = partSpec.iterator().next().getSharedSDPartitionSpec().getPartitionsSize();
    }
    assertEquals("Partition Spec check failed: " + expr.getExprString(), numParts, partSpecSize);
  }


  /**
   * Helper class for building an expression.
   */
  public static class ExprBuilder {
    private final String tblName;
    private final Stack<ExprNodeDesc> stack = new Stack<ExprNodeDesc>();

    public ExprBuilder(String tblName) {
      this.tblName = tblName;
    }

    public ExprNodeGenericFuncDesc build() throws Exception {
      if (stack.size() != 1) {
        throw new Exception("Bad test: " + stack.size());
      }
      return (ExprNodeGenericFuncDesc)stack.pop();
    }

    public ExprBuilder pred(String name, int args) throws Exception {
      return fn(name, TypeInfoFactory.booleanTypeInfo, args);
    }

    private ExprBuilder fn(String name, TypeInfo ti, int args) throws Exception {
      List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
      for (int i = 0; i < args; ++i) {
        children.add(stack.pop());
      }
      stack.push(new ExprNodeGenericFuncDesc(ti,
          FunctionRegistry.getFunctionInfo(name).getGenericUDF(), children));
      return this;
    }

    public ExprBuilder strCol(String col) {
      return colInternal(TypeInfoFactory.stringTypeInfo, col, true);
    }
    public ExprBuilder intCol(String col) {
      return colInternal(TypeInfoFactory.intTypeInfo, col, true);
    }
    private ExprBuilder colInternal(TypeInfo ti, String col, boolean part) {
      stack.push(new ExprNodeColumnDesc(ti, col, tblName, part));
      return this;
    }

    public ExprBuilder val(String val) {
      return valInternal(TypeInfoFactory.stringTypeInfo, val);
    }
    public ExprBuilder val(int val) {
      return valInternal(TypeInfoFactory.intTypeInfo, val);
    }
    public ExprBuilder val(boolean val) {
      return valInternal(TypeInfoFactory.booleanTypeInfo, val);
    }
    private ExprBuilder valInternal(TypeInfo ti, Object val) {
      stack.push(new ExprNodeConstantDesc(ti, val));
      return this;
    }
  }

  private void addSd(ArrayList<FieldSchema> cols, Table tbl) {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(cols);
    sd.setCompressed(false);
    sd.setNumBuckets(1);
    sd.setParameters(new HashMap<String, String>());
    sd.setBucketCols(new ArrayList<String>());
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setName(tbl.getTableName());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.getSerdeInfo().getParameters()
        .put(serdeConstants.SERIALIZATION_FORMAT, "1");
    sd.setSortCols(new ArrayList<Order>());
    sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());
    sd.setInputFormat(HiveInputFormat.class.getName());
    sd.setOutputFormat(HiveOutputFormat.class.getName());
    tbl.setSd(sd);
  }

  private void addPartition(HiveMetaStoreClient client, Table table,
      List<String> vals, String location) throws TException {

    Partition part = new Partition();
    part.setDbName(table.getDbName());
    part.setTableName(table.getTableName());
    part.setValues(vals);
    part.setParameters(new HashMap<String, String>());
    part.setSd(table.getSd().deepCopy());
    part.getSd().setSerdeInfo(table.getSd().getSerdeInfo());
    part.getSd().setLocation(table.getSd().getLocation() + location);

    client.add_partition(part);
  }
}
