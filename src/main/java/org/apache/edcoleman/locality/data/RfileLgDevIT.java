/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.edcoleman.locality.data;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * Uses AccumuloIt and AccumuloClusterTest to generate files with and without locality groups set.
 */
public class RfileLgDevIT extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(RfileLgDevIT.class);

  private static final int NUM_ROWS = 16 * 16 * 16;

  private static final long SLOW_SCAN_SLEEP_MS = 250L;

  private Connector connector;

  private static final ExecutorService pool = Executors.newCachedThreadPool();

  private String tableName;

  private String secret;

  private Text[] colFamilies = {new Text("G1_COL1"), new Text("G1_COL2"), new Text("G2_COL1"),
      new Text("G2_COL2"), new Text("G2_COL3"), new Text("G4_COL1"), new Text("G5_COL1")};

  @Before
  public void setup() {

    connector = getConnector();

    tableName = getUniqueNames(1)[0];

    secret = cluster.getSiteConfiguration().get(Property.INSTANCE_SECRET);

    createData(tableName);
  }

  @AfterClass
  public static void cleanup() {
    pool.shutdownNow();
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  @Test
  public void t1() throws Exception {

    String tableId = Tables.getTableId(connector.getInstance(), tableName);

    TableState tstate = Tables.getTableState(connector.getInstance(), tableId);

    assertEquals("verify table online after created", TableState.ONLINE, tstate);

    connector.tableOperations().compact(tableName, null, null, true, true);

    String newTableName = "group1_" + tableName;

    connector.tableOperations().clone(tableName, newTableName, true, null, null);

    Map<String,Set<Text>> groups = new TreeMap<>();
    Set<Text> g1 = new TreeSet<>();
    groups.put("z_group_1", g1);
    Set<Text> g2 = new TreeSet<>();
    groups.put("a_group_2", g2);

    Set<Text> g3 = new TreeSet<>();
    g3.add(new Text("na"));
    groups.put("na_group", g3);

    for (Text colFamily : colFamilies) {
      String x = colFamily.toString();
      if (x.startsWith("G1")) {
        g1.add(colFamily);
      } else if (x.startsWith("G2")) {
        g2.add(colFamily);
      }
    }

    connector.tableOperations().setLocalityGroups(newTableName, groups);

    connector.tableOperations().compact(newTableName, null, null, true, true);

    log.debug("TG: {}", connector.tableOperations().getLocalityGroups(newTableName));

    log.debug("Groups {}", groups);
  }

  /**
   * Create the provided table and populate with some data using a batch writer. The table is
   * scanned to ensure it was populated as expected.
   *
   * @param tableName
   *          the name of the table
   */
  private void createData(final String tableName) {

    try {

      // create table.
      connector.tableOperations().create(tableName);
      BatchWriter bw = connector.createBatchWriter(tableName, new BatchWriterConfig());

      int dataCount = 0;

      // populate
      for (int i = 0; i < NUM_ROWS; i++) {

        for (Text colFamily : colFamilies) {

          Mutation m = new Mutation(new Text(String.format("%03x", i)));

          m.put(colFamily, new Text("qual"),
              new Value(String.format("%05d", dataCount++).getBytes(UTF_8)));
          bw.addMutation(m);
        }
      }

      bw.close();

      long startTimestamp = System.nanoTime();

      int count = scanCount(tableName);

      log.trace("Scan time for {} rows {} ms", NUM_ROWS, TimeUnit.MILLISECONDS
          .convert((System.nanoTime() - startTimestamp), TimeUnit.NANOSECONDS));

      if (count != (NUM_ROWS * colFamilies.length)) {
        throw new IllegalStateException(
            String.format("Number of rows %1$d does not match expected %2$d", count, NUM_ROWS));
      }
    } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException
        | TableExistsException ex) {
      throw new IllegalStateException("Create data failed with exception", ex);
    }
  }

  private int scanCount(String tableName) throws TableNotFoundException {

    Scanner scanner = connector.createScanner(tableName, Authorizations.EMPTY);
    int count = 0;
    for (Map.Entry<Key,Value> elt : scanner) {
      // String expected = String.format("%05d", count);
      // assert (elt.getKey().getRow().toString().equals(expected));
      count++;
    }

    scanner.close();

    return count;
  }

  /**
   * Provides timing information for online operation.
   */
  private static class OnlineOpTiming {

    private final long started;
    private long completed = 0L;

    OnlineOpTiming() {
      started = System.nanoTime();
    }

    /**
     * stop timing and set completion flag.
     */
    void setComplete() {
      completed = System.nanoTime();
    }

    /**
     * @return running time in nanoseconds.
     */
    long runningTime() {
      return completed - started;
    }
  }

  /**
   * Run online operation in a separate thread and gather timing information.
   */
  private class OnLineCallable implements Callable<OnlineOpTiming> {

    final String tableName;

    /**
     * Create an instance of this class to set the provided table online.
     *
     * @param tableName
     *          The table name that will be set online.
     */
    OnLineCallable(final String tableName) {
      this.tableName = tableName;
    }

    @Override
    public OnlineOpTiming call() throws Exception {

      OnlineOpTiming status = new OnlineOpTiming();

      log.trace("Setting {} online", tableName);

      connector.tableOperations().online(tableName, true);
      // stop timing
      status.setComplete();

      log.trace("Online completed in {} ms",
          TimeUnit.MILLISECONDS.convert(status.runningTime(), TimeUnit.NANOSECONDS));

      return status;
    }
  }

}
