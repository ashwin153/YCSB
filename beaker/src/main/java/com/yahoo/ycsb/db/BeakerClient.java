/**
 * Copyright (c) 2013-2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of 
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software            
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT                         
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the                          
 * License for the specific language governing permissions and limitations under                     
 * the License. See accompanying LICENSE file.                                                       
 *                                                                                                   
 * Submitted by Ashwin Madavan on 09/27/2017.
 */
package com.yahoo.ycsb.db;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import scala.collection.JavaConverters;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import beaker.client.Client;
import beaker.server.protobuf.Revision;

/**
 * A Caustic, YCSB client.
 *
 * @author ashwin153
 */
public class BeakerClient extends DB {

  // Address of the Beaker instance.
  private static final String BEAKER_HOST = "beaker.host";
  private static final String BEAKER_PORT = "beaker.port";

  // By default, each key has 10 possible fields.
  private static final Set<String> DEFAULT_FIELDS = new HashSet(Arrays.asList(
      "FIELD0", "FIELD1", "FIELD2", "FIELD3", "FIELD4",
      "FIELD5", "FIELD6", "FIELD7", "FIELD8", "FIELD9"
  ));

  private Client client;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per 
   * client thread.
   */
  @Override
  public void init() throws DBException {
    Properties properties = getProperties();
    this.client = Client.apply(
        properties.getProperty(BEAKER_HOST, "localhost"),
        Integer.parseInt(properties.getProperty(BEAKER_PORT, "9090"))
    );
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB instance per client 
   * thread.
   */
  @Override
  public void cleanup() throws DBException {
    this.client.close();
  }

  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a 
   * HashMap.
   *
   * @param table The name of the table.
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them.
   * @param result A Map of field/value pairs for the result.
   * @return Zero on success, a non-zero error code on error.
   */
  @Override
  public Status read(
      String table, 
      String key, 
      Set<String> fields, 
      Map<String, ByteIterator> result
  ) {
    try {
      // Execute the transaction and parse the result.
      Set<String> names = new HashSet<>();
      for (String field : (fields == null) ? DEFAULT_FIELDS : fields) {
        names.add(table + "$" + key + "$" + field);
      }

      Map<String, Revision> values = JavaConverters.mapAsJavaMap(
          this.client.get(Conversions.toSet(names)).get()
      );

      for (Map.Entry<String, Revision> entry : values.entrySet()) {
        result.put(entry.getKey(), new StringByteIterator(entry.getValue().value()));
      }

      // Return completion.
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value pair from the
   * result will be stored in a HashMap.
   *
   * @param table The name of the table.
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read.
   * @param fields The list of fields to read, or null for all of them.
   * @param result A Vector of Maps, in which each Map is a set of field/value pairs for each record.
   * @return Zero on success, a non-zero error code on error.
   */
  @Override
  public Status scan(
      String table, 
      String startkey, 
      int recordcount, 
      Set<String> fields, 
      Vector<HashMap<String, ByteIterator>> result
  ) {
    return Status.ERROR;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be
   * written into the record with the specified record key, overwriting any existing values with the
   * same field name.
   *
   * @param table The name of the table.
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record.
   * @return Zero on success, a non-zero error code on error.
   */
  @Override
  public Status update(
      String table,
      String key,
      Map<String, ByteIterator> values
  ) {
    // Update is equivalent to an insert.
    return insert(table, key, values);
  }

  /**
   * Insert a record into the database. Any field/value pairs in the specified values HashMap will
   * be written into the record with the specified record key.
   *
   * @param table The name of the table.
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record.
   * @return Zero on success, a non-zero error code on error.
   */
  @Override
  public Status insert(
      String table, 
      String key, 
      Map<String, ByteIterator> values
  ) {
    // Construct the transaction.
    Map<String, String> updates = new HashMap<>();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      updates.put(table + "$" + key + "$" + entry.getKey(), entry.getValue().toString());
    }

    try {
      // Execute the transaction.
      this.client.put(Conversions.toMap(updates));
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error.
   */
  @Override
  public Status delete(String table, String key) {
    // Delete all the fields of the key.
    Map<String, ByteIterator> fields = new HashMap<>();
    for (String field : DEFAULT_FIELDS) {
      fields.put(field, new StringByteIterator(""));
    }

    // Deletion is equivalent to an insert.
    return insert(table, key, fields);
  }

}
