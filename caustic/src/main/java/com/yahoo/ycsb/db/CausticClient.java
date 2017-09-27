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

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ForkJoinPool;

import scala.concurrent.ExecutionContext;
import scala.concurrent.ExecutionContext$;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import caustic.runtime.jdbc.MySQLDatabase;
import caustic.runtime.thrift.Transaction;

import caustic.service.Client;
import caustic.service.Connection;
import caustic.service.JavaCodec;
import static caustic.service.JavaCodec.*;
import caustic.service.Server;

/**
 * A Caustic, YCSB client.
 *
 * @author ashwin153
 */
public class CausticClient extends DB {

  // By default, each key has 10 possible fields.
  private static final String[] DEFAULT_FIELDS = new String[] {
      "FIELD0", "FIELD1", "FIELD2", "FIELD3", "FIELD4",
      "FIELD5", "FIELD6", "FIELD7", "FIELD8", "FIELD9"
  };

  private ComboPooledDataSource pool;
  private Server server;
  private Client client;
  private boolean initialized;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per 
   * client thread.
   */
  @Override
  public void init() throws DBException {
    if (this.initialized) {
      throw new DBException("Client is already initialized.");
    }

    // Extract the database host name and port.
    Properties props = getProperties();
   
    try {
      // Setup database connection pool.
      this.pool = new ComboPooledDataSource();
      this.pool.setDriverClass(props.getProperty("db.driver"));
      this.pool.setJdbcUrl(props.getProperty("db.url"));
      this.pool.setUser(props.getProperty("db.user"));
      this.pool.setPassword(props.getProperty("db.passwd"));
    } catch (PropertyVetoException e) {
      throw new DBException("Unable to initialize connection pool.", e);
    }

    // Configure a server instance.
    ExecutionContext executor = ExecutionContext$.MODULE$.fromExecutorService(ForkJoinPool.commonPool());
    this.server = new Server(new MySQLDatabase(this.pool, executor), 9000);
    this.server.serve();

    // Bind a client connection.
    this.client = new Connection("localhost", 9000);
    this.initialized = true;
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB instance per client 
   * thread.
   */
  @Override
  public void cleanup() throws DBException {
    try {
      // Close the database client and server.
      this.client.close();
      this.server.close();
      this.pool.close();
    } catch (IOException e) {
      throw new DBException("Unable to close client/server.", e);
    }
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
    int length = (fields == null) ? DEFAULT_FIELDS.length : fields.size();
    String[] keys = new String[length];
    String[] names = (fields == null) ? DEFAULT_FIELDS : fields.toArray(new String[fields.size()]);
   
    // Construct the transaction.
    Transaction txn = Empty();
    for (int i = 0; i < names.length; i++) {
      keys[i] = table + "$" + key + "$" + names[i];
      txn = add(add(txn, JavaCodec.read(text(keys[i]))), text("\0"));
    } 

    try {
      // Execute the transaction and parse the result.
      String[] values = this.client.execute(txn).get().getText().split("\0");
      for (int i = 0; i < values.length; i++) {
        if (values[i].length() > 0) {
          result.put(names[i], new StringByteIterator(values[i]));
        }
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
    Transaction txn = Empty();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      String name = table + "$" + key + "$" + entry.getKey();
      String value = entry.getValue().toString();
      txn = cons(txn, write(text(name), text(value)));
    }

    try {
      // Execute the transaction.
      this.client.execute(txn).get();
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
   * @return
   */
  @Override
  public Status delete(String table, String key) {
    // Delete all the fields of the key.
    Map<String, ByteIterator> fields = new HashMap<>();
    for (int i = 0; i < DEFAULT_FIELDS.length; i++) {
      fields.put(DEFAULT_FIELDS[i], new StringByteIterator(""));
    }

    // Deletion is equivalent to an insert.
    return insert(table, key, fields);
  }

}
