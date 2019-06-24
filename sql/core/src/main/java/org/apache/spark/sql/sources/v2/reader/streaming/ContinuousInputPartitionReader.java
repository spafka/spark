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

package org.apache.spark.sql.sources.v2.reader.streaming;

<<<<<<< HEAD:sql/catalyst/src/main/java/org/apache/spark/sql/sources/v2/reader/streaming/ContinuousPartitionReader.java
import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.sources.v2.reader.PartitionReader;
=======
import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
>>>>>>> a71e90a76a982dde09d3b60bb2cf4548c62f57a1:sql/core/src/main/java/org/apache/spark/sql/sources/v2/reader/streaming/ContinuousInputPartitionReader.java

/**
 * A variation on {@link InputPartitionReader} for use with streaming in continuous processing mode.
 */
<<<<<<< HEAD:sql/catalyst/src/main/java/org/apache/spark/sql/sources/v2/reader/streaming/ContinuousPartitionReader.java
@Evolving
public interface ContinuousPartitionReader<T> extends PartitionReader<T> {

  /**
   * Get the offset of the current record, or the start offset if no records have been read.
   *
   * The execution engine will call this method along with get() to keep track of the current
   * offset. When an epoch ends, the offset of the previous record in each partition will be saved
   * as a restart checkpoint.
   */
  PartitionOffset getOffset();
=======
@InterfaceStability.Evolving
public interface ContinuousInputPartitionReader<T> extends InputPartitionReader<T> {
    /**
     * Get the offset of the current record, or the start offset if no records have been read.
     *
     * The execution engine will call this method along with get() to keep track of the current
     * offset. When an epoch ends, the offset of the previous record in each partition will be saved
     * as a restart checkpoint.
     */
    PartitionOffset getOffset();
>>>>>>> a71e90a76a982dde09d3b60bb2cf4548c62f57a1:sql/core/src/main/java/org/apache/spark/sql/sources/v2/reader/streaming/ContinuousInputPartitionReader.java
}
