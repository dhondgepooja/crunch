/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.crunch.io;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import com.cloudera.crunch.Target;
import com.cloudera.crunch.types.PType;

public interface MapReduceTarget extends Target {
  void configureForMapReduce(Job job, PType<?> ptype, Path outputPath, String name);
}