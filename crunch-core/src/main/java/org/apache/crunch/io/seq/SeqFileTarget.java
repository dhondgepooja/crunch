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
package org.apache.crunch.io.seq;

import org.apache.crunch.SourceTarget;
import org.apache.crunch.io.FileNamingScheme;
import org.apache.crunch.io.SequentialFileNamingScheme;
import org.apache.crunch.io.impl.FileTargetImpl;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class SeqFileTarget extends FileTargetImpl {
  public SeqFileTarget(String path) {
    this(new Path(path));
  }

  public SeqFileTarget(Path path) {
    this(path, SequentialFileNamingScheme.getInstance());
  }

  public SeqFileTarget(Path path, FileNamingScheme fileNamingScheme) {
    super(path, SequenceFileOutputFormat.class, fileNamingScheme);
  }

  @Override
  public String toString() {
    return "SeqFile(" + path.toString() + ")";
  }

  @Override
  public <T> SourceTarget<T> asSourceTarget(PType<T> ptype) {
    if (ptype instanceof PTableType) {
      return new SeqFileTableSourceTarget(path, (PTableType) ptype);
    } else {
      return new SeqFileSourceTarget(path, ptype);
    }
  }
}
