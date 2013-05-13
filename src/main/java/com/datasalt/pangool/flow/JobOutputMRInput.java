/**
 * Copyright [2012] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasalt.pangool.flow;

import com.datasalt.pangool.io.Schema;

/**
 * Abstraction of an input which is derived from a property of a Job. This property can be the main output or a named
 * output.
 */
public class JobOutputMRInput implements MRInput {

	String jobName;
	String name;
	Schema schema;

	public JobOutputMRInput(String jobName, Schema schema) {
		this(jobName, "output", schema);
	}

	public JobOutputMRInput(String jobName, String name, Schema schema) {
		this.jobName = jobName;
		this.name = name;
		this.schema = schema;
	}

	/**
	 * The final Input ID is "jobName" + "." + "outputName".
	 */
	@Override
	public String getId() {
		return jobName + "." + name;
	}

	@Override
  public Schema getSchema() {
	  return schema;
  }
}
