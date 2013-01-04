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
package com.datasalt.pangool.flow.io;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.datasalt.pangool.flow.mapred.TextMapper;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;

/**
 * Input spec for text inputs.
 */
public class TextInput extends HadoopInput {

	public TextInput(TextMapper processor) {
	  super(new HadoopInputFormat(TextInputFormat.class), processor, processor.getIntermediateSchema());
  }
}
