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

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("serial")
public class Params extends ArrayList<Param> {
	
	public Params(List<Param> params) {
		this(params.toArray(new Param[0]));
	}
	
	public Params(Param... config) {
		super(config.length);
		for(Param cfg: config) {
			add(cfg);
		}
	}
	
	public static Params NONE = new Params();
}
