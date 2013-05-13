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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;

import com.datasalt.pangool.flow.io.HadoopInput;
import com.datasalt.pangool.flow.io.HadoopOutput;
import com.datasalt.pangool.flow.io.RichInput;
import com.datasalt.pangool.flow.io.RichOutput;
import com.datasalt.pangool.flow.io.TupleInput;
import com.datasalt.pangool.flow.io.TupleOutput;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.IdentityTupleReducer;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleReducer;

/**
 * Steps to be used in {@link MapReduceFlowBuilder}.
 */
@SuppressWarnings("rawtypes")
public class MRStep {

	transient TupleReducer reducer = new IdentityTupleReducer();
	transient TupleReducer combiner = null;
	transient GroupBy groupBy;
	transient OrderBy orderBy = null;

	transient LinkedHashMap<String, RichInput> bindedInputs = new LinkedHashMap<String, RichInput>();
	transient RichOutput jobOutput;
	transient LinkedHashMap<String, RichOutput> bindedOutputs = new LinkedHashMap<String, RichOutput>();

	transient LinkedHashMap<String, MRInput> bindings = new LinkedHashMap<String, MRInput>();

	transient List<OperativeStep> extraDependencies = new ArrayList<OperativeStep>();
	
	int nReducers = -1; 
	
	String name;
	String help;
	Class jarByClass;

	public MRStep(String name, Class jarByClass) {
		this(name, null, jarByClass);
	}

	public MRStep(String name, String help, Class jarByClass) {
		this.name = name;
		this.help = help;
		this.jarByClass = jarByClass;
		this.reducer = new IdentityTupleReducer();
	}

	public void groupBy(GroupBy groupBy) {
		this.groupBy = groupBy;
	}

	public void orderBy(OrderBy orderBy) {
		this.orderBy = orderBy;
	}

	public void setNReducers(int nReducers) {
		this.nReducers = nReducers;
	}
	
	public void dependsOn(OperativeStep operativeStep) {
		extraDependencies.add(operativeStep);
	}
	
	public void addInput(MRInput mrInput) {
		addInput(mrInput, new TupleInput(mrInput.getSchema()));
	}
	
	public void addInput(MRInput mrInput, Schema schema) {
		addInput(mrInput, new TupleInput(schema));
	}
	
	public void addInput(MRInput mrInput, RichInput inputSpec) {
		String inputName = "input" + bindedInputs.keySet().size();
		bindedInputs.put(inputName, inputSpec);
		bindings.put(inputName, mrInput);
	}

	public MRInput setOutput(Schema outputSchema) {
		return setOutput(new TupleOutput(outputSchema));
	}
	
	public MRInput setOutput(RichOutput outputSpec) {
		jobOutput = outputSpec;
		if(outputSpec instanceof TupleOutput) {
			System.err.println("return a schema : " + ((TupleOutput)outputSpec).getOutputSchema());
			return new JobOutputMRInput(name, ((TupleOutput)outputSpec).getOutputSchema());						
		} else {
			return new JobOutputMRInput(name, null);			
		}
	}

	public MRInput setOutput(String name, RichOutput outputSpec) {
		bindedOutputs.put(name, outputSpec);
		if(outputSpec instanceof TupleOutput) {
			return new JobOutputMRInput(this.name, name, ((TupleOutput)outputSpec).getOutputSchema());
		} else {
			return new JobOutputMRInput(this.name, name, null);
		}
	}

	public void setReducer(TupleReducer reducer) {
		this.reducer = reducer;
	}

	public void setCombiner(TupleReducer<ITuple, NullWritable> combiner) {
		this.combiner = combiner;
	}

	protected TupleMRBuilder getMRBuilder() {
		return mr;
	}

	transient TupleMRBuilder mr;

	@SuppressWarnings("serial")
	public Step getStep() {
		
		// This is a trick for making {@link OperativeStep} be dependent on {@link MRStep}
		// We do it through a fake Input whose prefix is "extradep".
		// Normal inputs are named with a prefix "input".
		List<String> inputs = new ArrayList<String>();
		for(@SuppressWarnings("unused") OperativeStep st: extraDependencies) {
			inputs.add("extradep" + inputs.size());
		}
		// Add the normal inputs
		inputs.addAll(bindedInputs.keySet());
		
		// Create the low-level Step
		return new Step(name, new Inputs(inputs.toArray(new String[0])), Params.NONE,
		    new NamedOutputs(bindedOutputs.keySet().toArray(new String[0])), help) {
			@SuppressWarnings("unchecked")
			@Override
			public int run(Path outputPath, Map<String, Path> parsedInputs,
			    Map<String, Object> parsedParameters) throws Exception {

				mr = new TupleMRBuilder(hadoopConf, getName());
				mr.setJarByClass(jarByClass);

				if(MRStep.this.nReducers > 0) {
					setNReducers(MRStep.this.nReducers);
				}
				
				for(Map.Entry<String, RichInput> inputEntry : bindedInputs.entrySet()) {
					RichInput input = inputEntry.getValue();
					String inputName = inputEntry.getKey();
					if(input instanceof HadoopInput) {
						HadoopInput hadoopInput = (HadoopInput) input;
						mr.addInput(parsedInputs.get(inputName), hadoopInput.getFormat(), hadoopInput.getProcessor());
						for(Schema schema : hadoopInput.getIntermediateSchemas()) {
							mr.addIntermediateSchema(schema);
						}
					} else if(input instanceof TupleInput) {
						TupleInput tupleInput = (TupleInput) input;
						mr.addTupleInput(parsedInputs.get(inputName), tupleInput.getProcessor());
						for(Schema schema : tupleInput.getIntermediateSchemas()) {
							mr.addIntermediateSchema(schema);
						}
					}
				}

				mr.setTupleReducer(reducer);
				if(combiner != null) {
					mr.setTupleCombiner(combiner);
				}

				if(jobOutput instanceof HadoopOutput) {
					HadoopOutput hadoopOutput = (HadoopOutput) jobOutput;
					mr.setOutput(outputPath, hadoopOutput.getOutputFormat(), hadoopOutput.getKey(),
					    hadoopOutput.getValue());
				} else if(jobOutput instanceof TupleOutput) {
					TupleOutput tupleOutput = (TupleOutput) jobOutput;
					mr.setTupleOutput(outputPath, tupleOutput.getOutputSchema());
				}

				for(Map.Entry<String, RichOutput> namedOutputEntry : bindedOutputs.entrySet()) {
					RichOutput output = namedOutputEntry.getValue();
					String outputName = namedOutputEntry.getKey();
					if(output instanceof HadoopOutput) {
						HadoopOutput hadoopOutput = (HadoopOutput) output;
						mr.addNamedOutput(outputName, hadoopOutput.getOutputFormat(), hadoopOutput.getKey(),
						    hadoopOutput.getValue());
					} else if(output instanceof TupleOutput) {
						TupleOutput tupleOutput = (TupleOutput) output;
						mr.addNamedTupleOutput(outputName, tupleOutput.getOutputSchema());
					}
				}

				mr.setGroupByFields(groupBy.groupByFields);
				if(orderBy != null) {
					mr.setOrderBy(orderBy);
				}

				return executeCoGrouper(mr);
			}
		};
	}
}