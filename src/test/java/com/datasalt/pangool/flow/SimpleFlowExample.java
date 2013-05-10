package com.datasalt.pangool.flow;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import com.datasalt.pangool.flow.BaseFlow.EXECUTION_MODE;
import com.datasalt.pangool.flow.io.TextInput;
import com.datasalt.pangool.flow.io.TextOutput;
import com.datasalt.pangool.flow.mapred.SingleSchemaReducer;
import com.datasalt.pangool.flow.mapred.TextMapper;
import com.datasalt.pangool.flow.ops.ReturnCallback;
import com.datasalt.pangool.flow.ops.TupleOp;
import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.TupleMRException;

/**
 * Work-in-progress
 */
@SuppressWarnings({ "serial", "unchecked", "rawtypes" })
public class SimpleFlowExample implements Serializable {

	@Test
	public void test() throws Exception {
		MapReduceFlowBuilder builder = new MapReduceFlowBuilder();

		// Define the main input of the flow
		ResourceMRInput mainInput = new ResourceMRInput("src/test/resources/barackobama_tweets.json");
		// Initial inputs MUST be added (tricky)
		builder.addInput(mainInput);

		final Schema tweetSchema = new Schema("tweet",
		    Fields.parse("date:string?,text:string,screen_name:string,location:string?"));

		// Operation that parses a JSON tweet and converts it into a tuple with some fields (see above tweetSchema).
		TupleOp<String> tweetParseOp = new TupleOp<String>() {

			ObjectMapper mapper;
			ITuple tuple = new Tuple(tweetSchema);

      @Override
			public void process(String input, ReturnCallback<ITuple> callback) throws IOException,
			    InterruptedException {

				if(mapper == null) { // because it can't be serialized, we lazily initialize it
					mapper = new ObjectMapper();
				}
				Map<String, Object> map = mapper.readValue(input, HashMap.class);
				tuple.set(0, map.get("created_at"));
				tuple.set(1, map.get("text"));
				tuple.set(2, map.get("user") != null ? ((Map) map.get("user")).get("screen_name") : null);
				tuple.set(3, map.get("user") != null ? ((Map) map.get("user")).get("location") : null);
				// don't forget to use the callback to actually emit something!
				callback.onReturn(tuple);
			}

      // This is a bit tricky - the schema may be needed
      // either it is passed by constructor or specified here
      // (this needs to be improved)
			@Override
			public Schema getSchema() {
				return tweetSchema;
			}
		};

		final Pattern mentionsRegex = Pattern.compile("@([\\w]*)");
		final Pattern urlRegex = Pattern.compile("(\\Qhttp://\\E[\\w./-]*)");
		
		// Define the first MapReduce Step (MRStep)
		MRStep step1 = new MRStep("st1", this.getClass());
		// textinput -> textmapper -> tupleOp looks redundant... wondering how this could be refactored
		step1.addInput(mainInput, new TextInput(new TextMapper(tweetParseOp)));
		step1.groupBy(new GroupBy("screen_name"));
		// this is also a mess (the reducer type)
		step1.setReducer(new SingleSchemaReducer() {
			
			Text key, value;
			
			public void setup(TupleMRContext tupleMRContext, Collector collector) throws IOException,InterruptedException,TupleMRException {
				key = new Text(); value = new Text();
			}
			
			public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context,
			    Collector collector) throws IOException, InterruptedException, TupleMRException {

				// key will be user of the tweet
				key.set(group.get("screen_name").toString());

				for(ITuple tuple : tuples) {
					String text = tuple.get("text").toString();
					Matcher matcher = mentionsRegex.matcher(text);
					while(matcher.find()) {
						value.set(matcher.group(1));
						collector.getNamedOutput("mentions").write(key, value);						
					}
					matcher = urlRegex.matcher(text);
					while(matcher.find()) {
						value.set(matcher.group(1));
						collector.getNamedOutput("links").write(key, value);												
					}
					// emit raw tuple to main output
					collector.write(tuple, NullWritable.get());
				}
			};
		});
				
		// The output MUST be declared
		MRInput out = step1.setOutput(new TextOutput()); // maybe the reducer should be specified here?
		// multiple outputs!! declare them
		step1.setOutput("links", new TextOutput());
		step1.setOutput("mentions", new TextOutput());
		
		// Final output MUST be binded (tricky)
		builder.bindOutput(out, "out1");
		// Steps need to be added to the builder - a lot of things not to forget!
		builder.addStep(step1);
		
		BaseFlow flow = builder.getFlow();
		flow.execute(EXECUTION_MODE.OVERWRITE, new Configuration(), "out1");
	}
}
