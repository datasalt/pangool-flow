package com.datasalt.pangool.flow.mapred;

import java.io.IOException;

import com.datasalt.pangool.flow.Pair;
import com.datasalt.pangool.flow.ops.Op;
import com.datasalt.pangool.flow.ops.ReturnCallback;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleReducer;

@SuppressWarnings("serial")
public class TuplePairOpReducer<K, V> extends TupleReducer<K, V> {
	
	private Op<Iterable<ITuple>, Pair<K, V>> op;
	private Collector collector;

	public TuplePairOpReducer(Op<Iterable<ITuple>, Pair<K, V>> op) {
		this.op = op;
	}

	public void setup(TupleMRContext tupleMRContext, Collector collector) throws IOException,
	    InterruptedException, TupleMRException {
		this.collector = collector;
	};

	ReturnCallback<Pair<K, V>> callback = new ReturnCallback<Pair<K, V>>() {

    @Override
		public void onReturn(Pair<K, V> element) {
			if(element != null) {
				try {
					collector.write(element.getFirst(), element.getSecond());
				} catch(IOException e) {
					throw new RuntimeException(e);
				} catch(InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}
	};

	@Override
	public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
	    throws IOException, InterruptedException, TupleMRException {
		op.process(tuples, callback);
	}
}