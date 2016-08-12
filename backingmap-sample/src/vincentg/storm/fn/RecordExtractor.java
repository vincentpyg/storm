package vincentg.storm.fn;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class RecordExtractor extends BaseFunction{

	private static final long serialVersionUID = 7726571660259461479L;

	@Override
	public void execute(TridentTuple t, TridentCollector collector) {
		Values v = new Values();
		
		String rec = (String) t.getValueByField("rec");
		if (rec != null) {
			String[] rec_p = rec.split(",");
			v.add( rec_p[0] );
			v.add( Integer.parseInt(rec_p[1]) );
			collector.emit(v);
		}
		
		
		
	}

}
