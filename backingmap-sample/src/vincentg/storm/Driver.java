package vincentg.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import vincentg.storm.fn.RecordExtractor;
import vincentg.storm.state.SampleBackingMap;

public class Driver {
	
	public static void main(String[] args) {
		try {
			final TridentTopology topology = new TridentTopology();
			
			@SuppressWarnings("unchecked")
			final FixedBatchSpout spout = 
				new FixedBatchSpout(new Fields("rec"), 4, 
					new Values("dog,1"),
					new Values("cat,1"),
					new Values("rat,1"),
					new Values("dog,1"),
					
					new Values("cat,2"),
					new Values("rat,1"),
					new Values("dog,2"),
					new Values("dog,2"),
					
					new Values("rat,3"),
					new Values("rat,3"),
					new Values("rat,3"),
					new Values("rat,3"),
					
					new Values("dog,5"),
					new Values("rat,4"),
					new Values("dog,5"),
					new Values("rat,4"),
					
					new Values("sheep,5"),
					new Values("turtle,4"),
					new Values("pig,5"),
					new Values("goat,4")
					);
			
//			spout.setCycle(true);
			
			Stream source = topology.newStream("sample-stream", spout);

			source
				.each( new Fields("rec"), new RecordExtractor(), new Fields("id","val") )
				.groupBy(new Fields("id"))
				.persistentAggregate(
						SampleBackingMap.FACTORY,
						new Fields("val"),
						new Sum(), 
						new Fields("agg_count"));
			
			
			final Config conf = new Config();
			conf.setNumWorkers(1);
			conf.setMaxSpoutPending(1);
			
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("sample-topology", conf, topology.build());
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
