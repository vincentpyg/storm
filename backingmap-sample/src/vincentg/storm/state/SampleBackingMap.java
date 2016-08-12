package vincentg.storm.state;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.OpaqueValue;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.tuple.TridentTuple;

@SuppressWarnings("rawtypes")
public class SampleBackingMap implements IBackingMap<OpaqueValue> {
	
	private Connection conn;
	
	//initialize connection to DB
	public SampleBackingMap() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(
					"jdbc:mysql://192.168.187.10:3306/sample_db?"
							+"user=aoc-user"
							+"&password=password"
							+"&autoReconnect=true"
							+"&useDynamicCharsetInfo=false");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public List<OpaqueValue> multiGet(List<List<Object>> keys) {
		
		List<OpaqueValue> ovl = new ArrayList<>();
		
		System.out.println("keys.size = "+keys.size());
		
		if ( keys.isEmpty() ) {
			System.out.println("retrieving keys from caches");
		} else {
			
			System.out.println("retrieving keys from DB");
			
			PreparedStatement ps = makeQuery(keys.size());
			
			List<String> ids = new ArrayList<>();
			Map <String, List<Object>> kvMap = new HashMap<>();
			
			try {
				
				//batch: get all ids
				int cntr1=0;
				for (List<Object> key : keys) {
//						ids.add(key.get(0).toString());
						ids.add( ((TridentTuple)key).getValueByField("id").toString() ); //trident tuple!
						ps.setObject(++cntr1, key.get(0));
				}
				
				ResultSet results = ps.executeQuery();
	
				//retrieve values
				while ( results.next() ) {
					List<Object> vals = new ArrayList<>();
					vals.add(results.getLong("txid"));
					vals.add(results.getInt("prev"));
					vals.add(results.getInt("val"));
					kvMap.put(results.getString("id"), vals);
				}
				
				//build opaque values
				for (String id : ids) {
					OpaqueValue ov;
					if (kvMap.get(id) == null) {
						ov=new OpaqueValue<>(0L, null, null);
					} else {
						ov=new OpaqueValue<>(
								(long)kvMap.get(id).get(0),
								(int)kvMap.get(id).get(1),
								(int)kvMap.get(id).get(2));
					}
					ovl.add(ov);
				}
				ps.close();
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return ovl;
	}

	@Override
	public void multiPut(List<List<Object>> keys, List<OpaqueValue> values) {
		
		List<String> ids = new ArrayList<>();
		
		PreparedStatement ps = makeUpdate(keys.size());
		
		try { 
			int cntr = 0;
			int vcntr = 0;
			for(List<Object> key : keys) {
				ids.add( key.get(0).toString() );
				ps.setObject( ++cntr, key.get(0).toString() );
				ps.setObject( ++cntr, values.get(vcntr).getCurrTxid() );
				ps.setObject( ++cntr, values.get(vcntr).getPrev() );
				ps.setObject( ++cntr, values.get(vcntr).getCurr() );
				
				System.out.println("[U] I: "
						+"id="+key.get(0).toString()
						+", txid="+values.get(vcntr).getCurrTxid() 
						+", prev="+values.get(vcntr).getPrev()
						+", val="+values.get(vcntr).getCurr());
				vcntr++;
			}
			
			ps.execute();
			ps.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	private PreparedStatement makeUpdate(int sz) {
		PreparedStatement ps = null;
		
		String values = "";
		
		for (int x=0; x<sz; x++) {
			values+="(?,?,?,?)";
			if (x < sz-1)
				values+=", ";
		}
		
		
		String update = "INSERT INTO sample_table (id,txid,prev,val) "
				+"VALUES "+values
				+" ON DUPLICATE KEY UPDATE "
				+"txid=VALUES(txid),prev=VALUES(prev),val=VALUES(val)";
		
		try {
			ps = conn.prepareStatement(update);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("[U]: "+update);
		
		
		return ps;
	}
	
	private PreparedStatement makeQuery(int sz) {
		PreparedStatement ps = null;
		
		
		String query = "SELECT id,txid,prev,val "
				+ "FROM sample_table "
				+ "WHERE"+formatValues(sz);
		try {
			ps = conn.prepareStatement(query);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("[Q]: "+query);
		return ps;
	}
	
	
	private String formatValues(int sz) {
		
		String[] vcols = new String[]{"id"};
		String result = "";
		
		for (int x=0; x<sz; x++) {
			result += "(";
				for (int y=0; y< vcols.length; y++) {
					result += vcols[0]+"=?";
					if (y < vcols.length -1)
						result += " AND ";
				}
			result += ")";
			if (x < sz-1)
				result +=" OR ";
		}
		
		return result;
	}
	

	public static StateFactory FACTORY = new StateFactory() {

		private static final long serialVersionUID = -2817037860853813963L;
		
		@SuppressWarnings({"unchecked"})
		public State makeState(
				Map conf, 
				IMetricsContext metrics, 
				int partitionIndex, 
				int numPartitions) {

			final CachedMap map = new CachedMap(new SampleBackingMap(), 5000);
			return OpaqueMap.build(map);
		}
	};
	
}
