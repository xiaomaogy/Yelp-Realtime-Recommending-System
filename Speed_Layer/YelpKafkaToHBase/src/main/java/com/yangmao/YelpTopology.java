package com.yangmao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.storm.hdfs.bolt.HdfsBolt;
//import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
//import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
//import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
//import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
//import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class YelpTopology {
	
	static class SplitFieldsBolt extends BaseBasicBolt {

		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			super.prepare(stormConf, context);
		}

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String review_raw = tuple.getString(0);
			String[] review_list = review_raw.split("\u0001");
			
			if(review_list.length == 10){
				collector.emit(new Values(
						review_list[0], //review_id
						review_list[1], //business_id
						review_list[2], //user_id
						Integer.parseInt(review_list[3]), //review_stars
						review_list[4], //review_date
						review_list[5], //review_text
						review_list[6], //type
						Integer.parseInt(review_list[7]), //votes_funny
						Integer.parseInt(review_list[8]), //votes_useful
						Integer.parseInt(review_list[9]))); //votes_cool
			}
			
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("review_id", 
					"business_id",
					"user_id",
					"review_stars", 
					"review_date", 
					"review_text", 
					"type",
					"votes_funny",
					"votes_useful", 
					"votes_cool"));
		}

	}

	static class UpdateReviewToHBaseBolt extends BaseBasicBolt {
		private org.apache.hadoop.conf.Configuration conf;
		private HConnection hConnection;
		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			try {
				conf = HBaseConfiguration.create();
				conf.set("zookeeper.znode.parent", "/hbase-unsecure");
				hConnection = HConnectionManager.createConnection(conf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			super.prepare(stormConf, context);
		}

		@Override
		public void cleanup() {
			try {
				hConnection.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// TODO Auto-generated method stub
			super.cleanup();
		}

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			try {
				HTableInterface table = hConnection.getTable("yuan_yelp_review_changing");
				
				Get get_entry_exists = new Get(Bytes.toBytes(input.getStringByField("review_id")));
				
				HTableInterface business_table = hConnection.getTable("yuan_yelp_business_wrc_hbase_sync_2");
				Increment inc = new Increment(Bytes.toBytes(input.getStringByField("business_id")));
				inc.addColumn(Bytes.toBytes("business"), Bytes.toBytes("review_count_new"), 1L);
				try{
					business_table.increment(inc);
				}catch (IOException e){
					e.printStackTrace();
				}
				System.out.println("Incremented here");
				
				business_table.close();
				
				if(!table.exists(get_entry_exists)){
					Put put = new Put(Bytes.toBytes(input.getStringByField("review_id")));
					put.add(Bytes.toBytes("review"), Bytes.toBytes("business_id"), Bytes.toBytes(input.getStringByField("business_id")));
					put.add(Bytes.toBytes("review"), Bytes.toBytes("user_id"), Bytes.toBytes(input.getStringByField("user_id")));
					put.add(Bytes.toBytes("review"), Bytes.toBytes("review_stars"), Bytes.toBytes(input.getIntegerByField("review_stars")));
					put.add(Bytes.toBytes("review"), Bytes.toBytes("review_date"), Bytes.toBytes(input.getStringByField("review_date")));
					put.add(Bytes.toBytes("review"), Bytes.toBytes("review_text"), Bytes.toBytes(input.getStringByField("review_text")));
					put.add(Bytes.toBytes("review"), Bytes.toBytes("type"), Bytes.toBytes(input.getStringByField("type")));
					put.add(Bytes.toBytes("review"), Bytes.toBytes("votes_funny"), Bytes.toBytes(input.getIntegerByField("votes_funny")));
					put.add(Bytes.toBytes("review"), Bytes.toBytes("votes_useful"), Bytes.toBytes(input.getIntegerByField("votes_useful")));
					put.add(Bytes.toBytes("review"), Bytes.toBytes("votes_cool"), Bytes.toBytes(input.getIntegerByField("votes_cool")));
					
					table.put(put);
					table.close();
					
					
					
					
					
				}else{
					System.out.println("This row already exists");
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub

		}

	}

	
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

		String zkIp1 = "hadoop-w-1.c.mpcs53013-2015.internal";
		String zkIp2 = "hadoop-w-0.c.mpcs53013-2015.internal";
		String zkIp3 = "hadoop-m.c.mpcs53013-2015.internal";
		List<String> zkServers = new ArrayList<String>();
		zkServers.add(zkIp1);
		zkServers.add(zkIp2);
		zkServers.add(zkIp3);
		
		String nimbusHost = "hadoop-m.c.mpcs53013-2015.internal";

		String zookeeperHost = zkIp1 +":2181";

		ZkHosts zkHosts = new ZkHosts(zookeeperHost);
		
		
		SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "yuan_yelp_reviews", "/yuan_yelp_reviews","test_id");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
		kafkaConfig.zkServers = zkServers;
		kafkaConfig.zkRoot = "/yuan_yelp_reviews";
		kafkaConfig.zkPort = 2181;
		kafkaConfig.forceFromStart = true;
		KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);

		TopologyBuilder builder = new TopologyBuilder();

//		HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl("hadoop-m.c.mpcs53013-2015.internal:8020")
//				.withFileNameFormat(new DefaultFileNameFormat().withPath("/tmp/test"))
//				.withRecordFormat(new DelimitedRecordFormat().withFieldDelimiter("|"))
//				.withSyncPolicy(new CountSyncPolicy(10))
//				.withRotationPolicy(new FileSizeRotationPolicy(5.0f, Units.MB));
		builder.setSpout("raw_yelp_reviews_spout", kafkaSpout, 1);
		builder.setBolt("split_fields_bolt", new SplitFieldsBolt(), 1).shuffleGrouping("raw_yelp_reviews_spout");
		builder.setBolt("update_review_to_hbase_bolt", new UpdateReviewToHBaseBolt(), 1).fieldsGrouping("split_fields_bolt", new Fields("review_id"));


		Map conf = new HashMap();
		conf.put(backtype.storm.Config.TOPOLOGY_WORKERS, 4);
		conf.put(backtype.storm.Config.TOPOLOGY_DEBUG, true);
		if (args != null && args.length > 0) {
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}   else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("yelp-review-topology", conf, builder.createTopology());
		}
	}
}
