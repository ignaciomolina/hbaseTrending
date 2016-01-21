package com.fi.upm.muii.hbaseApp;

import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;

public class TrendingTable {

	private final static String TABLE = "trendingTopics";
	private final static String COLUMN_FAMILY = "BasicData";
	private HTable table;

	public TrendingTable() {

		Configuration conf = HBaseConfiguration.create();
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);

			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE));
			HColumnDescriptor family = new HColumnDescriptor(COLUMN_FAMILY);
			family.setMaxVersions(10); // Default is 3.
			tableDescriptor.addFamily(family);
			admin.createTable(tableDescriptor);
			admin.close();

			HConnection conn = HConnectionManager.createConnection(conf);
			table = new HTable(TableName.valueOf(TABLE), conn);

		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void storeData(Trending trending) {

		byte [] columnFamily = Bytes.toBytes(COLUMN_FAMILY);
		byte [] key = Bytes.toBytes(trending.getId());
		Put put = new Put(key);
		put.add(columnFamily, Bytes.toBytes("lenguage"), Bytes.toBytes(trending.getLenguage()));
		put.add(columnFamily, Bytes.toBytes("timestamp"), Bytes.toBytes(trending.getHashtag()));
		put.add(columnFamily, Bytes.toBytes("hashtag"), Bytes.toBytes(trending.getFrequency()));
		put.add(columnFamily, Bytes.toBytes("frequency"), Bytes.toBytes(trending.getTimestamp()));
		
		try {
			table.put(put);
		} catch (RetriesExhaustedWithDetailsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedIOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


}
