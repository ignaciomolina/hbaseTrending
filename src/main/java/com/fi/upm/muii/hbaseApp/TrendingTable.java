/*
 * Task for:
 * 		Cloud Computing and Big Data Ecosystems Design
 * 
 * Team: 14
 * Members:
 * 		Ignacio Molina Cuquerella
 * 		Jose María Ramiréz Barambones
 */
package com.fi.upm.muii.hbaseApp;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class TrendingTable {

	private final static String TABLE = "trendingTopics";
	private final static String CF_FREQUENCY = "frequncies";
	private final static String CF_LENGUAGE = "lenguage";
	private final static String CF_TIMESTAMP = "timestamp";

	private Configuration conf;

	public TrendingTable() {

		conf = HBaseConfiguration.create();
		String [] columnFamilies = {CF_TIMESTAMP, CF_LENGUAGE, CF_FREQUENCY};
		createTable(TABLE, columnFamilies);
	}

	public void createTable(String table, String [] columnFamilies) {

		try {

			HBaseAdmin admin = new HBaseAdmin(conf);

			if (!admin.tableExists(TABLE)) {

				HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE));
				for (String columnFamily : columnFamilies) {

					HColumnDescriptor family = new HColumnDescriptor(columnFamily);
					tableDescriptor.addFamily(family);
				}
				admin.createTable(tableDescriptor);
			}

			admin.close();

		} catch (MasterNotRunningException e) {

			System.out.println("Error when trying to create table in hbase." + e);
		} catch (ZooKeeperConnectionException e) {

			System.out.println("Error when trying to create table in hbase." + e);
		} catch (IOException e) {

			System.out.println("Error when trying to create table in hbase." + e);
		}
	}

	public void deleteTable(String tableName) {

		try {

			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			admin.close();

		} catch (MasterNotRunningException e) {

			System.out.println("Error when trying to delete table in hbase." + e);
		} catch (ZooKeeperConnectionException e) {

			System.out.println("Error when trying to delete table in hbase." + e);
		} catch (IOException e) {

			System.out.println("Error when trying to delete table in hbase." + e);
		}
	}

	public void storeData(Collection<Trending> trendings) {

		HConnection conn = null;
		HTable table = null;
		try {
			conn = HConnectionManager.createConnection(conf);
			table = new HTable(TableName.valueOf(TABLE), conn);

		} catch (IOException e) {

			System.out.println("Error when trying connect to hbase." + e);
		}
		try {

			for (Trending trending : trendings) {

				byte [] key = Bytes.toBytes(trending.getId());

				Put put = new Put(key);
				put.add(Bytes.toBytes(CF_LENGUAGE), Bytes.toBytes("lenguage"), Bytes.toBytes(trending.getLenguage()));
				put.add(Bytes.toBytes(CF_TIMESTAMP), Bytes.toBytes("timestamp"), Bytes.toBytes(trending.getHashtag()));
				put.add(Bytes.toBytes(CF_FREQUENCY), Bytes.toBytes("hashtag"), Bytes.toBytes(trending.getFrequency()));
				put.add(Bytes.toBytes(CF_FREQUENCY), Bytes.toBytes("frequency"), Bytes.toBytes(trending.getTimestamp()));
				table.put(put);
			}

			table.close();
			conn.close();
		} catch (RetriesExhaustedWithDetailsException e) {

			System.out.println("Error when trying to load data." + e);
		} catch (InterruptedIOException e) {

			System.out.println("Error when trying to load data." + e);
		} catch (IOException e) {

			System.out.println("Error when trying to close table." + e);
		}
	}

	public String queryOne(long startTS, long endTS, int n, String language) {

		String result = "";

		HConnection conn = null;
		HTable table = null;
		try {
			conn = HConnectionManager.createConnection(conf);
			System.out.println("accedemos a table.");
			table = new HTable(TableName.valueOf(TABLE), conn);

		} catch (IOException e) {

			System.out.println("Error when trying connect to hbase." + e);
		}

//		Scan scan = new Scan(Bytes.toBytes(startTS),
//							 Bytes.toBytes(endTS));
		Scan scan = new Scan();

		//		Scan scan = new Scan(Trending.generateStartKey(language, startTS),
		//				Trending.generateEndKey(language, endTS));

		/*Filter f = new
				SingleColumnValueFilter(Bytes.toBytes(COLUMN_FAMILY),
				Bytes.toBytes("lenguage"),
				CompareFilter.CompareOp.EQUAL,Bytes.toBytes(language));

		scan.setFilter(f);*/

		try {

			System.out.println("Scaneando...");
			ResultScanner rs = table.getScanner(scan);
			//			Result res = rs.next();

			System.out.println("Resultados: ");
			for (Result res : rs) {
				
				System.out.println("item size: " + res.size() + ", " + new String(res.getRow()));
				for(KeyValue kv : res.raw()){
					//				CellScanner scanner = res.cellScanner();
					//
					//				while (scanner.advance()) {
					//
					//					Cell cell = scanner.current();
					//					byte[] value = CellUtil.cloneValue(cell);
					//					//TODO instanciar la salida en una estructura de datos y sacar el top-N
					//					result += Bytes.toLong(value) + "\n"; // PROVISIONAL
					//				}
					Map<String, Object> values = kv.toStringMap();
					String key = kv.getKeyString();
					System.out.println("Key: " + key + ", Values: " + values);

				}
				//				res = rs.next();
			}
			System.out.println("¡Eso es todo!");

			table.close();
			conn.close();
		} catch (IOException e) {

			System.out.println("Error when trying to do query one." + e);
		}

		return result;
	}

	public String queryTwo(long startTS, long endTS, int n, List<String> languages) {

		String result = "";

		HConnection conn = null;
		HTable table = null;
		try {
			conn = HConnectionManager.createConnection(conf);
			System.out.println("accedemos a table.");
			table = new HTable(TableName.valueOf(TABLE), conn);

		} catch (IOException e) {

			System.out.println("Error when trying connect to hbase." + e);
		}

		for (String lang : languages) {

			Scan scan = new Scan(Trending.generateStartKey(lang, startTS),
					Trending.generateEndKey(lang, endTS));

			try {

				ResultScanner rs = table.getScanner(scan);
				Result res = rs.next();

				while (res != null && !res.isEmpty()) {

					CellScanner scanner = res.cellScanner();

					while (scanner.advance()) {

						Cell cell = scanner.current();
						byte[] value = CellUtil.cloneValue(cell);
						//TODO instanciar la salida en una estructura de datos y sacar el top-N
						result += Bytes.toLong(value) + "\n"; // PROVISIONAL
					}

					res = rs.next();
				}
				table.close();
				conn.close();
			} catch (IOException e) {

				System.out.println("Error when trying to do query two." + e);
			}

		}

		return result;
	}

	public String queryThree(long startTS, long endTS) {

		String result = "";

		HConnection conn = null;
		HTable table = null;
		try {
			conn = HConnectionManager.createConnection(conf);
			System.out.println("accedemos a table.");
			table = new HTable(TableName.valueOf(TABLE), conn);

		} catch (IOException e) {

			System.out.println("Error when trying connect to hbase." + e);
		}

		try {

			Scan scan = new Scan();
			ResultScanner rs;
			rs = table.getScanner(scan);
			Result res = rs.next();

			while (res != null && !res.isEmpty()) {

				CellScanner scanner = res.cellScanner();

				while (scanner.advance()) {

					Cell cell = scanner.current();
					byte[] value = CellUtil.cloneValue(cell);
					//TODO instanciar la salida en una estructura de datos y sacar el top-N
					result += Bytes.toLong(value) + "\n"; // PROVISIONAL
				}

				res = rs.next();
			}
		} catch (IOException e) {

			System.out.println("Error when trying to do query three." + e);
		}

		return result;
	}
}
