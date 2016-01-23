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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrendingTable {

	private static final Logger logger = LoggerFactory.getLogger(TrendingTable.class);

	private final static String TABLE = "trendingTopics";
	private final static String COLUMN_FAMILY = "frequncies";
	private HTable table;

	public TrendingTable() {

		Configuration conf = HBaseConfiguration.create();

		try {

			HBaseAdmin admin = new HBaseAdmin(conf);

			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE));
			HColumnDescriptor family = new HColumnDescriptor(COLUMN_FAMILY);
			tableDescriptor.addFamily(family);
			admin.createTable(tableDescriptor);
			admin.close();

			HConnection conn = HConnectionManager.createConnection(conf);
			table = new HTable(TableName.valueOf(TABLE), conn);

		} catch (MasterNotRunningException e) {

			logger.error("Error when trying to connect to hbase.", e);
		} catch (ZooKeeperConnectionException e) {

			logger.error("Error when trying to connect to hbase.", e);
		} catch (IOException e) {

			logger.error("Error when trying to connect to hbase.", e);
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

			logger.error("Error when trying to load data.", e);
		} catch (InterruptedIOException e) {

			logger.error("Error when trying to load data.", e);
		}
	}

	public String queryOne(long startTS, long endTS, int n, String language) {

		String result = "";

		Scan scan = new Scan(Trending.generateStartKey(language, startTS),
							 Trending.generateEndKey(language, endTS));

		/*Filter f = new
				SingleColumnValueFilter(Bytes.toBytes(COLUMN_FAMILY),
				Bytes.toBytes("lenguage"),
				CompareFilter.CompareOp.EQUAL,Bytes.toBytes(language));

		scan.setFilter(f);*/

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

		} catch (IOException e) {

			logger.error("Error when trying to do query one.", e);
		}

		return result;
	}

	public String queryTwo(long startTS, long endTS, int n, List<String> languages) {

		String result = "";

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
			} catch (IOException e) {

				logger.error("Error when trying to do query two.", e);
			}

		}

		return result;
	}

	public String queryThree(long startTS, long endTS) {

		String result = "";

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

			logger.error("Error when trying to do query three.", e);
		}

		return result;
	}
}
