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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class TrendingTable {

	private final static String TABLE = "trendingTopics";
	private final static String CF_HASHTAG = "hashtag";
	private final static String CF_METADATA = "metadata";

	private Configuration conf;

	/* Esquema de la tabla
	 * 
	 * trendingTopics
	 * keys	| hashtag 						| metadata
	 * 		| hashtag:name	| hashtag:freq	| metadata:lang	| metadata:ts
	 */
	
	public TrendingTable() {

		conf = HBaseConfiguration.create();
		String [] columnFamilies = {CF_HASHTAG, CF_METADATA};
		createTable(TABLE, columnFamilies);
	}
	
	public void createTable(String table, String [] columnFamilies) {

		try {

			HBaseAdmin admin = new HBaseAdmin(conf);

			// DEBUGGING: crea siempre hasta asegurar que las tablas son correctas
			if (!admin.tableExists(table)) {

				HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(table));
				for (String columnFamily : columnFamilies) {

					HColumnDescriptor family = new HColumnDescriptor(columnFamily);
					tableDescriptor.addFamily(family);
				}
				admin.createTable(tableDescriptor);
			}

			//System.out.println("Table " + table + " created with table families: " + columnFamilies);
			admin.close();

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

		} catch (IOException e) {
			System.out.println("Error when trying to delete table in hbase." + e);
		}
	}

	public void storeData(Collection<Trending> trendings) {

		try {
			
			HConnection conn = HConnectionManager.createConnection(conf);
			HTable table = new HTable(TableName.valueOf(TABLE), conn);

			for (Trending trending : trendings) {

				byte [] key = trending.getKey();

				Put put = new Put(key);
				put.add(Bytes.toBytes(CF_HASHTAG), Bytes.toBytes("name"), Bytes.toBytes(trending.getHashtag()));
				put.add(Bytes.toBytes(CF_HASHTAG), Bytes.toBytes("freq"), Bytes.toBytes(trending.getFrequency()));
				put.add(Bytes.toBytes(CF_METADATA), Bytes.toBytes("lang"), Bytes.toBytes(trending.getLenguage()));
				put.add(Bytes.toBytes(CF_METADATA), Bytes.toBytes("ts"), Bytes.toBytes(trending.getTimestamp()));
				table.put(put);
			}

			table.close();
			conn.close();
			
		} catch (IOException e) {
			System.out.println("Error when trying to close table." + e);
		}
	}

	public String queryOne(long startTS, long endTS, int n, String language) {

		Map<String, Integer> hashtags = new TreeMap<>();
		ValueComparator bvc = new ValueComparator(hashtags);
		TreeMap<String, Integer> sorted = new TreeMap<>(bvc);
		
		try {

			HConnection conn = HConnectionManager.createConnection(conf);
			HTable table = new HTable(TableName.valueOf(TABLE), conn);

			Scan scan = new Scan(Trending.generateKey(language,startTS),
								 Trending.generateKey(language,endTS));

			//Aplicamos el filtro para que solo nos devuelva los hs con el lang adecuado
			Filter f = new SingleColumnValueFilter(Bytes.toBytes(CF_METADATA),
												   Bytes.toBytes("lang"),
												   CompareOp.EQUAL,Bytes.toBytes(language));
			scan.setFilter(f);

			ResultScanner scanner = table.getScanner(scan);
			
			for (Result result = scanner.next(); result != null; result = scanner.next()) {

				byte[] bname = result.getValue(Bytes.toBytes(CF_HASHTAG), Bytes.toBytes("name"));
				byte[] bfreq = result.getValue(Bytes.toBytes(CF_HASHTAG), Bytes.toBytes("freq"));

				Integer freq = hashtags.get(Bytes.toString(bname));
				if (freq == null) {

					hashtags.put(Bytes.toString(bname), Bytes.toInt(bfreq));
				} else {

					hashtags.put(Bytes.toString(bname), freq + Bytes.toInt(bfreq));
				}
			}

			table.close();
			conn.close();
		} catch (IOException e) {

			System.out.println("Error when trying to do query one." + e);
		}

		sorted.putAll(hashtags);
		
		String queryOutput = "";
		Object [] ranking = sorted.keySet().toArray();
		for (int position = 1; position <= n && position < ranking.length; position++) {
			
			queryOutput += language + ", " +
						   position + ", " +
						   ranking[position] + ", " +
						   startTS + ", " +
						   endTS +"\n";
		}
		
		return queryOutput;
	}

	public String queryTwo(long startTS, long endTS, int n, List<String> languages) {

		String queryOutput = "";

		HConnection conn = null;
		HTable table = null;

		try {

			conn = HConnectionManager.createConnection(conf);
			table = new HTable(TableName.valueOf(TABLE), conn);

			for (String lang : languages) {

				Map<String, Integer> hashtags = new TreeMap<>();
				ValueComparator bvc = new ValueComparator(hashtags);
				TreeMap<String, Integer> sorted = new TreeMap<>(bvc);
				
				Scan scan = new Scan(Trending.generateKey(lang, startTS),
									 Trending.generateKey(lang, endTS));
				//Aplicamos el filtro para que solo nos devuelva los hashtag con el lang adecuado
				Filter f = new SingleColumnValueFilter(Bytes.toBytes(CF_METADATA),
													   Bytes.toBytes("lang"),
													   CompareOp.EQUAL,Bytes.toBytes(lang));
				scan.setFilter(f);

				ResultScanner scanner = table.getScanner(scan);
	
				for (Result result = scanner.next(); result != null; result = scanner.next()) {

					byte[] bname = result.getValue(Bytes.toBytes(CF_HASHTAG),Bytes.toBytes("name"));
					byte[] bfreq = result.getValue(Bytes.toBytes(CF_HASHTAG),Bytes.toBytes("freq"));

					Integer freq = hashtags.get(Bytes.toString(bname));
					if (freq == null) {

						hashtags.put(Bytes.toString(bname), Bytes.toInt(bfreq));
					} else {

						hashtags.put(Bytes.toString(bname), freq + Bytes.toInt(bfreq));
					}
				}

				sorted.putAll(hashtags);
				
				Object [] ranking = sorted.keySet().toArray();
				for (int position = 1; position <= n  && position < ranking.length; position++) {
					
					queryOutput += lang + ", " +
								   position + ", " +
								   ranking[position] + ", " +
								   startTS + ", " +
								   endTS +"\n";
				}
			}

			table.close();
			conn.close();
		} catch (IOException e) {

			System.out.println("Error when trying to do query two." + e);
		}

		return queryOutput;
	}

	public String queryThree(long startTS, long endTS, int n) {

		String queryOutput = "";
		Map<String, Integer> hashtags = new TreeMap<>();
		ValueComparator bvc = new ValueComparator(hashtags);
		TreeMap<String, Integer> sorted = new TreeMap<>(bvc);

		try {

			HConnection conn = HConnectionManager.createConnection(conf);
			HTable table = new HTable(TableName.valueOf(TABLE), conn);

			Scan scan = new Scan(Trending.generateStartKey(startTS),Trending.generateEndKey(endTS));

			ResultScanner scanner = table.getScanner(scan);

			for (Result result = scanner.next(); result != null; result = scanner.next()) {

				byte[] bname = result.getValue(Bytes.toBytes(CF_HASHTAG),Bytes.toBytes("name"));
				byte[] bfreq = result.getValue(Bytes.toBytes(CF_HASHTAG),Bytes.toBytes("freq"));

				Integer freq = hashtags.get(Bytes.toString(bname));
				if (freq == null) {

					hashtags.put(Bytes.toString(bname), Bytes.toInt(bfreq));
				} else {

					hashtags.put(Bytes.toString(bname), freq + Bytes.toInt(bfreq));
				}
			}

			table.close();
			conn.close();
		} catch (IOException e) {

			System.out.println("Error when trying to do query one." + e);
		}
		
		sorted.putAll(hashtags);
		
		Object [] ranking = sorted.keySet().toArray();
		for (int position = 1; position <= n && position < ranking.length; position++) {

			queryOutput += position + ", " +
						   ranking[position] + ", " +
						   sorted.get(ranking[position]) + ", " +
						   startTS + ", " +
						   endTS +"\n";
		}

		return queryOutput;
	}
}