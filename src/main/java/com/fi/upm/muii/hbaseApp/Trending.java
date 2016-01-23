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

import org.apache.hadoop.hbase.util.Bytes;

public class Trending {

	private long timestamp;
	private String hashtag;
	private String lenguage;
	private int frequency;
	
	public Trending(long timestamp, String lenguage, String hashtag, int frequency) {
		
		this.timestamp = timestamp;
		this.hashtag = hashtag;
		this.lenguage = lenguage;
		this.frequency = frequency;
	}
	
	public String getId() {
		
		return String.valueOf(timestamp); //POR QUE AQUI EL TS?
	}

	public long getTimestamp() {
		return timestamp;
	}

	public String getHashtag() {
		return hashtag;
	}

	public String getLenguage() {
		return lenguage;
	}

	public int getFrequency() {
		return frequency;
	}
	
	/* Structure of the Key
	 * 48 Bytes  ( 10 lenguage + 30 hashtag + 8 timestamp)
	 */
	
	public static byte[] generateKey(String lang, String hashtag, long timestamp) {
		byte[] key = new byte[48];
		System.arraycopy(Bytes.toBytes(lang),0,key,0,lang.length());
		System.arraycopy(Bytes.toBytes(hashtag),0,key,10,hashtag.length());
		System.arraycopy(Bytes.toBytes(timestamp),0,key,40,hashtag.length());
		return key;
	}
	
	public static byte[] generateStartKey(String lang, long timestamp) {
		
		byte[] key = new byte[10];
		System.arraycopy(Bytes.toBytes(lang),0,key,
		0,lang.length());
		
		for (int i = 10; i < 40; i++){
			key[i] = (byte)-255;
		}
		return key;
	}
	
	public static byte[] generateEndKey(String lang, long timestamp) {
		
		byte[] key = new byte[20];
		System.arraycopy(Bytes.toBytes(lang),0,key,0,lang.length());
		System.arraycopy(Bytes.toBytes(timestamp),0,key,40,lang.length());
		for (int i = 10; i < 40; i++){
			key[i] = (byte)255;
		}
		return key;
	}
}
