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
	
	/* Todos los elementos se distinguen como minimo lenguaje y su timestamp.
	 * Dicho esto, configuramos la clave:
	 * 
	 * | timestamp 	| lang 		|
	 * | 8Bytes		| 2Bytes	|
	 * 
	 */
	
	public byte[] getKey() {
		
		return generateKey(lenguage, timestamp, hashtag);
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
	
	/* Structure of the Key: 8 para el long del ts y 2 para los carácteres del lenguaje.
	 * Se debe poner primero el ts ya que filtraremos lang para la query 3.
	 * 10 Bytes  (8 timestamp + 2 lang)
	 */
	
	public static byte[] generateKey(String lang, long timestamp, String hashtag) {
		
		byte[] key = new byte[12];
		System.arraycopy(Bytes.toBytes(timestamp),0,key,0,8);
		System.arraycopy(Bytes.toBytes(lang),0,key,8,2);
		System.arraycopy(Bytes.toBytes(hashtag),0,key,10,2);
		return key;
	}
	
	public static byte[] generateStartKey(long timestamp) {
		
		byte[] key = new byte[10];
		System.arraycopy(Bytes.toBytes(timestamp),0,key,0,8);
		
		for (int i = 8; i < 10; i++){
			key[i] = (byte)-255;
		}
		return key;
	}
	
	public static byte[] generateEndKey(long timestamp) {
		
		byte[] key = new byte[10];
		System.arraycopy(Bytes.toBytes(timestamp),0,key,0,8);

		for (int i = 8; i < 10; i++){
			key[i] = (byte)255;
		}
		return key;
	}
}
