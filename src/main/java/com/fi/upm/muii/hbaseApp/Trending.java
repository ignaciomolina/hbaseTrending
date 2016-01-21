package com.fi.upm.muii.hbaseApp;

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
		
		return lenguage + hashtag + timestamp;
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
}
