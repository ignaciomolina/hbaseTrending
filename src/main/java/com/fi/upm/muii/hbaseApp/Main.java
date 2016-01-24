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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

	private TrendingTable trendingTable;

	public Main () {

		this.trendingTable = new TrendingTable();
	}

	private Collection<Trending> readFile(String file) throws IOException {

		Collection<Trending> trendings = new ArrayList<>();

		byte[] data = Files.readAllBytes(Paths.get(file));

		String content = new String(data, Charset.forName("UTF-8"));

		String regEx = "(\\d+),(.*),(.+),([\\d+]),(.+),([\\d+]),(.+),([\\d+])";

		Pattern pattern = Pattern.compile(regEx);
		Matcher matcher = pattern.matcher(content);

		while (matcher.find()) {

			long timestamp = Long.parseLong(matcher.group(1)); // timestamp
			String lenguage = matcher.group(2); // lenguage
			trendings.add(new Trending(timestamp, lenguage, matcher.group(3), Integer.parseInt(matcher.group(4))));
			trendings.add(new Trending(timestamp, lenguage, matcher.group(5), Integer.parseInt(matcher.group(6))));
			trendings.add(new Trending(timestamp, lenguage, matcher.group(7), Integer.parseInt(matcher.group(8))));
		}

		return trendings;
	}

	private void loadData(String dataFolder) throws IOException {

		Collection<Trending> trendings = new ArrayList<>();

		File dir = new File(dataFolder);

		if (dir.isDirectory()) {

			// Look up for files
			File[] files = dir.listFiles(new FilenameFilter() {
				public boolean accept(File dir, String name)
				{
					return name.endsWith(".out");
				}
			});

			// Reading trendings
			for (File file : files) {

				trendings.addAll(readFile(file.getAbsolutePath()));
			}

			// Storing trendings
			trendingTable.storeData(trendings);
			System.out.println("Hecho, " + trendings.size() + " trending topic almacenados con exito.");
		}
	}

	private void writeOutputFolder(String result, String outputFolder) {

		Path file = Paths.get(outputFolder);

		try {

			if (!Files.exists(file)) {

				Files.createDirectories(file.getParent().toAbsolutePath());
				Files.createFile(file);
			}

			Files.write(file, result.getBytes(), StandardOpenOption.APPEND);
		} catch (SecurityException e) {
			
			System.out.println("Error to write by lack of authority for output folder.");
		} catch (IOException e) {

			System.out.println("Write to file failed. " + e);
		}
	}

	private void executeQueryOne(long startTS, long endTS, int n, String language, String outputFolder) {

		String result = this.trendingTable.queryOne(startTS, endTS, n, language);
		writeOutputFolder(result, outputFolder + "14_query1.out");
	}

	private void executeQueryTwo(long startTS, long endTS, int n, List<String> languages, String outputFolder) {

		String result = this.trendingTable.queryTwo(startTS, endTS, n, languages);
		writeOutputFolder(result, outputFolder + "14_query2.out");
	}

	private void executeQueryThree(long startTS, long endTS, int n, String outputFolder) {

		String result = this.trendingTable.queryThree(startTS, endTS, n);
		writeOutputFolder(result, outputFolder + "14_query3.out");
	}

	public static void main( String[] args ) {

		if (args.length == 2 || args.length == 6 || args.length == 5) {

			Main main = new Main();
			int mode = Integer.parseInt(args[0]);

			switch(mode) {
			case 1:
				// mode startTS endTS N language outputFolder
				main.executeQueryOne(Long.parseLong(args[1]),
									 Long.parseLong(args[2]),
									 Integer.parseInt(args[3]),
									 args[4],
									 args[5]);
				break;
			case 2:
				// mode startTS endTS N languages outputFolder
				main.executeQueryTwo(Long.parseLong(args[1]),
									 Long.parseLong(args[2]),
									 Integer.parseInt(args[3]),
									 Arrays.asList(args[4].split(",")),
									 args[5]);
				break;
			case 3:

				// mode startTS endTS N outputFolder
				main.executeQueryThree(Long.parseLong(args[1]),
									   Long.parseLong(args[2]),
									   Integer.parseInt(args[3]),
									   args[4]);
				break;
			case 4:
				// mode dataFolder
				try {
					main.loadData(args[1]);
				} catch (IOException e) {

					System.out.println("Error when processing files: " + e);
				}
				break;
			default:
				appHelp();
			}

		} else {

			appHelp();
		}
	}

	private static void appHelp() {

		System.out.println("startTwitterApp usage:\n" +
				"	- mode:\n" +
				"		1: run first query\n" +
				"		2: run second query\n" +
				"		3: run third query\n" +
				"		4: load data files\n" +
				"	- startTS: timestamp in milliseconds to be used as start timestamp.\n" +
				"	- endTS: timestamp in milliseconds to be used as end timestamp.\n" +
				"	- N: size of the ranking for the top-N.\n" +
				"	- lenguaje: one language or a cvs list of languages.\n" +
				"	- dataFolder: path to the folder containing the data sources.\n" +
				"	- outputFolder: path to the folder where to store the query results.");
		System.exit(0);
	}
}
