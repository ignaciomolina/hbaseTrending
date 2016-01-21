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

import java.util.List;
import java.util.Arrays;

public class Main {

	private void loadData(String dataFolder) {

	}

	private void executeQueryOne(long startTS, long endTS, int n, List<String> lenguages, String outputFolder) {

	}

	private void executeQueryTwo(long startTS, long endTS, int n, List<String> lenguages, String outputFolder) {

	}

	private void executeQueryThree(long startTS, long endTS, String outputFolder) {

	}

	public static void main( String[] args ) {

		Main main = new Main();
		if (args.length == 2 || args.length == 6 || args.length == 5) {
			int mode = Integer.parseInt(args[0]);
			switch(mode) {
			case 0:
				//			mode dataFolder
				main.loadData(args[1]);
				break;
			case 1:
	
				//			mode startTS endTS N language outputFolder
				main.executeQueryOne(Long.parseLong(args[1]),
									 Long.parseLong(args[2]),
									 Integer.parseInt(args[3]),
									 Arrays.asList(args[4].split(",")),
									 args[5]);
				break;
			case 2:
				//			mode startTS endTS N language outputFolder
				main.executeQueryTwo(Long.parseLong(args[1]),
									 Long.parseLong(args[2]),
									 Integer.parseInt(args[3]),
									 Arrays.asList(args[4].split(",")),
									 args[5]);
				break;
			case 3:
	
				//			mode startTS endTS N outputFolder
				main.executeQueryThree(Long.parseLong(args[1]),
									   Long.parseLong(args[2]),
								       args[3]);
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
