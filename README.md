Hbase Application
 Description: The goal of this assignment is to implement a Java application that stores trending topics from Twitter into HBase and provides users with a set of queries for data analysis. The trending topics to load in HBase are stored into text files with the same format used to store the results of the project 1 assignment. This format was: 1 file per language and each line of the file with the CSV format “timestamp_ms, lang, tophashtag1, frequencyhashtag1, tophashtag2, frequencyhashtag2, tophashtag3, frequencyhashtag3”. The query set is composed by 3 queries:
    1. Given a language (lang), do find the Top-N most used words for the given language in a time interval defined with a start and end timestamp. Start and end timestamp are in milliseconds.
    2. Do find the list of Top-N most used words for each language in a time interval defined with the provided start and end timestamp. Start and end timestamp are in milliseconds.
    3. Do find the Top-N most used words and the frequency of each word regardless the language in a time interval defined with the provided start and end timestamp. Start and end timestamp are in milliseconds.
    
 Deadline: 24th January 2016
 Where: All the required files must be uploaded to Moodle by the deadline. The file name must
be ID.rar (ID is the id of the students) and has the following structure:
  o ID.rar
      hbaseApp/
        appassembler/
        src/
  o etc/
      hbase-site.xml

where appassembler is the folder generated with the appassembler Maven plugin, that contains among the other files the hbaseApp.sh script. The appassembler etc folder must contain the hbase-site.xml with the property hbase.zookeeper.quorum properly configured (it must point to ZK instance of the mini cluster assigned to your group) . src is a folder containing all the source code developed for the project.

o The application must be developed using the versions of the software (Oracle Java 7)
installed in the computers available for the students (CESVIMA) and deployed using
Ubuntu 14.04.
o HBase cluster must have at least 2 Region Servers (each one running in a different VM).

To be Released:
  o Script to use the application
  Script name: hbaseApp.sh
  Script parameters:
    o Mode, integer whose value can be: o 1: run first query
    o 2: run second query o 3: run third query
    o 4: load data files
    o startTS: timestamp in milliseconds to be used as start timestamp. o endTS: timestamp in milliseconds to be used as end timestamp. o N: size of the ranking for the top-N.
    o Languages: one language or a cvs list of languages.
    o dataFolder: path to the folder containing the files with the trending topics (the path is related to the filesystem of the node that will be used to run the HBase app). File names are lang.out, for example en.out, it.out, es.out...
    o outputFolder: path to the folder where to store the files with the query results.
   According with the mode, the script will be used with the following parameters:
   Load: ./hbaseApp.sh mode dataFolder o Ex:./hbaseApp.sh 4 /local/data
   Query1: ./hbaseApp.sh mode startTS endTS N language outputFolder
    o Ex:./hbaseApp.sh 1 1450714465000 1450724465000 7 en
  /local/output/
   Query2: ./hbaseApp.sh mode startTS endTS N language outputFolder
  o Ex:./hbaseApp.sh 2 1450714465000 1450724465000 5 en,it,es /local/output/
   Query3: ./hbaseApp.sh mode startTS endTS N outputFolder
       
  o Ex:./hbaseApp.sh 3 1450714465000 1450724465000 10 /local/output/
  
o Output with results:
 One output file for each query to be stored in the folder specified with the outputFolder input parameter.

 Filenames must be: ID_query1.out, ID_query2.out, ID_query3.out
 File format (startTS end endTS are the once used as input parameters):
 language, position, word, startTS, endTS
 In case of words with the same frequency the ranking is done according with the alphabetic order.
 Multiple executions of the same query must use the same file to store the results without overwriting previous results.

  o The script hbaseApp.sh must be tested on the Ubuntu nodes available for students and must be created using the appassembler maven plugin.
 
