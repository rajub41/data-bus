package com.inmobi.databus.utils;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LocalStreamDataConsistency {

	private static final Log LOG = LogFactory.getLog(
			LocalStreamDataConsistency.class);
	
	public LocalStreamDataConsistency() {
		
	}
	
	public void checkDataConsistency(List<String> listOfDataTrashFiles, 
		List<String> listOfLocalFiles) {
		Collections.sort(listOfDataTrashFiles);
		Collections.sort(listOfLocalFiles);
		int i, j;
		for (i = 0, j = 0; i< listOfDataTrashFiles.size() && j < 
				listOfLocalFiles.size(); i++, j++  ) {
			LOG.info("file comparision:" + listOfDataTrashFiles.get(i));
			LOG.info("file comparision: "+listOfLocalFiles.get(j));
			if (!listOfDataTrashFiles.get(i).equals(listOfLocalFiles.get(j))) {
				if (listOfDataTrashFiles.get(i).compareTo(listOfLocalFiles.get(j)) < 0) {
					System.out.println("Missing file:" + listOfDataTrashFiles.get(i));
					j--;
				} else {
					System.out.println("Data Repaly:" + listOfLocalFiles.get(j));
					i--;
				}
			} else {
				//System.out.println("match");
			}
		}
		if (i == j && i == listOfDataTrashFiles.size() && j == listOfLocalFiles.size()) {
			System.out.println("There is no inconsistency");
		} else {
			if (i == listOfDataTrashFiles.size()) {
				for (; j < listOfLocalFiles.size(); j++)
				System.out.println("extra files in local stream:" + listOfLocalFiles.get(j));
			} else {
				for (; i < listOfDataTrashFiles.size(); i++ ) {
					System.out.println("Files To be sent:" + listOfDataTrashFiles.get(i));
				}
			}
		}
		
	}
	
	public void doRecursiveListing(Path pathName, List<String> listOfFiles, 
			FileSystem fs, String baseDir) throws Exception {
		FileStatus [] fileStatuses = fs.listStatus(pathName);
		if (fileStatuses == null || fileStatuses.length == 0) {
			LOG.debug("No files in directory:" + pathName);
		} else {
			
			for (FileStatus file : fileStatuses) {
				if (file.isDir()) {
					LOG.info("directory");
					if (baseDir.equals("trash")) {
						System.out.println("trash dirs" + file.getPath() );
					}
					
					doRecursiveListing(file.getPath(), listOfFiles, fs, baseDir);
				} else {
					if (baseDir.equals("data")) {
						
						listOfFiles.add(file.getPath().getParent()+ "-" + file.getPath().
								getName() + ".gz");
						System.out.println(file.getPath().getParent()+ "-" + file.getPath().
								getName() + ".gz");
					} else if (baseDir.equals("trash")){
						listOfFiles.add(file.getPath().getName() + ".gz");
						System.out.println(file.getPath().getName() + ".gz");
					} else {
						listOfFiles.add(file.getPath().getName());
						System.out.println("files are:" + file.getPath().getName());
					}
				}
			}
		}
	}
	
	public void getStreamNames(List<String> streamNames, String pathName) throws 
			Exception{
		for (String streamName : pathName.split(",")) {
			streamNames.add(streamName);
		}
	}
	
	public void getStreamCollectorNames( Path streamDir, List<String>  
			streamCollectorNames) throws Exception {
		FileSystem fs = streamDir.getFileSystem(new Configuration());
		FileStatus [] filestatuses = fs.listStatus(streamDir);
		for (FileStatus file : filestatuses) {
			streamCollectorNames.add(file.getPath().getName());
		}
	}
	
	public FileSystem getFs(Path pathName) throws Exception {
		FileSystem fs = pathName.getFileSystem(new Configuration());
		return fs;
	}

	public void listingAllPaths(String rootDir, String streamName, String 
		collectorName, List<String> listOfDataTrashFiles, List<String> listOfLocalFiles) throws Exception {
		Path pathName;
	//	List<String> listOfDataTrashFiles = new ArrayList<String>();
		//List<String> listOfTrashFiles = new ArrayList<String>();
		//List<String> listOfLocalFiles = new ArrayList<String>();
		FileSystem fs;
		
		// for data
		pathName = new Path(new Path(new Path (rootDir, "data"), streamName), 
				collectorName); 
		fs = getFs(pathName);
		//invoke data listing
		doRecursiveListing(pathName, listOfDataTrashFiles, fs, "data");
		LOG.info("data:");
		//invoke trash
		pathName = new Path(new Path(rootDir, "system"), "trash");
		fs = getFs(pathName);
		doRecursiveListing(pathName, listOfDataTrashFiles, fs, "trash");
		
		//invoke streams_local
		pathName = new Path(new Path(rootDir, "streams_local"), streamName);
		fs = getFs(pathName);
		doRecursiveListing(pathName, listOfLocalFiles, fs, "streams_local");
		
		//invoke checkDataConsistency
		
	}
	
	public void processing(String rootDir, String streamName, List<String> 
			collectorNames, List<String> listOfDataTrashFiles, List<String> listOfLocalFiles) throws Exception {
		for (String collectorName : collectorNames) {
			listingAllPaths(rootDir, streamName, collectorName, listOfDataTrashFiles, listOfLocalFiles);
		}
	}
	
	public void run(String [] args) throws Exception {
		String [] rootDirs = args[0].split(",");
		List<String> streamNames = new ArrayList<String>();
		List<String> collectorNames = new ArrayList<String>();
		List<String> listOfDataTrashFiles = new ArrayList<String>();
		//List<String> listOfTrashFiles = new ArrayList<String>();
		List<String> listOfLocalFiles = new ArrayList<String>();
		
		if (args.length == 1) {
			for (String rootDir : rootDirs) {
				Path baseDir = new Path(rootDir, "streams_local");
				streamNames = new ArrayList<String>();
				getStreamCollectorNames(baseDir, streamNames);
				baseDir = new Path(rootDir, "data");
				for (String streamName : streamNames) {
					collectorNames = new ArrayList<String>();
					getStreamCollectorNames(new Path(baseDir, streamName), collectorNames);
				//	processing();
				}
			}
		} else if (args.length == 2) {
			 getStreamNames(streamNames, args[1]);
			 for (String rootDir : rootDirs) {
				 Path baseDir = new Path(rootDir, "data");
				 for (String streamName : streamNames ) {
					 collectorNames = new ArrayList<String>();
					 getStreamCollectorNames(new Path(baseDir, streamName), collectorNames);
					 //code
					 processing(rootDir, streamName, collectorNames, listOfDataTrashFiles, listOfLocalFiles);
					 //compare local consistency
					 checkDataConsistency(listOfDataTrashFiles, listOfLocalFiles);
				 }
			 }
		} else if (args.length == 3) {
			LOG.info("3 arguments");
			getStreamNames(streamNames, args[1]);
			LOG.info("streamnames" + streamNames.size());
			
			for (String collectorName : args[2].split(",")) {
				collectorNames.add(collectorName);
			}
			for (String rootDir : rootDirs) {
				 for (String streamName : streamNames ) {
						processing(rootDir, streamName, collectorNames, listOfDataTrashFiles, listOfLocalFiles);
						 checkDataConsistency(listOfDataTrashFiles, listOfLocalFiles);
				 }
			}
		
			//code
		}
		
	}
	
	public static void main(String [] args) throws Exception {
		if (args.length >= 1) {
			LocalStreamDataConsistency obj = new LocalStreamDataConsistency();
			obj.run(args);
		} else {
			System.out.println("incorrect number of arguments:" + "Enter 1st arg: " +
					"rootdir url" + "2nd arg: set of stream names" + "3rd arg: set of " +
							"collector names" + "2nd and 3rd arguments are optional here");
		}
	}
}
