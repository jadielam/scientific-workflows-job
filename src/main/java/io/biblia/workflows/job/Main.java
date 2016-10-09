package io.biblia.workflows.job;

import java.net.URL;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;


import com.google.common.base.Preconditions;

public class Main {

	private static FileSystem fs;
	
	private static String NAMENODE_URL = "hdfs://localhost:8020";
	
	private static String garbageToWrite;
	
	private static int noBytesGarbageToWrite;
	
	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		try {
			Configuration conf = new Configuration();
			conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
			conf.set("fs.defaultFS", NAMENODE_URL);
			fs = FileSystem.get(new URI(NAMENODE_URL), conf);
		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
		
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 1000000; i++) {
			sb.append("A");
		}
		garbageToWrite = sb.toString();
		try{
			noBytesGarbageToWrite = garbageToWrite.getBytes("UTF-8").length;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		
		int sizeInMB  = (int)Double.parseDouble(args[0]);
		int computationInSeconds = (int) Double.parseDouble(args[1]);
		String outputFolder = args[args.length - 1];
		long tStart = System.currentTimeMillis();
		writeMBsToFile(sizeInMB, outputFolder, "garbage.txt");
		long tEnd = System.currentTimeMillis();
		double tDeltaSeconds = (tEnd - tStart) / 1000.0;
		
		if (tDeltaSeconds < computationInSeconds) {
			long timeToSleep = (long) (computationInSeconds - tDeltaSeconds);
			long sStart, sEnd, sSlept;
			boolean interrupted = false;
			
			while(timeToSleep > 0) {
				sStart = System.currentTimeMillis();
				try {
					Thread.sleep(timeToSleep);
					break;
				}
				catch(InterruptedException e) {
					sEnd = System.currentTimeMillis();
					sSlept = sEnd - sStart;
					timeToSleep -= sSlept;
					interrupted = true;
				}
			}
			
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		}
		
	}
	
	/**
	 * 
	 * Combines the hdfs paths into one.
	 */
	public static String combinePath(String basePath, String relativePath) 
		throws MalformedURLException 
	{
		Preconditions.checkNotNull(basePath);
		Preconditions.checkNotNull(relativePath);
		basePath = basePath + "/";
		//URL mergedURL = new URL(new URL(basePath), relativePath);
		//return mergedURL.toString();
		return basePath + relativePath;
	}
	
	private static void writeMBsToFile(int MBs, String folderPath, String fileName) {
		try {
			Path file = new Path(combinePath(folderPath, fileName));
			if (fs.exists(file)) {
				fs.delete(file, true);
			}
			
			double mbsWritten = 0;
			OutputStream out = fs.create(file);
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
			while (mbsWritten < MBs) {
				br.write(garbageToWrite);
				mbsWritten += noBytesGarbageToWrite / 1000000.0;
			}
			br.close();

		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		
		return;
	}
}
