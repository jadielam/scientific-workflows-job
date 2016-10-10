package io.biblia.workflows.job;

import java.util.Properties;
import java.net.URL;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;


import com.google.common.base.Preconditions;

public class Main {
	
	private static String NAMENODE_URL;
	
	private static String garbageToWrite;
	
	private static int noBytesGarbageToWrite;
	
	static {
		
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
	
	private static FileSystem getFileSystem(String nameNode) throws Exception {
		FileSystem fs = null;
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		try {
			Configuration conf = new Configuration();
			conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
			conf.set("fs.defaultFS", nameNode);
			fs = FileSystem.get(new URI(NAMENODE_URL), conf);
			return fs;
		}
		catch(Exception ex) {
			ex.printStackTrace();
			throw ex;
		}
	}
	
	public static void main(String[] args) throws IOException, IllegalArgumentException,
	MalformedURLException, Exception  {
		
		int sizeInMB  = (int)Double.parseDouble(args[0]);
		int computationInSeconds = (int) Double.parseDouble(args[1]);
		String nameNode = args[2];
		FileSystem fs = getFileSystem(nameNode);
		String outputFolder = args[args.length - 1];
		long tStart = System.currentTimeMillis();
		writeMBsToFile(fs, sizeInMB, outputFolder, "garbage.txt");
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
	
	private static void writeMBsToFile(FileSystem fs, int MBs, String folderPath, String fileName) 
	throws IOException, MalformedURLException, IllegalArgumentException {
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
			throw e;
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw e;
		}
		catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
		
		return;
	}
}
