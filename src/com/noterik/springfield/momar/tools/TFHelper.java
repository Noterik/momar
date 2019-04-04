package com.noterik.springfield.momar.tools;

import java.io.File;
import java.util.ArrayList;

import com.noterik.springfield.momar.homer.LazyHomer;
import com.noterik.springfield.momar.homer.MountProperties;
import com.noterik.springfield.momar.queue.Job;

/**
 * Tools for running Transcoders.
 *
 * @author Derk Crezee <d.crezee@noterik.nl>
 * @author Daniel Ockeloen <daniel@noterik.nl>
 * @copyright Copyright: Noterik B.V. 2012
 * @package com.noterik.springfield.momar.tools
 * @access private
 * @version $Id: TFHelper.java,v 1.8 2012-07-31 19:06:45 daniel Exp $
 *
 */
public class TFHelper {
	/**
	 * Determines if the input file of this job is local or not.
	 * 
	 * @param job 	job
	 * @return
	 */
	public static boolean isLocalJob(Job job) {
		 String[] mounts = job.getProperty("mount") != null ? job.getProperty("mount").split(",") : new String[0];
		 for(int i =0; i < mounts.length ; i++) {
			 String name = mounts[i];
			 MountProperties minfo = LazyHomer.getMountProperties(name);
		   
			 if (minfo == null) {
				 System.out.println("Mount properties not set for Momar for mount "+name);
				 return false;
			 }		   
			 
			 System.out.println("mount ip = "+minfo.getHostname()+" this momar ip "+LazyHomer.myip);		   
			 if (minfo.getHostname().equals(LazyHomer.myip)) {
				 return true;
			 }
		}		 
		return false;
	}

	public static boolean isFtpJob(Job job) {
		 String[] mounts = job.getProperty("mount").split(",");
		 for(int i =0; i < mounts.length ; i++) {
		   String name = mounts[i];
		   MountProperties minfo = LazyHomer.getMountProperties(name);
	
		   if (minfo!=null && minfo.getProtocol().equals("ftp")) {
			   return true;
		   }
		 }
		 
		return false;
	}

	
	/**
	 * Returns the file path to the original raw.
	 * 
	 * @param job
	 * @return
	 */
	public static String getInputFilePath(Job job) {
		// get streams
		String[] streams = getStreams(job);
		if(streams==null) {
			return null;
		}
		
		// get input
		String input = job.getInputURI();
		
		// construct path
		String filePath = getPathOfStream(streams[0]) + input + File.separatorChar + job.getInputFilename();
		
		return filePath;
	}
	
	public static String getDomainFromUrl(String url) {
		int pos = url.indexOf("/domain/");
		if (pos!=-1) {
			String result = url.substring(pos+8);
			pos = result.indexOf("/");
			return result.substring(0,pos);
		}
		return null;
	}
	
	public static String getUserFromUrl(String url) {
		int pos = url.indexOf("/user/");
		if (pos!=-1) {
			String result = url.substring(pos+6);
			pos = result.indexOf("/");
			return result.substring(0,pos);
		}
		return null;
	}
	
	public static String getVideoIdFromUrl(String url) {
		int pos = url.indexOf("/video/");
		if (pos!=-1) {
			String result = url.substring(pos+7);
			pos = result.indexOf("/");
			return result.substring(0,pos);
		}
		return null;
	}
	
	public static String getRawvideoIdFromUrl(String url) {
		int pos = url.indexOf("/rawvideo/");
		if (pos!=-1) {
			String result = url.substring(pos+10);
			pos = result.indexOf("/");
			if (pos!=-1) {
				return result.substring(0,pos);
			} else {
				return result;
			}
		}
		return null;
	}
	
	/**
	 * Returns the streams of a job.
	 * 
	 * @param job
	 * @return
	 */
	public static String[] getStreams(Job job) {
		String[] streams = null;
		
		// get mount
		String mountStr = job.getProperty("mount");
		if(mountStr != null) {
			streams = mountStr.split(",");
		}
		
		return streams;
	}
	
	/**
	 * Returns the base path of the given stream.
	 * 
	 * @param stream	Stream name
	 * @return			The base path of the given stream, null if not found.
	 */
	public static String getPathOfStream(String stream) {
		MountProperties mp = LazyHomer.getMountProperties(stream);
		if (mp!=null) {
			return mp.getPath();
		}
		return null;
	}
}
