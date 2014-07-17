package com.noterik.springfield.momar.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.noterik.springfield.momar.homer.LazyHomer;
import com.noterik.springfield.momar.homer.MomarProperties;
import com.noterik.springfield.momar.homer.MountProperties;
import com.noterik.springfield.tools.FileHelper;
import com.noterik.springfield.tools.ftp.FtpHelper;


/**
 * Utility for extracting screenshots
 * 
 * @copyright Copyright: Noterik B.V. 2010
 * @package org.springfield.nelson.util
 * @access private
 *
 */
public class ScreenShotExtractor {
	/** The ScreenshotExtractor's log4j Logger */
	private static final Logger LOG = Logger.getLogger(ScreenShotExtractor.class);
	
	/** the decimal format is used to parse the interval value of the request xml */
	private static DecimalFormat df = new DecimalFormat("#.####");
	
	/** The error messages FFMPEG can print */
	private static List<String> errorMessages = new ArrayList<String>();
	static {
		errorMessages.add("Unsupported video codec");
		errorMessages.add("does not contain any stream");
		errorMessages.add("Invalid data found when processing input");
		errorMessages.add("Incorrect value for r");
	}

	/**
	 * This function either uses ffmpeg or the cutdetector to extract
	 * screenshots from the source file (useCutDetector param). The
	 * useCutDetector can only be used if Kiff runs under windows. The
	 * CutDetector can process mms:// streams or URL's to an .asx or .wmv file.
	 *
	 * @param source	the source file
	 * @param dest		the destination directory
	 * @param size		the dimension of the resulting image
	 * @param interval	number of screenshots per second
	 * @return			success
	 */
	public static boolean extractScreenshots(String source, String dest, String size, String interval) {
		// input check
		if (source == null || dest == null) {
			LOG.error("Incorrect parameters specified");
			return false;
		}
		
		// build command
		String cmd = null;
	//	String ffmpegFolder = NelsonServer.instance().getFFMPEGPath();
		MomarProperties mp = LazyHomer.getMyMomarProperties();
		String ffmpegFolder = mp.getFFMPEGPath();

		//if(ffmpegFolder==null) {
		//	ffmpegFolder = "ffmpeg";
		//}
		cmd = ffmpegFolder+File.separator+"ffmpeg -i " + source + "";
		if (size != null && !size.equals("")) {
			cmd += " -s " + size;
		}
		if (!dest.endsWith(File.separator)) {
			LOG.debug("adding separator char");
			dest += File.separator;
		}
		cmd += " -r " + interval + " " + dest + "image%d.jpg";
		cmd = cmd.replaceAll("\n", "");
		if (cmd == null || cmd.equals("")) {
			LOG.error("Incorrect command");
			return false;
		}
		
		// execute command
		LOG.debug("Command is (LINUX): " + cmd);
		FFMPEGStatus status = commandRunner(cmd);
		LOG.debug("-- FINISHED WITH EXEC OF COMMAND --");
		
		// error reporting
		if(!status.isSuccess()) {
			LOG.error(status.getMessage());
		}
		
		return status.isSuccess();
	}

	/**
	 * Runs the command passed as parameter
	 *
	 * @param comand
	 */
	private static FFMPEGStatus commandRunner(String cmd) {
		FFMPEGStatus status = new FFMPEGStatus();
		BufferedReader br = null;
		try {
			// Execute command
			Process child = Runtime.getRuntime().exec(cmd);
			InputStream is = child.getErrorStream();
			if (is != null) {
				br = new BufferedReader( new InputStreamReader(is));
				String line;
				while ((line = br.readLine()) != null) {
					LOG.debug(line);
					for(String errMsg : errorMessages) {
						if(line.indexOf(errMsg) != -1) {
							status.setSuccess(false);
							status.setMessage(line + " ("+errMsg+")");
						}
					}
				}
			}
		} catch (IOException e) {
			LOG.error("",e);
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				LOG.error("",e);
			}
		}
		return status;
	}
	
	/**
	 * Status object
	 */
	private static class FFMPEGStatus {
		private boolean success = true;
		private String message;
		
		public FFMPEGStatus(){}
		
		/**
		 * @return the success
		 */
		public boolean isSuccess() {
			return success;
		}
		/**
		 * @param success the success to set
		 */
		public void setSuccess(boolean success) {
			this.success = success;
		}
		/**
		 * @return the message
		 */
		public String getMessage() {
			return message;
		}
		/**
		 * @param message the message to set
		 */
		public void setMessage(String message) {
			this.message = message;
		}
	}
	
	public static boolean ftpScreenshotsInCorrectDirs(String screenshotDir, String interval,MountProperties mp,String baserFolder) {
		int pos = baserFolder.indexOf("rawvideo/");
		if (pos!=-1) {
			baserFolder = baserFolder.substring(0,pos);
		}
		
		System.out.println("SCREENSHOT DIR: " + screenshotDir + " I: " + interval);
		
		boolean success = true;
		
		
		boolean filesLeft = true;
		
		/** # screenshots per sec */
		double cadence = 0.0d;
		
		try {
			cadence = df.parse(interval).doubleValue();
		} catch (ParseException e) {
			System.out.println("ImageExtractor error "+e);
			return false;
		}
		
		/** # sec interval (time between two files) */
		double secInterval = 1 / cadence;
		
		/** time the current screenshot represents in the corresponding movie */
		double curTimeInSeconds = 0.0d;
		int i = 1;
		File from = null;
		File to = null;
		//String toFilePath = null;
		
		while (filesLeft) {
			String lFilename = "image" + i + ".jpg";
			System.out.println("FILE="+screenshotDir+lFilename);
			from = new File(screenshotDir+lFilename);
			if (from.exists()) {
				
				String server = mp.getHostname();
				String username = mp.getAccount();
				String password = mp.getPassword();
				String lFolder = screenshotDir;
				String rFilename = getOutputFileAccordingToSeconds(curTimeInSeconds);
				
				// fix remote folder name and rFilename
				pos = rFilename.lastIndexOf("/");
				String rFolder = baserFolder+rFilename.substring(0,pos);
				rFilename = rFilename.substring(pos+1);

				System.out.println("RFOLDER="+rFolder+" RFILE="+rFilename);

				// now ftp it to the mount
				System.out.println("ftp sending to server: "+server+", username: "+username+", password: "+password+", rFolder: "+rFolder+", lFolder: "+lFolder+", filename: "+rFilename);

				boolean ok = FtpHelper.commonsSendFile(server, username, password, rFolder, lFolder, rFilename, lFilename, true);
				if (!ok) return false;
				// delete the file
				try {
					from.delete();
				} catch(Exception e) {
					System.out.println("Can't delete tmp image file");
				}
				curTimeInSeconds += secInterval;
				i++;
			} else {
				filesLeft = false;
			}
		}
		return success;
	}
	
	public static boolean moveScreenshotsInCorrectDirs(String screenshotDir, String interval,String baserFolder) {
		int pos = baserFolder.indexOf("rawvideo/");
		if (pos!=-1) {
			baserFolder = baserFolder.substring(0,pos);
		}
		
		System.out.println("SCREENSHOT DIR: " + screenshotDir + " I: " + interval);
				
		boolean success = true;		
		boolean filesLeft = true;
		
		/** # screenshots per sec */
		double cadence = 0.0d;
		
		try {
			cadence = df.parse(interval).doubleValue();
		} catch (ParseException e) {
			System.out.println("ImageExtractor error "+e);
			return false;
		}
		
		/** # sec interval (time between two files) */
		double secInterval = 1 / cadence;
		
		/** time the current screenshot represents in the corresponding movie */
		double curTimeInSeconds = 0.0d;
		int i = 1;
		File from = null;
		
		while (filesLeft) {
			String lFilename = "image" + i + ".jpg";
			System.out.println("FILE="+screenshotDir+lFilename);
			from = new File(screenshotDir+lFilename);
			if (from.exists()) {
				String rFilename = getOutputFileAccordingToSeconds(curTimeInSeconds);
				// fix remote folder name and rFilename
				pos = rFilename.lastIndexOf("/");
				String rFolder = baserFolder+rFilename.substring(0,pos);
				rFilename = rFilename.substring(pos+1);
				
				System.out.println("RFOLDER="+rFolder+" RFILE="+rFilename);
				File rDir = new File(rFolder);
				boolean rDirExists = false;
				if (!rDir.exists()) {
					rDirExists = rDir.mkdirs();
				} else {
					rDirExists = true;
				}
				if (rDirExists) {
	                from.renameTo(new File(rDir+File.separator+rFilename));
	            }
				curTimeInSeconds += secInterval;
				i++;
			} else {
				filesLeft = false;
			}
		}				
		return success;
	}
	
	/**
	 * Gets the correct directroy name for the current time in seconds. The dir
	 * structure is returned as follows:
	 * h/[HOUR]/m/[MINUTE]/sec[SECOND-OF-MINUTE].jpg
	 *
	 * @param seconds
	 * @return
	 */
	private static String getOutputFileAccordingToSeconds(double seconds) {
		String dir = null;
		int sec = 0;
		int hourSecs = 3600;
		int minSecs = 60;
		int hours = 0;
		int minutes = 0;
		while (seconds >= hourSecs) {
			hours++;
			seconds -= hourSecs;
		}
		while (seconds >= minSecs) {
			minutes++;
			seconds -= minSecs;
		}
		sec = new Double(seconds).intValue();
		dir = "h" + File.separator + hours + File.separator;
		dir += "m" + File.separator + minutes + File.separator;
		dir += "sec" + sec + ".jpg";
		return dir;
	}

	

	
}