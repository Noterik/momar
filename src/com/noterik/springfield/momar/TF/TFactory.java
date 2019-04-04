package com.noterik.springfield.momar.TF;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.springfield.mojo.ftp.FtpHelper;
import org.springfield.mojo.interfaces.ServiceInterface;
import org.springfield.mojo.interfaces.ServiceManager;

import com.noterik.springfield.momar.MomarServer;
import com.noterik.springfield.momar.commandrunner.CommandRunner;
import com.noterik.springfield.momar.homer.LazyHomer;
import com.noterik.springfield.momar.homer.MountProperties;
import com.noterik.springfield.momar.queue.Job;
import com.noterik.springfield.momar.tools.TFHelper;

/**
 * Transcoding part of service.
 *
 * @author Predrag <predrag@noterik.nl>
 * @author Levi Pires <l.pires@noterik.nl>
 * @author Derk Crezee <d.crezee@noterik.nl>
 * @author Pieter van Leeuwen <p.vanleeuwen@noterik.nl>
 * @author Daniel Ockeloen <daniel@noterik.nl>
 * @copyright Copyright: Noterik B.V. 2012
 * @package com.noterik.springfield.momar.TF
 * @access private
 * @version $Id: TFactory.java,v 1.62 2012-07-31 19:06:36 daniel Exp $
 *
 */
public class TFactory implements Runnable {
	/** The TFactory's log4j Logger */
	private static final Logger LOG = Logger.getLogger(TFactory.class);
	
	/** Level for logging command */
	private static final Level COMMAND = new Level(Integer.MAX_VALUE, "COMMAND", Integer.MAX_VALUE) {
		private static final long serialVersionUID = 1L;
	};
	
	/** Current duration of transcoding */
	private long curDuration;
	
	/** Current job */
	private Job _job;
	
	/** ffmpeg path */
	private static String ffmpegPath;
	
	/** path for temporary local storage */
	private static String tempPath;
	
	/** batch files path */
	private static String batchFilesPath;
	
	/** batch files extension */
	private static String batchFilesExtension;
	
	/** time in between status updates */
	private static final long STATUS_UPDATE_TIME = 2 * 1000;
	
	/** number of ffmpeg calls for the transcoding */
	private int totalFfmpegCalls;
	
	/** current ffmpeg call being handled */
	private int currentFfmpegCall;
	
	private TranscoderWorker transcoderWorker;
	
	// load static variables for the configuration
	static {
		ffmpegPath = MomarServer.instance().getConfiguration().getProperty("ffmpeg-path");
		tempPath = MomarServer.instance().getConfiguration().getProperty("temporary-directory");
		batchFilesPath = MomarServer.instance().getConfiguration().getProperty("batchFilesPath");
		batchFilesExtension = MomarServer.instance().getConfiguration().getProperty("batchFilesExtension");
	}
	
	private long statusLastUpdatedTime;
	public TFactory(Job job, TranscoderWorker transcoderWorker){
		statusLastUpdatedTime = 0;
		this._job = job;
		this.transcoderWorker = transcoderWorker;
	}
	
	@Override
	public void run() {
		LOG.info(Thread.currentThread().getName() +" starting with job "+_job.getInputFilename());
		
		setReencodeToFalse(_job);
		boolean success  = transcode(_job);
		// call to job finished
		jobFinished(success);
		removeJob();
		LOG.info(Thread.currentThread().getName() +" finished with job "+_job.getInputFilename());
		transcoderWorker.jobFinished();
		
	}
	
	/**
	 * sets the reencode tag to false in the rawvideo properties
	 * 
	 * @param job
	 */
	public void setReencodeToFalse(Job job){
		
		String rawUri = job.getProperty("referid");
		
		// send PUT call
		LOG.debug("uri: " + rawUri + "/properties/reencode" );
		ServiceInterface smithers = ServiceManager.getService("smithers");
		if (smithers==null) return;
		smithers.put(rawUri + "/properties/reencode", "false", "text/xml");
	}	
	
	/*
	 * Transcoding of a job using the parameters sent in the Job instance
	 */
	public boolean transcode(Job job){
		LOG.info("Transcode job("+job.getId()+")");
		
		// check job
		String mount = job.getProperty("mount");
		String extension = job.getProperty("extension");
		String referid = job.getProperty("referid");
		String width = job.getProperty("wantedwidth"); 
		String height = job.getProperty("wantedheight"); 
		String vbitrate = job.getProperty("wantedbitrate");
		String framerate = job.getProperty("wantedframerate");
		String abitrate = job.getProperty("wantedaudiobitrate");
		if(mount==null || extension==null || referid==null || width==null || height==null || vbitrate==null || framerate==null || abitrate==null) {
			LOG.error("incorrect parameters passed");
			job.setError("Transcoding Failed", "incorrect parameters passed");
			return false;
		}
		
		// get path to input file, and output directory
		String inputFile = "";
		String outputDir = "";
		boolean local = TFHelper.isLocalJob(job); 
		try {
			_job = job;
			String[] streamname = TFHelper.getStreams(job);
			String streamPath = TFHelper.getPathOfStream(streamname[0]);
			
			// check stream path
			LOG.debug("stream path is: " + streamPath);
			if(streamPath==null) {
				LOG.error("Transcoding failed, mount was incorrectly set");
				job.setError("Transcoding failed", "mount was incorrectly set");
				return false;
			}
			
			// get input
			String path = job.getInputURI();
			//LOG.debug("original: " + path);
			
			if(!local) {
				//LOG.debug("file is REMOTE");
				// get file if not local
				outputDir = tempPath +File.separator +job.getId()+File.separator;
				if(!(new File(outputDir)).exists()){
					(new File(outputDir)).mkdirs();
				}
				
				getOriginalFileWithFtp(job);
				inputFile = tempPath+File.separator+job.getId()+File.separator+"input."+job.getProperty("extension");

			} else {
				if (job.getOutputURI() != null) {
					inputFile = streamPath + path + job.getInputFilename(); 
					outputDir = streamPath + job.getOutputURI();
				} else {				
					inputFile = streamPath + path + File.separator + job.getInputFilename(); 
					outputDir = streamPath + File.separator + job.getProperty("referid") + File.separator;
				}
				
				LOG.debug("Output dir = "+outputDir);
				
				// create output folder
				if(!(new File(outputDir)).exists()) {
				    	LOG.debug("Output dir didn't exist yet, create");
					(new File(outputDir)).mkdirs();
				}
			}
			
			// custom batch file 
			if (job.getProperty("batchfile") != null) {
				String batchfile = job.getProperty("batchfile")+batchFilesExtension;
				LOG.debug("transcode using batch file "+batchfile);
			
				String originalBitrate = job.getOriginalProperty("videobitrate") != null ? job.getOriginalProperty("videobitrate") : "0";
				String originalDuration = job.getOriginalProperty("duration") != null ? job.getOriginalProperty("duration") : "1";
				String originalExtension = job.getOriginalProperty("extension") != null ? job.getOriginalProperty("extension") : "unknown";
				String originalFramerate = job.getOriginalProperty("framerate") != null ? job.getOriginalProperty("framerate") : "25";
				String outputFilename = job.getOutputFilename() != null ? "temp_" + job.getOutputFilename() : "raw." + job.getProperty("extension");
				
				String[] cmdArray = new String[] {batchFilesPath+File.separator+batchfile, ffmpegPath+File.separator, inputFile, job.getProperty("wantedwidth"), job.getProperty("wantedheight"), job.getProperty("wantedbitrate"), job.getProperty("wantedframerate"), job.getProperty("wantedaudiobitrate"), outputDir, job.getProperty("extension"), tempPath, job.getId(), job.getOriginalProperty("width"), job.getOriginalProperty("height"), originalBitrate, originalDuration, originalExtension, originalFramerate, outputFilename};
				LOG.debug("command: "+batchFilesPath+File.separator+batchfile+" "+ffmpegPath+File.separator+" "+inputFile+" "+job.getProperty("wantedwidth")+" "+job.getProperty("wantedheight")+" "+job.getProperty("wantedbitrate")+" "+job.getProperty("wantedframerate")+" "+job.getProperty("wantedaudiobitrate")+" "+outputDir+" "+job.getProperty("extension")+" "+tempPath+" "+job.getId()+" "+job.getOriginalProperty("width")+" "+job.getOriginalProperty("height")+" "+originalBitrate+" "+originalDuration+" "+originalExtension+" "+originalFramerate +" "+ outputFilename);
				
				File bFile = new File(batchFilesPath+File.separator+batchfile);
				if (!bFile.exists()) {
					System.out.println("Batch file not found "+batchFilesPath+File.separator+batchfile);
					job.setError("Error", "Transcoding Failed MP4, batch file not found");
					return false;
				}
				
				this.commandRunner(cmdArray);
			
				if(new File(outputDir + outputFilename).isFile()){
					// check filesize after transcode
					if (new File(outputDir + outputFilename).length() > 0) {
						//rename temp file to final filename
						if (job.getOutputFilename() != null) {
							new File(outputDir + "temp_"+job.getOutputFilename()).renameTo(new File(outputDir+job.getOutputFilename()));
						}						
						LOG.debug("Transcoding finished (mp4).");
					} else {
						job.setError("Error", "Transcoding Failed MP4 (empty file)");
						return false;
					}
				}else{
					job.setError("Error", "Transcoding Failed MP4");
					return false;
				}
			}
		}catch(Exception e) {
			job.setError("Transcoding failed","");
			LOG.error("Transcoding failed",e);
			return false;
		}
		
		LOG.debug("putting transcoded file on other streams");
		
		// ftp files to other streams, and if not local to all streams
		String[] streams = TFHelper.getStreams(job);
		for(int i = local ? 1: 0; i<streams.length; i++) {
		    String stream = streams[i];
		    MountProperties mp = LazyHomer.getMountProperties(stream);
	
			   
		    if (mp == null) {
			System.out.println("Mount properties not set for Momar for mount "+stream);
			break;
		    }		   
		    
		    //Check for local file
		    if (mp.getHostname().equals(LazyHomer.myip)) {
			InputStream is = null;
			OutputStream os = null;
			try {
			    String iFile = outputDir + "raw." + job.getProperty("extension");
			    String rFile = mp.getPath()+job.getProperty("referid")+File.separator+"raw."+ job.getProperty("extension");
			    
			    if (job.getOutputFilename() != null) {
				iFile = outputDir+job.getOutputFilename();
				rFile = mp.getPath()+job.getProperty("referid")+File.separator+job.getOutputFilename();
			    }
			    
			    //Create folder
			    File rPath = new File (mp.getPath()+job.getProperty("referid"));
			    if (!rPath.exists()) {
				rPath.mkdirs();
			    }
			    
			    //copy file
			    is = new FileInputStream(iFile);
			    os = new FileOutputStream(rFile);
			    byte[] buffer = new byte[1024];
			    int length;
			    while ((length = is.read(buffer)) > 0) {
				os.write(buffer, 0, length);
			    }
			} catch (Exception e) {
			    LOG.error("Could not copy file to stream "+stream);
			    LOG.error(e.getMessage());
			} finally {
			    try {
				is.close();
				os.close();
			    } catch (Exception e) {
				LOG.error("Could not copy file to stream "+stream);
				LOG.error(e.getMessage());
			    }
			}
		    } else {			
			String server = mp.getHostname();
        		String username = mp.getAccount();
        		String password = mp.getPassword();
        		String rFolder = mp.getPath()+job.getProperty("referid");
        		String lFolder = outputDir;
        		String filename = "raw."+extension;		
        		LOG.debug("sending to server: "+server+", username: "+username+", password: "+password+", rFolder: "+rFolder+", lFolder: "+lFolder+", filename: "+filename);
        			
        		// send
        		boolean ok = FtpHelper.commonsSendFile(server, username, password, rFolder, lFolder, filename);
        		if(!ok) {
        			LOG.error("Could not send file to ftp. " + server + " -- " + job.getProperty("referid"));
        		}
		    }
		}
		
		// cleanup temporary files
		if(!local) {		
			 File f = new File(tempPath+File.separator+job.getId()+File.separator+"input."+job.getProperty("extension"));
			 if (f.exists()) {
				 f.delete();
			 }
			 f = new File(tempPath+File.separator+job.getId()+File.separator+"raw."+job.getProperty("extension"));
			 if (f.exists()) {
				 f.delete();
			 }
			 File folder = new File(tempPath+File.separator+job.getId());
			 if (folder.exists()) {
				folder.delete(); 
			 }
		}
		
		// everything went fine
		job.setStatus("Progress", "Done");
		LOG.info("Transcode done job("+job.getId()+")");
		return true;
	}
	
	/**
	 * Get original file with ftp
	 * 
	 * @param job	The job
	 * @return 		Successfully got original file or not.
	 */
	private boolean getOriginalFileWithFtp(Job job) {
		// create the local folder
		//String localPath = tempPath+job.getInputURI();
		String localPath = tempPath;

		boolean created = false;
		/*
		try{
			created = new File(localPath).mkdirs();
		}catch(SecurityException e){
			LOG.error("security exception while creating local path",e);
		}
		LOG.debug("folder " + localPath + " was created: " + created);
		*/
		
		// get original properties
		String extension="", mount="", original = job.getInputURI();
		if(original==null) {
			LOG.error("Could not get original with ftp, orginal not set.");
			return false;
		}		
		
		//String mount = job.getProperty("mount");
		//String extention = job.getProperty("extention");
		
		
		Document doc = getProperties(original);	
		if (doc != null){			
			String mounts = doc.selectSingleNode("//properties/mount").getText();
			String[] streamnames = mounts.split(",");	
			
			mount = streamnames[0];
			extension = doc.selectSingleNode("//properties/extension").getText();
		}
		
		MountProperties mp = LazyHomer.getMountProperties(mount);
		
		// define ftp variables
		String server = mp.getHostname();
		String username = mp.getAccount();
		String password = mp.getPassword();
		String rFolder = mp.getPath()+job.getInputURI();
		String lFolder = localPath+File.separator+job.getId();
		String rFilename = "raw."+extension;
		String lFilename = "input."+extension;
		LOG.debug("server: "+server+", username: "+username+", password: "+password+", rFolder: "+rFolder+", lFolder: "+lFolder+", lFilename: "+lFilename+", rFilename:"+rFilename);
		
		// get file using ftp
		boolean success = FtpHelper.commonsGetFile(server, username, password, rFolder, lFolder, rFilename, lFilename);
		//boolean success = FtpHelper.commonsGetFile(server, username, password, rFolder, lFolder, filename);
		
		LOG.debug("getting file was successful: " + success);
		
		// return successful
		return success;
	}
	
	/**
	 * Get properties from smithers.
	 * 
	 * @param uri
	 * @return
	 */
	private Document getProperties(String uri){		
		
		// send request
		ServiceInterface smithers = ServiceManager.getService("smithers");
		if (smithers==null) return null;
		String response = smithers.get(uri, null, null);
		
		// parse
		Document doc = null;		
		try {
			doc = DocumentHelper.parseText(response);
		} catch (DocumentException e) {
			LOG.error("Could not parse respopnse from smithers",e);
		}		
		return doc;
	}
	 
 
	/**
	 * Runs command.
	 * 
	 * @param cmd
	 */
	private void commandRunner(String[] cmd) {
		// TODO: research on linux "nice" command for priority set
		totalFfmpegCalls = 1;
		currentFfmpegCall = 1;
	
		try {
			// Execute command
			final Process child = Runtime.getRuntime().exec(cmd);
			
			// Handle stdout...
			new Thread() {
			    public void run() {
			    	BufferedInputStream bis = null;
			    	try {		
						// Get the input stream and read from it
						InputStream is = child.getInputStream();

						if (is != null) {
							bis = new BufferedInputStream(is);
							int c;
							StringBuffer line = new StringBuffer();
							while ((c = bis.read()) != -1) {
								if(c != '\r' && c != '\n'){
									line.append((char) c);
								} else {
									parseOutput(line.toString());
									line = new StringBuffer();
								}
							}
						}
					} catch (IOException e) {
						e.printStackTrace();
					} finally {
						try {
							bis.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
			    }
			}.start();

			// Handle stderr...
			new Thread() {
			    public void run() {
			    	BufferedInputStream bis = null;
			    	try {		
						// Get the error stream and read from it
						InputStream is = child.getErrorStream();

						if (is != null) {
							bis = new BufferedInputStream(is);
							int c;
							StringBuffer line = new StringBuffer();
							while ((c = bis.read()) != -1) {
								if(c != '\r' && c != '\n'){
									line.append((char) c);
								} else {
									parseOutput(line.toString());
									line = new StringBuffer();
								}
							}
						}
					} catch (IOException e) {
						e.printStackTrace();
					} finally {
						try {
							bis.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
			    }
			}.start();	
			
			child.waitFor();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Parse ffmpeg output.
	 * 
	 * TODO: create OutputParsers for different ffmpeg versions
	 * 
	 * @param line
	 */
	private void parseOutput(String line){
		LOG.debug(line);
		int di = line.indexOf("Duration: ");
		int fi = line.indexOf(" time=");
		int tfc = line.indexOf("Total ffmpeg calls:");
		int cfc = line.indexOf("Current ffmpeg call:");
		int prg = line.indexOf("Progress: ");
		try {
			if(di != -1){
				//String ds = line.substring(di + "Duration: ".length(), line.indexOf("."));
				String ds = line.substring(di + "Duration: ".length(), line.indexOf(",",di));
				LOG.debug("DURATION: " + ds);
				String hs = ds.substring(0, ds.indexOf(":"));
				ds = ds.substring(ds.indexOf(":") + 1, ds.length());
				String ms = ds.substring(0, ds.indexOf(":"));
				ds = ds.substring(ds.indexOf(":") + 1, ds.length());
				String ss = ds.substring(0, ds.indexOf("."));
				ds = ds.substring(ds.indexOf(".") +1, ds.length());
				String dms = ds.substring(0, ds.length());
				LOG.debug("H: " + hs + " M: " + ms + " S: " + ss +" MS: "+ dms);
				short h = new Short(hs).shortValue();
				short m = new Short(ms).shortValue();
				short s = new Short(ss).shortValue();
				short mms = new Short(dms).shortValue();
				curDuration = (h * 60 * 60 * 1000) + (m * 60 * 1000) + (s *1000) + (mms * 10);
				LOG.debug("CURRENT DURATION: " + curDuration);
			} else if(fi != -1){
				String ts = line.substring(fi + " time=".length());
				ts = ts.substring(0, ts.indexOf(" "));
				LOG.debug("TIME: " + ts);
				if (ts.indexOf(":") == -1) {
					String ss = ts.substring(0, ts.indexOf("."));
					String dss = ts.substring(ts.indexOf(".") + 1);
					short s = new Short(ss).shortValue();
					short ds = new Short(dss).shortValue();
					long time = (s * 1000) + (ds * 10);
					setProgress(time);
				} else {
					String[] result = ts.split(":");
					if (result.length == 3) {
						String hh = result[0];
						String mm = result[1];
						String ss = result[2];
						String sss = ss.substring(0, ss.indexOf("."));
						String dss = ss.substring(ss.indexOf(".") + 1);						
						
						short h = new Short(hh).shortValue();
						short m = new Short(mm).shortValue();
						short s = new Short(sss).shortValue();
						short ds = new Short(dss).shortValue();
						long time = (h * 60 * 60 * 1000) + (m * 60 * 1000) +(s * 1000) + (ds * 10);
						setProgress(time);
					}
				}
			} else if (tfc != -1) {
				String tfcs = line.substring(tfc + "Total ffmpeg calls:".length());
				LOG.debug("TOTAL FFMPEG CALLS: "+tfcs);
				if (tfcs != null) {
					totalFfmpegCalls = Integer.parseInt(tfcs);
				}
			} else if (cfc != -1) {
				String cfcs = line.substring(cfc + "Current ffmpeg call:".length());
				LOG.debug("CURRENT FFMPEG CALL: "+cfcs);
				if (cfcs != null) {
					currentFfmpegCall = Integer.parseInt(cfcs);
				}
			} else if (prg != -1) {
			    String progress = line.substring(prg);
			    progress = progress.substring(progress.indexOf(":")+1).trim();
			    double progr = Double.parseDouble(progress);
			    
			    if (totalFfmpegCalls > 1) {
				progr = progr / (double) totalFfmpegCalls;
				progr += ((double)(currentFfmpegCall-1)/(double)totalFfmpegCalls)*100;
				LOG.debug("Total progress: "+progr);
			    }			    
			    
			    long currentTime = System.currentTimeMillis();
			    // only log every so many seconds
			    if( (currentTime - statusLastUpdatedTime) > STATUS_UPDATE_TIME) {
				_job.setStatus("Progress", Double.toString(progr));
				statusLastUpdatedTime = System.currentTimeMillis(); // setStatus could have taken some time
			    }
			}
		} catch(Exception e) {
			LOG.error("Could not parse ffmpeg output",e);
			_job.setError("Error", "Transcoding Failed, corrupted video");
		}
	}
	
	/**
	 * Set progress in procentages.
	 * 
	 * @param time
	 */
	private void setProgress(long time){	
		if(curDuration > 0 && time > 0){
			long currentTime = System.currentTimeMillis();
			long now = time / (curDuration / 100);
			LOG.debug(now);
			now = now / totalFfmpegCalls;
			now += ((double)(currentFfmpegCall-1)/(double)totalFfmpegCalls)*100;
			LOG.debug("Total progress: "+now);
			// only log every so many seconds
			if( (currentTime - statusLastUpdatedTime) > STATUS_UPDATE_TIME ) {
				_job.setStatus("Progress", now + "");
				statusLastUpdatedTime = System.currentTimeMillis(); // setStatus could have taken some time
			}
		}
	}
	
	/**
	 * set the properties in the rawvideo after transcoding
	 * 
	 * @param job
	 * @param success
	 */
	private void jobFinished(boolean success){
		LOG.debug("call to jobFinished");
		
		// rawvideo uri 
		String rawUri = _job.getProperty("referid");
		
		// set the transferred property
		ServiceInterface smithers = ServiceManager.getService("smithers");
		if (smithers==null) return;
		String response = smithers.put(rawUri + "/properties/transferred", "false", "text/xml");
		
		if (success){				
			// set the status property to done
			smithers.put(rawUri + "/properties/status", "done", "text/xml");
		}else{		
			// set the status property to fail
			smithers.put(rawUri + "/properties/status", "failed", "text/xml");
		}
		//Check if an additional script is provided to run after the job finished
		String mount = _job.getProperty("mount");
		if (mount.indexOf(",") > -1) {
			mount = mount.substring(0,mount.indexOf(","));
		}
		
		MountProperties mp = LazyHomer.getMountProperties(mount);
		String jobFinished = mp.getJobFinished();
		if (jobFinished != null && !jobFinished.equals("")) {
			LOG.debug("About to run script "+jobFinished);
			String batchFilesPath = MomarServer.instance().getConfiguration().getProperty("batchFilesPath");
			String batchFilesExtension = MomarServer.instance().getConfiguration().getProperty("batchFilesExtension");
			
			String filename = _job.getProperty("filename");
			String filePath = filename.substring(0, filename.lastIndexOf("/"));
			
			String[] cmdArray = new String[] {batchFilesPath+File.separator+jobFinished+batchFilesExtension, filePath};
			LOG.debug("About to run "+batchFilesPath+File.separator+jobFinished+batchFilesExtension+" "+filePath);
			CommandRunner.run(cmdArray);
		}
	}
	
	/**
	 * Removes job from queue 
	 * 
	 * @param job
	 */
	public void removeJob() {
		LOG.debug("removing job: "+_job);
		
		// send delete call
		ServiceInterface smithers = ServiceManager.getService("smithers");
		if (smithers==null) return;
		smithers.delete( _job.getUri(), null, null);
		LOG.debug("send delete call to "+_job.getUri());
	}
}