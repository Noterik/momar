package com.noterik.springfield.momar.TF;

import java.io.File;

import org.apache.log4j.Logger;

import com.noterik.bart.marge.model.Service;
import com.noterik.bart.marge.server.MargeServer;
import com.noterik.springfield.momar.MomarServer;
import com.noterik.springfield.momar.commandrunner.CommandRunner;
import com.noterik.springfield.momar.homer.LazyHomer;
import com.noterik.springfield.momar.homer.LazyMarge;
import com.noterik.springfield.momar.homer.MargeObserver;
import com.noterik.springfield.momar.homer.MountProperties;
import com.noterik.springfield.momar.queue.Job;
import com.noterik.springfield.momar.queue.QueueManager;
import com.noterik.springfield.tools.fs.URIParser;

/**
 * Worker thread that picks up jobs.
 *
 * @author Derk Crezee <d.crezee@noterik.nl>
 * @author Daniel Ockeloen <daniel@noterik.nl>
 * @copyright Copyright: Noterik B.V. 2012
 * @package com.noterik.springfield.momar.TF
 * @access private
 * @version $Id: TranscoderWorker.java,v 1.30 2012-07-31 19:06:36 daniel Exp $
 *
 */
public class TranscoderWorker implements MargeObserver {
	/**	the TranscoderWorker's log4j logger */
	private static final Logger LOG = Logger.getLogger(TranscoderWorker.class);
	
	private boolean busy = false;
	
	/**
	 * Current executing job
	 */
	private Job cJob = null;
	
	public void init() {
		LOG.info("Starting worker");
		
		// subscribe to changed on the queue's
				LazyMarge.addObserver("/domain/webtv/service/momar/queue", this);
				LazyMarge.addTimedObserver("/domain/webtv/service/momar/queue",6,this);
	}
	
	public synchronized boolean checkForNewJob() {
		// get next job
		busy = true;
		
		QueueManager qm = MomarServer.instance().getQueueManager();
		if (qm==null) {
			LOG.info("TranscoderWorker : Queuemanager not found");
			busy = false;
			return false;
		}
		cJob = qm.getJob();
		if(cJob!=null) {
			//System.out.println("TR="+cJob.getStatusProperty("trancoder"));
			if (cJob.getStatusProperty("trancoder")==null) { // no transcoder
				
				LOG.debug("got new job: "+cJob);

				// transcode job
				boolean success = transcode();
				LOG.debug("finished transcoding successfully: "+Boolean.toString(success));
			
				// call to job finished
				jobFinished(success);
			
				// remove job
				removeJob();
				busy = false;
				return success;
			} else {
				System.out.println("JOB TAKEN BY = "+cJob.getStatusProperty("trancoder"));
			}
		} else {
			LOG.debug("No job found");
		}
		busy = false;	
		return false;
	}
	
	public void remoteSignal(String from,String method,String url) {
		if (from.equals("localhost") || method.equals("POST")) {
			if (!busy) {
				boolean donework = checkForNewJob();
				while (donework) {
					donework = checkForNewJob();
				}
			}
		}
	}
	
	/**
	 * Get the current executing job
	 * 
	 * @return
	 */
	public Job getCurrentJob() {
		return cJob;
	}
	
	/**
	 * Handles the job
	 * 
	 * @param job
	 */
	public boolean transcode() {
		// get uri and streams
		TFactory tf = new TFactory();
		
		// set the reencode to false
		tf.setReencodeToFalse(cJob);
		
		// transcode
		return tf.transcode(cJob);
	}
	
	/**
	 * Removes job from queue 
	 * 
	 * @param job
	 */
	public void removeJob() {
		LOG.debug("removing job: "+cJob);
		
		// send delete call
		LazyHomer.sendRequest("DELETE", cJob.getUri(), null, null);
		LOG.debug("send delete call to "+cJob.getUri());
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
		String rawUri = cJob.getProperty("referid");
		
		// set the transferred property
		LazyHomer.sendRequest("PUT", rawUri + "/properties/transferred", "false", "text/xml");
		
		if (success){				
			// set the status property to done
			LazyHomer.sendRequest("PUT", rawUri + "/properties/status", "done", "text/xml");
		}else{		
			// set the status property to fail
			LazyHomer.sendRequest("PUT", rawUri + "/properties/status", "failed", "text/xml");
		}
		//Check if an additional script is provided to run after the job finished
		MountProperties mp = LazyHomer.getMountProperties(cJob.getProperty("mount"));
		String jobFinished = mp.getJobFinished();
		if (jobFinished != null && !jobFinished.equals("")) {
			LOG.debug("About to run script "+jobFinished);
			String batchFilesPath = MomarServer.instance().getConfiguration().getProperty("batchFilesPath");
			String batchFilesExtension = MomarServer.instance().getConfiguration().getProperty("batchFilesExtension");
			
			String filename = cJob.getProperty("filename");
			String filePath = filename.substring(0, filename.lastIndexOf("/"));
			
			String[] cmdArray = new String[] {batchFilesPath+File.separator+jobFinished+batchFilesExtension, filePath};
			LOG.debug("About to run "+batchFilesPath+File.separator+jobFinished+batchFilesExtension+" "+filePath);
			CommandRunner.run(cmdArray);
		}
	}
}








