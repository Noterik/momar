package com.noterik.springfield.momar.queue.dist;

import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import com.noterik.springfield.momar.queue.Job;

public class DistributedDecisionEngine implements DecisionEngine {

	/** The DistributedDecisionEngine's log4j Logger */
	private static final Logger LOG = Logger.getLogger(DistributedDecisionEngine.class);
	private String hostname = null;
	
	public boolean processJob(Job job) {
		try {
			//check if job is already being processed
			if (!isJobBeingProcessed(job)) {
				//claim job
				job.setStatusProperty("transcoder", getHostname());
				//wait 2,5 seconds to check if we can claim this job
				Thread.sleep(2500);
				if (job.getStatusProperty("transcoder") != null && job.getStatusProperty("transcoder").equals(getHostname())) {
					return true;
				}
			}
		} catch (InterruptedException e) {
			LOG.error("InterruptedException",e);
		}
		return false;
	}
	
	private String getHostname() {
		if (hostname == null) {
			try {
				hostname = java.net.InetAddress.getLocalHost().getHostName();
			} catch (UnknownHostException e) {
				LOG.error("Could not determine host name",e);
			} 
		}
		return hostname;
	}
	
	private boolean isJobBeingProcessed(Job job) {
		String transcoder = job.getStatusProperty("transcoder");
		String message = job.getStatusProperty("message");
		
		if (transcoder == null && message == null) {
			return false;
		}
		return true;
	}
}
