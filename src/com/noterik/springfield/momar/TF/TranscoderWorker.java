package com.noterik.springfield.momar.TF;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.noterik.springfield.momar.MomarServer;
import com.noterik.springfield.momar.queue.Job;
import com.noterik.springfield.momar.queue.QueueManager;

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
public class TranscoderWorker {
	/**	the TranscoderWorker's log4j logger */
	private static final Logger LOG = Logger.getLogger(TranscoderWorker.class);
	
	private boolean running = true;
	private int busyWorkers = 0;
	private int numberOfWorkers = 1;
	
	ExecutorService executor;
	
	/**
	 * Current executing job
	 */
	//private Job cJob = null;
	
	public TranscoderWorker(int numberOfWorkers) {
		this.numberOfWorkers = numberOfWorkers;
		
		init();
		while (running) {
			try {
				LOG.debug("TranscoderWorker checking for new jobs");
				checkForNewJob();
				Thread.sleep(25000);
			} catch (InterruptedException e) {
				
			}
		}
	}
	
	public void init() {
		LOG.info("Starting main transcoder worker");
		
		executor = Executors.newFixedThreadPool(numberOfWorkers);
	}
	
	public void checkForNewJob() {
		if (busyWorkers < numberOfWorkers) {
			LOG.debug("TranscoderWorker checking for new job (only "+busyWorkers+" of "+numberOfWorkers+" busy)");
			
			// get next job
			QueueManager qm = MomarServer.instance().getQueueManager();
			if (qm==null) {
				LOG.info("TranscoderWorker : Queuemanager not found");
			}
			Job cJob = qm.getJob();
			
			if(cJob!=null) {
				//System.out.println("TR="+cJob.getStatusProperty("trancoder"));
				if (cJob.getStatusProperty("trancoder")==null) { // no transcoder					
					LOG.debug("got new job: "+cJob);
					
					busyWorkers++;
					Runnable tfworker = new TFactory(cJob, this);
					executor.execute(tfworker);
				} else {
					System.out.println("JOB TAKEN BY = "+cJob.getStatusProperty("trancoder"));
				}
			} else {
				LOG.debug("No job found");
			}
		} else {
			LOG.debug("TranscoderWorker: all workers busy ("+busyWorkers+ " of "+numberOfWorkers+")");
		}
	}

	public void jobFinished() {
		LOG.debug("TranscoderWorker received a job finished");
		busyWorkers--;
	}
	
	public void destroy() {
		executor.shutdownNow();
	}
}