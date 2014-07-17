package com.noterik.springfield.momar.queue.dist;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.dom4j.Node;

import com.noterik.springfield.momar.homer.LazyHomer;
import com.noterik.springfield.momar.queue.Job;
import com.noterik.springfield.momar.tools.TFHelper;

/**
 * Decision engine to pick who does encoding jobs based on local, ftp access
 *
 * @author Daniel Ockeloen <daniel@noterik.nl>
 * @copyright Copyright: Noterik B.V. 2012
 * @package com.noterik.springfield.momar.tools
 * @access private
 * @version $Id: TFHelper.java,v 1.8 2012-07-31 19:06:45 daniel Exp $
 *
 */
public class OfferDecisionEngine implements DecisionEngine {

	/** The DistributedDecisionEngine's log4j Logger */
	private static final Logger LOG = Logger.getLogger(OfferDecisionEngine.class);
	private String hostname = null;
	
	public boolean processJob(Job job) {

		try {
			//check if job is already being processed
			if (!isJobBeingProcessed(job) && !hasVoted(job)) {
				String myname = LazyHomer.getMyMomarProperties().getName();
				int score = 0; // we start with a score of zero
				
				// The trick is to make a offer for the job, after x seconds each node
				// will look at the offers and pick a winner.
						
				// do we have access to the file ?
				//System.out.println("LOCALFILE="+TFHelper.isLocalJob(job));
				if (TFHelper.isLocalJob(job)) {
					score = score + 1000; // its a local file 
				} else {
					if (TFHelper.isFtpJob(job)) {
						score = score + 100; // its remote but we can reach it using ftp
					} else {
						score = -1;
					}
				} 
				
				// we can use the position in the list and a mod to device a know 'random'
				if (score!=-1) {
					try {
						int m = Integer.parseInt(job.getId())%LazyHomer.getNumberOfMomars();
						if (m==(LazyHomer.getMyMomarPosition()-1)) {
							score++;
						}
					} catch (Exception e) {};
						
				}
				LOG.info("My offer score ="+score+"(momar count ="+LazyHomer.getNumberOfMomars()+" my pos="+LazyHomer.getMyMomarPosition()+")");
				if (score==-1) {
					// we refuse to make a offer on the job, so return false;
					return false;
				}
				job.setStatusProperty("offer_"+URLEncoder.encode(myname),""+score);
				
				//job.setStatusProperty("transcoder", myname);
				
				if (score<1000) {
					// we are not sure if we won so wait for 2.5 seconds and
					// check the offers
					if (score<1000) Thread.sleep(2500);
					if (getWinningOffer(job).equals(myname)) {
						// we won the offer claim it
						job.setStatusProperty("transcoder",URLEncoder.encode(myname));
						return true;	
					}
				} else {
					// we claim victory right away we won by default !
					job.setStatusProperty("transcoder", URLEncoder.encode(myname));
					return true;
				}
			}
		} catch (InterruptedException e) {
			LOG.error("InterruptedException",e);
		}
		return false;
	}
	
	private String getWinningOffer(Job job) {
		String winner = "unknown";
		int maxscore = -1;
		for(Iterator<String> iter = job.getStatusProperties().iterator(); iter.hasNext(); ) {
			String offer = iter.next();
			if (offer.indexOf("offer_")==0) { // is it a valid offer ?
				int pos = offer.indexOf("=");
				int score = Integer.parseInt(offer.substring(pos+1));
				if (score>maxscore) {
					maxscore = score;
					winner = offer.substring(0,pos);
					winner = winner.substring(6);
					winner = URLDecoder.decode(winner);
				}
			}
		}
		if (!winner.equals("unknown")) System.out.println("Winning job offer by "+winner+" with score="+maxscore);
		return winner;
	}
	
	private boolean isJobBeingProcessed(Job job) {
		String transcoder = job.getStatusProperty("transcoder");
		if (transcoder == null) {
			return false;
		}
		LOG.debug("Job is already being processed by "+transcoder);
		return true;
	}
	
	private boolean hasVoted(Job job) {
		String myname = LazyHomer.getMyMomarProperties().getName();
		String transcoder = job.getStatusProperty("offer_"+URLEncoder.encode(myname));
		if (transcoder == null) {
			return false;
		}
		LOG.debug("Job is offered "+transcoder);
		return true;
	}
}
