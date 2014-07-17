package com.noterik.springfield.momar.queue.dist;

import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import com.noterik.springfield.momar.queue.Job;
import com.noterik.springfield.momar.tools.TFHelper;

/**
 * Simple implementation of a decision engine. Only determines if 
 * the video file is available on this stream.
 *
 * @author Derk Crezee <d.crezee@noterik.nl>
 * @copyright Copyright: Noterik B.V. 2009
 * @package com.noterik.springfield.momar.queue
 * @access private
 * @version $Id: SimpleDecisionEngine.java,v 1.7 2012-07-31 19:06:24 daniel Exp $
 *
 */
public class SimpleDecisionEngine implements DecisionEngine {
	/** The SimpleDecisionEngine's log4j Logger */
	private static final Logger LOG = Logger.getLogger(SimpleDecisionEngine.class);
	
	/**
	 * Returns true if the video file is on this stream.
	 */
	public boolean processJob(Job job) {
		// Get (first) stream
		String[] streams = TFHelper.getStreams(job);
		if(streams==null) {
			LOG.error("cannot determine streams for item "+job.getUri());
			return false;
		}
		// Get own IP, and streamer IP
		try {
			String firstStream = streams[0];
			String host = firstStream + ".noterik.com";
			String localIP = java.net.InetAddress.getLocalHost().getHostAddress();
			String streamIP = java.net.InetAddress.getByName(host).getHostAddress();
			if(localIP.equals(streamIP)) {
				return true;
			}
		} catch (UnknownHostException e) {
			LOG.error("Could not determine IP adress",e);
		} catch(Exception e) {
			LOG.error("Error while determining to process job",e);
		}
		return false;
	}

}
