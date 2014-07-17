package com.noterik.springfield.momar.queue.dist;

import com.noterik.springfield.momar.queue.Job;

/**
 * Decide if this job is yours or not
 *
 * @author Derk Crezee <d.crezee@noterik.nl>
 * @copyright Copyright: Noterik B.V. 2009
 * @package com.noterik.springfield.momar.queue
 * @access private
 * @version $Id: DecisionEngine.java,v 1.2 2009-05-06 10:11:59 derk Exp $
 *
 */
public interface DecisionEngine {
	/**
	 * Returns if this momar should process the job or not.
	 * 
	 * @return If this momar should process the job or not.
	 */
	public boolean processJob(Job job);
}
