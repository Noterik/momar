package com.noterik.springfield.momar.util;

/**
 * Interface for checking if a process is alive
 * 
 * @author Derk Crezee <d.crezee@noterik.nl>
 * @copyright Copyright: Noterik B.V. 2010
 * @package org.springfield.nelson.homer
 * @access private
 *
 */
public interface AliveStatus {
	/**
	 * Returns alive status
	 * 
	 * @return alive status
	 */
	public boolean alive();
}
