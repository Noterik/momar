
package com.noterik.springfield.momar.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class Chmod {

	/**
	 * Changes the rights of the file/dir to 774
	 *
	 * 0: 	no problems
	 * >0:	chmod encountered problems to execute the command
	 * -1:	methods default value (for instance returned when the Runtime.getRuntime().exc() failed
	 */
	public static final Integer chmodDefault(String path) {
		if (path.lastIndexOf("/") == -1) {
			throw new IllegalArgumentException("Not a UNIX path like string! + " + path);
		}
		return Chmod.chmod(774, path);
		
	}
	/**
	 * Changes the file/dir permissions/rights
	 * 
	 * @param path	file/dir whose permission/rights you wanna change
	 * @param octa	any kind of rights (for example: 764)
	 * 
	 * @return	RetVal of the chmod execution
	 *  
	 * 			-1: this methods default return value indicating something went wrong
	 * 			 0: chmod execution was successful
	 */
	public static final Integer chmod(Integer octa, String path) {
		Integer retVal = -1;
		Process proc = null;
		try {
			System.out.println("Command to execute: chmod " + octa + " " + path);
			proc = Runtime.getRuntime().exec("chmod " + octa + " " + path);
			
	        BufferedReader br = null; 
	        String currentInput = null;
            try {
            	br = new BufferedReader(new InputStreamReader(proc.getInputStream()));
	            while ( (currentInput = br.readLine()) != null) {
	                System.out.println(currentInput);
	            }
            }
            catch (Exception e) {
            	System.out.println("Error during reading inputStream");
            }
            finally { 
            	if (br != null)
	            	try {
	            			br.close(); 
	            		}
	            	catch (Exception e) {
	            		//silently
	            	}
            }
	        
			retVal = proc.waitFor();
		} catch (IOException e) {
			// thrown by process getRuntime()
			e.printStackTrace();
		} catch (InterruptedException e) {
			// thrown by proc.waitFor()
			e.printStackTrace();
		}
		finally {
			if (proc != null)
				try {
					proc.getErrorStream().close();
					proc.getInputStream().close();
					proc.getOutputStream().close();
				}
				catch (Exception e) {
					//silently closing
				}
				
		}
		return retVal;
	}
	
	public static void main(String[] args) {
		System.out.println("******************************************************");
		System.out.println("Usage:: java Chmod path octa");
		System.out.println("for example: java Chmod 764 /usr/home/rene/blabla.jpg ");
		System.out.println("******************************************************");
		
		if (args.length != 2) {
			System.out.println("No correct argument size");
			System.exit(-1);
		}
		if (args[1].lastIndexOf("/") == -1) {
			System.out.println("Not a UNIX path like string (\"/\" missing) : " + args[1]);
			System.exit(-1);
		}
		Integer chmod = null;
		
		try { 
			chmod = Integer.parseInt(args[0]);
		}
		catch(NumberFormatException e) {
			System.out.println("chmod is not an Integer number" + args[0]);
			System.exit(-1);
		}
		//we should check if the number is a correct number for settings rights/permissions
		
		System.out.println("retval: " + Chmod.chmod(chmod,args[1]));
		System.out.println("***END***");
	}
	
	
	
}