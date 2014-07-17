package com.noterik.springfield.momar.restlet;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.restlet.Context;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.StringRepresentation;
import org.restlet.resource.Variant;

import com.noterik.springfield.momar.MomarServer;
import com.noterik.springfield.momar.homer.LazyHomer;

/**
 * The logging resource is used to configure the debugging behavior. 
 * The logging can be configured hierarchically. Logging inherited level
 * for a given logger is equal to that of its parent. So to configure
 * a single class use the full name, com.fu.Bar, and to configure a
 * package and every ancestor use the package name, com.fu
 * 
 * Resource uri:
 * 		/logging
 * 
 * Agruments:
 *  	level				The logging level you want to set (ALL,INFO,WARN,
 *  						DEBUG,TRACE,ERROR,FATAL,OFF) 
 *  	name (optional)		The full package, or classname you want to configure.
 *
 * @author Derk Crezee <d.crezee@noterik.nl>
 * @copyright Copyright: Noterik B.V. 2009
 * @package com.noterik.bart.fs.restlet
 * @access private
 * @version $Id: LoggingResource.java,v 1.2 2009-05-06 10:12:00 derk Exp $
 *
 */
public class LoggingResource extends Resource {

	/**
	 * Default constructor
	 * 
	 * @param context
	 * @param request
	 * @param response
	 */
	public LoggingResource(Context context, Request request, Response response) {
		super(context, request, response);
		
		// add representational variants allowed
        getVariants().add(new Variant(MediaType.TEXT_XML));
	}

	// allowed actions: GET 
	public boolean allowPut() {return false;}
	public boolean allowPost() {return false;}
	public boolean allowGet() {return true;}
	public boolean allowDelete() {return false;}
	
	/**
	 * GET
	 */
	@Override
    public Representation getRepresentation(Variant variant) {
		String responseBody = "";
		
		// get parameters
		Form qForm = getRequest().getResourceRef().getQueryAsForm();
		String name = qForm.getFirstValue("name",LazyHomer.PACKAGE_ROOT); // default root package
		String level = qForm.getFirstValue("level",null);
		
		// check level parameter
		if(level!=null) {
			// determine level
			Level logLevel = null;
			if(level.toLowerCase().equals("all")) {
				logLevel = Level.ALL;
			} else if(level.toLowerCase().equals("info")) {
				logLevel = Level.INFO;
			} else if(level.toLowerCase().equals("warn")) {
				logLevel = Level.WARN;
			} else if(level.toLowerCase().equals("debug")) {
				logLevel = Level.DEBUG;
			} else if(level.toLowerCase().equals("trace")) {
				logLevel = Level.TRACE;
			} else if(level.toLowerCase().equals("error")) {
				logLevel = Level.ERROR;
			} else if(level.toLowerCase().equals("fatal")) {
				logLevel = Level.FATAL;
			} else if(level.toLowerCase().equals("off")) {
				logLevel = Level.OFF;
			}
			
			// check level
			if(logLevel==null) {
				responseBody = "please provide log level: {ALL,INFO,WARN,DEBUG,TRACE,ERROR,FATAL,OFF}";
			} else {
				// set level
				Logger.getLogger(name).setLevel(logLevel);
				
				responseBody = "logging for " + name + " was set to " + logLevel.toString();
			}
		} else {
			// error message
			responseBody = "please provide log level: {ALL,INFO,WARN,DEBUG,TRACE,ERROR,FATAL,OFF}";
		}
		
		// return
		Representation entity = new StringRepresentation(responseBody);
        return entity;
	}
}
