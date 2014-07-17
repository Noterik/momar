package com.noterik.springfield.momar.restlet;

import org.restlet.Context;
import org.restlet.Router;

public class MomarRestlet extends Router {	
	public MomarRestlet(Context cx) {
		super(cx);
		
		// logging resource
		this.attach("/logging",LoggingResource.class);
		
		// default resource
		this.attach("/",MomarResource.class);
	}
}
