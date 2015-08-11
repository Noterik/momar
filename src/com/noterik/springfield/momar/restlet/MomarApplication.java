package com.noterik.springfield.momar.restlet;

import org.restlet.Application;

import org.restlet.Context;
import org.restlet.Restlet;

public class MomarApplication extends Application {	

    public MomarApplication() {
    	super();
    }
    
    public MomarApplication(Context parentContext) {
    	super(parentContext);
    }
    
    public void start(){
		try{
			super.start();
		}catch(Exception e){
			System.out.println("Error starting application");
			e.printStackTrace();
		}
	} 
    
    /**
	 * Called on shutdown
	 */
	public void stop() throws Exception {
		try {
			super.stop();
		} catch (Exception e) {
			System.out.println("momar: error stopping application");
			e.printStackTrace();
		}
	}

    @Override
    public Restlet createInboundRoot() {		
		return new MomarRestlet(super.getContext());
    }
}
