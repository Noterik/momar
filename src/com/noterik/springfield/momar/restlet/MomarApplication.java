package com.noterik.springfield.momar.restlet;

import javax.servlet.ServletContext;

import org.restlet.Application;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.Restlet;

import com.noelios.restlet.ext.servlet.ServletContextAdapter;
import com.noterik.springfield.momar.homer.LazyHomer;

public class MomarApplication extends Application {	
	private static LazyHomer lh = null; 
	
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
		lh.destroy();
	}

    @Override
    public Restlet createRoot() {
    	// set rootpath and return restlet
    	ServletContextAdapter adapter = (ServletContextAdapter) getContext();
		ServletContext servletContext = adapter.getServletContext();
		
		lh = new LazyHomer();
		lh.init(servletContext.getRealPath("/"));
		
		// disable logging
		Component component = (Component)servletContext.getAttribute("com.noelios.restlet.ext.servlet.ServerServlet.component");
		component.getLogService().setEnabled(false);
		
		return new MomarRestlet(super.getContext());
    }
}
