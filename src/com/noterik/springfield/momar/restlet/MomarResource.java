package com.noterik.springfield.momar.restlet;

import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.Delete;
import org.restlet.resource.Get;
import org.restlet.resource.Post;
import org.restlet.resource.Put;
import org.restlet.resource.ServerResource;

public class MomarResource extends ServerResource {
	/**
	 * Request uri
	 */
	protected String uri; 
	
	/**
	 * Sole constructor
	 * 
	 * @param context
	 * @param request
	 * @param response
	 */
	public MomarResource() {
		//constructor
	}
	
	
	public void doInit(Context context, Request request, Response response) {
        super.init(context, request, response);
        
        // add representational variants allowed
        getVariants().add(new Variant(MediaType.TEXT_XML));
        
        // get request uri
        uri = getRequestUri();
	}
	
	// allowed actions: POST, PUT, GET, DELETE 
	public boolean allowPut() {return true;}
	public boolean allowPost() {return true;}
	public boolean allowGet() {return true;}
	public boolean allowDelete() {return true;}
	
	/**
	 * GET
	 */
	@Get
    public void handleGet() {
		String responseBody = "GET: " + uri;
		Representation entity = new StringRepresentation(responseBody);
		getResponse().setEntity(entity);
	}
	
	/**
	 * PUT
	 */
	@Put
	public void handlePut(Representation representation) {
		String responseBody = "PUT: " + uri;
		Representation entity = new StringRepresentation(responseBody);
		getResponse().setEntity(entity);
	}
	
	/**
	 * POST
	 */
	@Post
	public void handlePost(Representation representation) {
		String responseBody = "POST: " + uri;
		Representation entity = new StringRepresentation(responseBody);
		getResponse().setEntity(entity);
	}
	
	/**
	 * DELETE
	 */
	@Delete
	public void handleDelete() {
		String responseBody = "DELETE: " + uri;
		Representation entity = new StringRepresentation(responseBody);
		getResponse().setEntity(entity);
	}
	
	/**
	 * Get request uri
	 * @return
	 */
	private String getRequestUri() {
		 // get uri
        String reqUri = getRequest().getResourceRef().getPath();
        reqUri = reqUri.substring(reqUri.indexOf("/",1));
        if(reqUri.endsWith("/")) {
        	reqUri = reqUri.substring(0,reqUri.length()-1);
        }
        return reqUri;
	}
}