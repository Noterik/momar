package com.noterik.springfield.momar.restlet;

import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.StringRepresentation;
import org.restlet.resource.Variant;

public class MomarResource extends Resource {
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
	public MomarResource(Context context, Request request, Response response) {
        super(context, request, response);
        
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
	@Override
    public Representation getRepresentation(Variant variant) {
		String responseBody = "GET: " + uri;
		Representation entity = new StringRepresentation(responseBody);
        return entity;
	}
	
	/**
	 * PUT
	 */
	public void put(Representation representation) {
		String responseBody = "PUT: " + uri;
		Representation entity = new StringRepresentation(responseBody);
		getResponse().setEntity(entity);
	}
	
	/**
	 * POST
	 */
	public void post(Representation representation) {
		String responseBody = "POST: " + uri;
		Representation entity = new StringRepresentation(responseBody);
		getResponse().setEntity(entity);
	}
	
	/**
	 * DELETE
	 */
	public void delete() {
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