package com.noterik.springfield.momar.queue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.Node;

import com.noterik.bart.marge.model.Service;
import com.noterik.bart.marge.server.MargeServer;
import com.noterik.springfield.momar.homer.LazyHomer;
import com.noterik.springfield.tools.fs.FSXMLBuilder;
import com.noterik.springfield.tools.fs.URIParser;

/**
 * Container for jobs
 *
 * @author Derk Crezee <d.crezee@noterik.nl>
 * @author Daniel Ockeloen <daniel@noterik.nl>
 * @copyright Copyright: Noterik B.V. 2012
 * @package com.noterik.springfield.momar.queue
 * @access private
 * @version $Id: Job.java,v 1.31 2012-08-02 04:16:38 daniel Exp $
 *
 * TODO: optimize getOriginal and getInput (video xml is requested twice)
 */
public class Job {
	/** the Job's log4j logger */
	private static final Logger LOG = Logger.getLogger(Job.class);
	
	/**
	 * uri
	 */
	private String uri;
	
	/**
	 * uri to original raw video
	 */
	private String original;
	
	/**
	 * filename of original raw video
	 */
	private String originalFilename;
	
	/**
	 * uri of video to transcode
	 */
	private String inputURI;
	
	/**
	 * filename of video to transcode
	 */
	private String inputFilename;
	
	/**
	 * properties
	 */
	private Map<String,String> properties;
	
	/**
	 * jobsid
	 */
	private String id;
	
	/**
	 * path of the video 
	 */
	private String outputURI;
	
	/**
	 * output filename
	 */
	private String outputFilename;
	
	
	/**
	 * Default constructor
	 * 
	 * @param uri the uri of this job
	 * @param properties the properties of this job
	 */
	public Job(String uri, Map<String,String> properties) {
		this.uri = uri;
		this.properties = properties;
	}
	
	/**
	 * Constructor that parses the document for all the properties
	 * @param uri job uri
	 * @param xml xml document containing all the job properties
	 * @throws DocumentException 
	 */
	public Job(String uri, String xml) throws DocumentException {
		this(uri,new HashMap<String,String>());
		
		// create document
		Document doc = DocumentHelper.parseText(xml);
		
		id = doc.getRootElement().attributeValue("id");
		
		// parse properties
		Node pNode = doc.selectSingleNode("//rawvideo/properties");
		if(pNode!=null) {
			List<Node> children = pNode.selectNodes("child::*");
			Node child;
			for(Iterator<Node> iter = children.iterator(); iter.hasNext(); ) {
				child = iter.next();
				properties.put(child.getName(), child.getText());
			}
		}
		
		// add referid
		String referid = doc.valueOf("//rawvideo/@referid");
		if(referid!=null) {
			properties.put("referid", referid);
		}
		
		// parse parent XML
		parseParentXML();
	}
	
	/**
	 * Parse the XML of the parent URI to determine original and input video
	 */
	private void parseParentXML() {
		LOG.debug("parsing parent XML for job "+uri);
			
		// get some variables
		String referid = getProperty("referid");
		String parentURI = URIParser.getParentUri(referid);
		String useraw = getProperty("useraw");
		
		// get all the raw videos
		LOG.debug("sending get request to: " + parentURI);
		
		String response = LazyHomer.sendRequest("GET", parentURI, null, null);

		LOG.debug("response was: " + response);
		
		// parse document
		try {
			Document doc = DocumentHelper.parseText(response);
			
			// determine original
			Node oNode = doc.selectSingleNode("//rawvideo/properties[original='true']");
			if(oNode!=null) {
				// construct original path and filename
				original = parentURI + "/rawvideo/" + oNode.getParent().valueOf("@id");
				originalFilename = "raw." + oNode.valueOf("extension");
			} else {
				oNode = doc.selectSingleNode("//rawvideo/properties[contains(original,'/domain/')]");
				if (oNode != null) {
					//get original refered video for original path and filename
					String resp = LazyHomer.sendRequest("GET", oNode.valueOf("original"), null, null);
					Document oDoc = DocumentHelper.parseText(resp);
					String filename = oDoc.selectSingleNode("//rawvideo/properties/filename") == null ? "" : oDoc.selectSingleNode("//rawvideo/properties/filename").getText();
					original = filename.substring(0, filename.lastIndexOf("/")+1);
					originalFilename = filename.substring(filename.lastIndexOf("/")+1);

					filename = doc.selectSingleNode("//rawvideo/properties/filename") == null ? "raw." + oNode.valueOf("extension") : doc.selectSingleNode("//rawvideo/properties/filename").getText();
					outputURI = filename.substring(0, filename.lastIndexOf("/")+1);
					outputFilename = filename.substring(filename.lastIndexOf("/")+1);
				} else {
					LOG.debug("could not find original");
				}
			}
			
			// determine input URI and filename
			if(useraw==null) {
				inputURI = original;
				inputFilename = originalFilename;
			} else {
				Node inpNode = doc.selectSingleNode("//rawvideo[@id='"+useraw+"']/properties");
				if(inpNode!=null) {
					// construct original path and filename
					inputURI = parentURI + "/rawvideo/" + inpNode.getParent().valueOf("@id");
					inputFilename = "raw." + inpNode.valueOf("extension");
				}
			}
		} catch(Exception e) {
			LOG.error("",e);
		}
		
		LOG.debug("Result of parsing parent - original: "+original+", originalFilename: "+originalFilename+", inputURI: "+inputURI+", inputFilename: "+inputFilename);

	}

	/**
	 * Get single property from the properties
	 * 
	 * @param property
	 * @return property, null if property does not exist
	 */
	public String getProperty(String property) {
		return properties.get(property);
	}
	
	/**
	 * Add status message to this job
	 * @param message
	 * @param details
	 */
	public void setStatus(String message, String details) {
		// make xml
		String statusXml = FSXMLBuilder.getFSXMLStatusMessage(message, details, "");
		
		LazyHomer.sendRequest("PUT",uri + "/status/1/properties",statusXml,"text/xml");
		
		// debug
		LOG.debug("Settings status for " + uri + "/status/1/properties");
		LOG.debug(statusXml);
		
	}

	
	/**
	 * Add an error message to this job
	 * 
	 * @param message
	 * @param details
	 */
	public void setError(String message, String details) {
		
		// get the uri of the rawvideo
		String rawUri = this.getProperty("referid");
		
		// make xml
		String errorXml = FSXMLBuilder.getFSXMLErrorMessage("500",message,details, rawUri);
			
		// set error message
		LazyHomer.sendRequest("PUT",uri + "/error/1/properties",errorXml,"text/xml");
		
		// debug
		LOG.debug("Settings status for " + uri + "/error/1/properties");
		LOG.debug(errorXml);
	}

	/**
	 * @return the uri
	 */
	public String getUri() {
		return uri;
	}

	/**
	 * @return the original
	 */
	private String getOriginal() {
		return original;
	}
	
	/**
	 * @return the job id
	 */
	public String getId() {
		return id;
	}

	/**
	 * @return the originalFilename
	 */
	private String getOriginalFilename() {
		return originalFilename;
	}

	/**
	 * @return the inputURI
	 */
	public String getInputURI() {
		return inputURI;
	}

	/**
	 * @return the inputFilename
	 */
	public String getInputFilename() {
		return inputFilename;
	}
	
	/**
	 * @return the outputURI
	 */
	public String getOutputURI() {
		return outputURI;
	}
	
	/**
	 * @return the outputFilename
	 */
	public String getOutputFilename() {
		return outputFilename;
	}
	
	/**
	 * toString method.
	 */
	@Override
	public String toString() {
		return properties.toString();
	}
	
	/**
	 * Equals function
	 */
	@Override
	public boolean equals(Object o){
		if(o != null && o instanceof Job){
			if(((Job)o).getUri() != null){
				if(((Job)o).getUri().equals(uri)){
					return true;
				}
			}
		}
		return false;
	}
	
	/**
	 * 
	 * @param property
	 * @return the value of the property, null if the property did not exists or an empty string if there was an error
	 */
	public String getStatusProperty(String property) {
		String response = LazyHomer.sendRequest("GET",uri + "/status/1/properties/"+property,null,null);

		LOG.debug("Get status property "+property+" response "+response);
		try { 
			Document xml = DocumentHelper.parseText(response);			
			return xml.selectSingleNode(property) == null ? null : xml.selectSingleNode(property).getText();
		} catch (DocumentException e) {
			LOG.error("could not parse response "+response);
		}
		return "";
	}

	/**
	 * 
	 * @param property
	 * @return the value of the property, null if the property did not exists or an empty string if there was an error
	 */
	public ArrayList<String> getStatusProperties() {
		ArrayList<String> results =new ArrayList<String>();
		
		String response = LazyHomer.sendRequest("GET",uri + "/status/1/properties",null,null);

		try { 
			Document doc = DocumentHelper.parseText(response);	
			Node pNode = doc.selectSingleNode("//status/properties");
			if(pNode!=null) {
				List<Node> children = pNode.selectNodes("child::*");
				Node child;
				for(Iterator<Node> iter = children.iterator(); iter.hasNext(); ) {
					child = iter.next();
					results.add(child.getName()+"="+child.getText());
				}
			}

		} catch (DocumentException e) {
			LOG.error("could not parse response "+response);
		}
		return results;
	}

	
	public void setStatusProperty(String property, String value) {
		// set status message
		LazyHomer.sendRequest("PUT", uri + "/status/1/properties/"+property, value, "text/xml");
		
		// debug
		LOG.debug("Settings status for " + uri + "/status/1/properties/"+property);
		LOG.debug(value);
	}


}
