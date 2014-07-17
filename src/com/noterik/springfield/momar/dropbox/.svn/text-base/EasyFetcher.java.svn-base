package com.noterik.springfield.momar.dropbox;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Node;

import com.noterik.springfield.momar.MomarServer;
import com.noterik.springfield.momar.homer.*;
import com.noterik.springfield.momar.util.FsEncoding;

public class EasyFetcher extends Thread {
	/** The Easyfetcher log4j Logger */
	private static final Logger LOG = Logger.getLogger(EasyFetcher.class);
	private static final String DOMAIN = "dans";

	private String fetcher_source = null;
	private String path = null;
	private static boolean running = false;
	
	public EasyFetcher(String fetcher_source, String collection, String path) {
		this.fetcher_source = fetcher_source;
		this.path = path;
		if (!running) {
			running = true;
			start();
		}
		LOG.info("STARTING EASY FETCHER = "+fetcher_source+" p="+path);
	}
	
	public void run() {
		while (running) {			
			try {
				// now we have the checklist so lets check the files we have
				// scan directory set in fetcher_source
				File dir = new File(fetcher_source);
				String[] files = dir.list();
				for (int i=0;i<files.length;i++) {
					String filename = files[i];
					if (filename.indexOf(".xml")!=-1) {
						handleXMLFile(fetcher_source+File.separator+filename);
					}
				}
				//TODO: schedule this instead of running in infinite loop
				sleep(5 * 60 * 1000); // wait 5 minutes
				
				//check if we have the MomarServer still running
				if (MomarServer.instance().isRunning() == false) {
					running = false;
				}
			} catch(InterruptedException e) {
				LOG.error("Problem in EasyFetcher");
				e.printStackTrace();
				if (running) {
					try {
						sleep(5*1000); // wait 5 seconds
					} catch(Exception e2) {}
				} else {
					LOG.error("Shutting down Easyfetcher");
					//destroy called, just stop
				}
			}
		}
	}
	
	private void handleXMLFile(String filename) {
		LOG.debug("Handling file"+filename);
		try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			StringBuffer str = new StringBuffer();
			String line = br.readLine();
			while (line != null) {
				str.append(line);
				str.append("\n");
				line = br.readLine();
			}
			br.close();
			//get dom4j doc
			String content = str.toString();
			//workaround so we don't have to use the namespace in the dom4j queries
			content = content.replaceAll("xmlns=\"[^\"]*\"","");
			Document easyXml = DocumentHelper.parseText(content);
			
			// xsd specification can be found here:
			// http://easy.dans.knaw.nl/schemas/md/2013/06/avt.xsd
			List<Node> datasets = easyXml.selectNodes("/AVT/dataset-list/dataset");
			
			LOG.debug("dataset size for "+filename+" = "+datasets.size());
			
			for (Iterator<Node> it = datasets.iterator(); it.hasNext(); ) {
				Node dataset = (Node) it.next();
				parseDataSet(dataset);
			}			
			try {
				LOG.debug("Delete file "+filename);
				File f2 = new File(filename);
				f2.delete();
			} catch(Exception e2) {
				LOG.error("Can't delete file : "+filename);
			}
		} catch (Exception e){
			LOG.error("Can't read or parse easy xml file "+filename);
			e.printStackTrace();
		}
	}
	
	private void parseDataSet(Node dataset) {
		String datasetId = dataset.selectSingleNode("@storeId") == null ? "" : dataset.selectSingleNode("@storeId").getText();
		//datasetId = datasetId.substring(datasetId.lastIndexOf(":")+1);
		String datasetAction = dataset.selectSingleNode("@object-action") == null ? "" : dataset.selectSingleNode("@object-action").getText();
		String metadataAction = dataset.selectSingleNode("metadata/@list-action") == null ? "" : dataset.selectSingleNode("metadata/@list-action").getText();
		String datasetTitle = dataset.selectSingleNode("metadata/dc:title") == null ? "" : dataset.selectSingleNode("metadata/dc:title").getText();
		String datasetDescription = dataset.selectSingleNode("metadata/dc:description") == null ? "" : dataset.selectSingleNode("metadata/dc:description").getText();
		
		LOG.debug("user id = "+datasetId+" title = "+datasetTitle+" description = "+datasetDescription);
	
		String response = LazyHomer.sendRequest("GET","/domain/"+DOMAIN+"/user/"+datasetId, null, null);
		
		//parse response
		try {
			Document doc = DocumentHelper.parseText(response);
			boolean nodeExists = doc.selectSingleNode("/fsxml/user") == null ? false : true;
		
			if (datasetAction.equals("add") || metadataAction.equals("add")) {
				if (!datasetTitle.equals("") || !datasetDescription.equals("")) {
					//user should not exist
					if (!nodeExists) {
						String body = "<fsxml><properties><title>"+FsEncoding.encode(datasetTitle)+"</title><description>"+FsEncoding.encode(datasetDescription)+"</description></properties></fsxml>";
						LazyHomer.sendRequest("PUT", "/domain/"+DOMAIN+"/user/"+datasetId+"/properties", body, "text/xml");
					} else {
						LOG.error("dataset could not be added, exists already");
					}
				}
			} else if (datasetAction.equals("update") || metadataAction.equals("update")) {
				if (!datasetTitle.equals("") || !datasetDescription.equals("")) {
					//user should exist
					if (nodeExists) {
						String body = "<fsxml><properties><title>"+FsEncoding.encode(datasetTitle)+"</title><description>"+FsEncoding.encode(datasetDescription)+"</description></properties></fsxml>";
						LazyHomer.sendRequest("PUT", "/domain/"+DOMAIN+"/user/"+datasetId+"/properties", body, "text/xml");
					} else {
						LOG.error("dataset could not be updated, does not exist");
					}
				}
			} else if (datasetAction.equals("delete")) {
				//user should exist, otherwise done
				
				//TODO: should be done bottom up, so first delete video, presentation, collection and finally user
				//LazyHomer.sendRequest("DELETE", "/domain/"+DOMAIN+"/user/"+datasetId, null, null);
			} else {
				//check if it's there, if so continue
				if (!nodeExists) {
					return;
				}
				
			}
		} catch (DocumentException e) {
			LOG.error("Error parsing fs response in parseDataSet: "+e.toString());
			return;
		}
		
		List<Node> projects = dataset.selectNodes("project-list/project");
		//TODO: handle project-list @list-action
		
		for (Iterator<Node> it = projects.iterator(); it.hasNext(); ) {
			Node project = (Node) it.next();
			parseProject(project, datasetAction, datasetId);
		}
	}
	
	private void parseProject(Node project, String parentAction, String datasetId) {
		String projectId = project.selectSingleNode("@storeId") == null ? "" : project.selectSingleNode("@storeId").getText();
		//projectId = projectId.substring(projectId.lastIndexOf(":")+1);
		String projectAction = project.selectSingleNode("@object-action") == null ? "" : project.selectSingleNode("@object-action").getText();
		projectAction = projectAction.equals("") ? parentAction : projectAction;
		String metadataAction = project.selectSingleNode("metadata/@list-action") == null ? "" : project.selectSingleNode("metadata/@list-action").getText();
		String projectTitle = project.selectSingleNode("metadata/dc:title") == null ? "" : project.selectSingleNode("metadata/dc:title").getText();
		String projectDescription = project.selectSingleNode("metadata/dc:description") == null ? "" : project.selectSingleNode("metadata/dc:description").getText();
		
		LOG.debug("collection id = "+projectId+" action = "+projectAction+" title = "+projectTitle+" description = "+projectDescription);
		
		String response = LazyHomer.sendRequest("GET","/domain/"+DOMAIN+"/user/"+datasetId+"/collection/"+projectId, null, null);
		
		//parse response
		try {
			Document doc = DocumentHelper.parseText(response);
			boolean nodeExists = doc.selectSingleNode("/fsxml/collection") == null ? false : true;
			
			if (projectAction.equals("add") || metadataAction.equals("add")) {
				if (!projectTitle.equals("") || !projectDescription.equals("")) {
					//collection should not exist
					if (!nodeExists) {
						String body = "<fsxml><properties><title>"+FsEncoding.encode(projectTitle)+"</title><description>"+FsEncoding.encode(projectDescription)+"</description></properties></fsxml>";
						LazyHomer.sendRequest("PUT", "/domain/"+DOMAIN+"/user/"+datasetId+"/collection/"+projectId+"/properties", body, "text/xml");
					} else {
						LOG.error("dataset could not be added, exists already");
					}
				}
			} else if (projectAction.equals("update") || metadataAction.equals("update")) {
				if (!projectTitle.equals("") || !projectDescription.equals("")) {
					//collection should exist
					if (nodeExists) {
						String body = "<fsxml><properties><title>"+FsEncoding.encode(projectTitle)+"</title><description>"+FsEncoding.encode(projectDescription)+"</description></properties></fsxml>";
						LazyHomer.sendRequest("PUT", "/domain/"+DOMAIN+"/user/"+datasetId+"/collection/"+projectId+"/properties", body, "text/xml");
					} else {
						LOG.error("dataset could not be updated, does not exist");
					}
				}
			} else if (projectAction.equals("delete")) {
				//collection should exist, otherwise done
				
				//TODO: should be done bottom up, so first delete video, presentation, collection and finally user
				//LazyHomer.sendRequest("DELETE", "/domain/"+DOMAIN+"/user/"+datasetId, null, null);
			} else {
				//check if it's there, if so continue
				if (!nodeExists) {
					return;
				}
			}
		} catch (DocumentException e) {
			LOG.error("Error parsing fs response in parseDataSet: "+e.toString());
			return;
		}
		
		List<Node> presentations = project.selectNodes("presentation-list/presentation");
		
		//TODO: handle presentation-list @list-action
		
		for (Iterator<Node> it = presentations.iterator(); it.hasNext(); ) {
			Node presentation = (Node) it.next();
			parsePresentation(presentation, projectAction, datasetId, projectId);
		}
	}
	
	private void parsePresentation(Node presentation, String parentAction, String datasetId, String projectId) {
		String presentationId = presentation.selectSingleNode("@storeId") == null ? "" : presentation.selectSingleNode("@storeId").getText();
		//presentationId = presentationId.substring(presentationId.lastIndexOf(":")+1);
		String presentationAction = presentation.selectSingleNode("@object-action") == null ? "" : presentation.selectSingleNode("@object-action").getText();
		presentationAction = presentationAction.equals("") ? parentAction : presentationAction;
		String metadataAction = presentation.selectSingleNode("metadata/@list-action") == null ? "" : presentation.selectSingleNode("metadata/@list-action").getText();
		String presentationTitle = presentation.selectSingleNode("metadata/dc:title") == null ? "" : presentation.selectSingleNode("metadata/dc:title").getText();
		String presentationDescription = presentation.selectSingleNode("metadata/dc:description") == null ? "" : presentation.selectSingleNode("metadata/dc:description").getText();
		String confinement = presentation.selectSingleNode("confinement") == null ? "restricted" : presentation.selectSingleNode("confinement").getText();

		LOG.debug("presentation id = "+presentationId+" access level = "+confinement+" title = "+presentationTitle+" description = "+presentationDescription);
		
		String response = LazyHomer.sendRequest("GET","/domain/"+DOMAIN+"/user/"+datasetId+"/presentation/"+presentationId, null, null);
		
		//parse response
		try {
			Document doc = DocumentHelper.parseText(response);
			boolean nodeExists = doc.selectSingleNode("/fsxml/presentation") == null ? false : true;
		
			if (presentationAction.equals("add") || metadataAction.equals("add")) {
				if (!presentationTitle.equals("") || !presentationDescription.equals("")) {
					//presentation should not exist					
					if (!nodeExists) {
						String body = "<fsxml><properties><title>"+FsEncoding.encode(presentationTitle)+"</title><description>"+FsEncoding.encode(presentationDescription)+"</description></properties><videoplaylist id='1'><properties/></videoplaylist></fsxml>";
						String resp = LazyHomer.sendRequest("PUT", "/domain/"+DOMAIN+"/user/"+datasetId+"/presentation/"+presentationId+"/properties", body, "text/xml");
							
						//make refer in collection to presentation
						Document respDoc = DocumentHelper.parseText(resp);
						String pRefer = respDoc.selectSingleNode("/status/properties/uri") == null ? "" : respDoc.selectSingleNode("/status/properties/uri").getText();
						LOG.debug("presentation uri = "+pRefer);
						String attributesXml = "<fsxml><attributes><referid>"+pRefer+"</referid></attributes></fsxml>";
						//post to add
						String respo = LazyHomer.sendRequest("PUT", "/domain/"+DOMAIN+"/user/"+datasetId+"/collection/"+projectId+"/presentation/"+presentationId+"/attributes", attributesXml, "text/xml");
						LOG.debug("collection presentation refer response = "+respo);
					} else {
						LOG.error("dataset could not be added, exists already");
					}
				}
			} else if (presentationAction.equals("update") || metadataAction.equals("update")) {
				if (!presentationTitle.equals("") || !presentationDescription.equals("")) {
					//presentation should exist
					if (nodeExists) {
						String body = "<fsxml><properties><title>"+FsEncoding.encode(presentationTitle)+"</title><description>"+FsEncoding.encode(presentationDescription)+"</description></properties></fsxml>";
						String resp3 = LazyHomer.sendRequest("PUT", "/domain/"+DOMAIN+"/user/"+datasetId+"/presentation/"+presentationId+"/properties", body, "text/xml");
						LOG.debug("updating presentation result = "+resp3);
					} else {
						LOG.error("dataset could not be updated, does not exist");
					}
				}
			} else if (presentationAction.equals("delete")) {
				//presentation should exist, otherwise done
				
				//TODO: should be done bottom up, so first delete video, presentation, collection and finally user
				//LazyHomer.sendRequest("DELETE", "/domain/"+DOMAIN+"/user/"+datasetId, null, null);
			} else {
				//check if it's there, if so continue
				if (!nodeExists) {
					return;
				}
			}
		} catch (DocumentException e) {
			LOG.error("Error parsing fs response in parseDataSet: "+e.toString());
			return;
		}
		
		List<Node> videos = presentation.selectNodes("reel-list/reel");
		
		//TODO: handle reel-list @list-action
		
		for (Iterator<Node> it = videos.iterator(); it.hasNext(); ) {
			Node video = (Node) it.next();
			parseVideo(video, presentationAction, datasetId, projectId, presentationId, confinement, presentationTitle, presentationDescription);
		}	
	}
	
	private void parseVideo(Node video, String parentAction, String datasetId, String projectId, String presentationId, String confinement, String pTitle, String pDesc) {
		String videoId = video.selectSingleNode("@storeId") == null ? "" : video.selectSingleNode("@storeId").getText();
		//videoId = videoId.substring(videoId.lastIndexOf(":")+1);
		String videoAction = video.selectSingleNode("@object-action") == null ? "" : video.selectSingleNode("@object-action").getText();
		videoAction = videoAction.equals("") ? parentAction : videoAction;
		String metadataAction = video.selectSingleNode("metadata/@list-action") == null ? "" : video.selectSingleNode("metadata/@list-action").getText();

		String format = video.selectSingleNode("metadata/dc:format") == null ? "" : video.selectSingleNode("metadata/dc:format").getText();
		String filename = video.selectSingleNode("file-name") == null ? "" : video.selectSingleNode("file-name").getText();
		
		LOG.debug("video id = "+videoId+" video format = "+format+" video filename = "+filename);
		
		//handle video
		String response = LazyHomer.sendRequest("GET","/domain/"+DOMAIN+"/user/"+datasetId+"/video/"+videoId, null, null);
		
		//parse response
		try {
			Document doc = DocumentHelper.parseText(response);
			boolean nodeExists = doc.selectSingleNode("/fsxml/video") == null ? false : true;
		
			if (videoAction.equals("add") || metadataAction.equals("add")) {
				//video should not exist				
				if (!nodeExists) {
					String body = "<fsxml><properties/></fsxml>";
					String resp = LazyHomer.sendRequest("PUT", "/domain/"+DOMAIN+"/user/"+datasetId+"/video/"+videoId+"/properties", body, "text/xml");
					LOG.debug("add video response = "+resp);
					
					Document respDoc = DocumentHelper.parseText(resp);
					String vRefer = respDoc.selectSingleNode("/status/properties/uri") == null ? "" : respDoc.selectSingleNode("/status/properties/uri").getText();					
					LOG.debug("video location = "+vRefer);
					String attributesXml = "<fsxml><attributes><referid>"+vRefer+"</referid></attributes></fsxml>";
					//make refer in presentation videoplaylist to this video
					String resp1 = LazyHomer.sendRequest("PUT", "/domain/"+DOMAIN+"/user/"+datasetId+"/presentation/"+presentationId+"/videoplaylist/1/video/1/attributes", attributesXml, "text/xml");
					LOG.debug("presentation videoplaylist refer = "+resp1);
					//make refer in collection to this video
					String resp2 = LazyHomer.sendRequest("POST", "/domain/"+DOMAIN+"/user/"+datasetId+"/collection/"+projectId+"/video", attributesXml, "text/xml");
					LOG.debug("collection video refer  ="+resp2);
				} else {
						LOG.error("dataset could not be added, exists already");
				}
			} else if (videoAction.equals("update") || metadataAction.equals("update")) {
				//video should exist
				if (nodeExists) {
					//don't handle as for now we don't store any of the video information
				} else {
					LOG.error("dataset could not be updated, does not exist");
				}
			} else if (videoAction.equals("delete")) {
				//presentation should exist, otherwise done
				
				//TODO: should be done bottom up, so first delete video, presentation, collection and finally user
				//LazyHomer.sendRequest("DELETE", "/domain/"+DOMAIN+"/user/"+datasetId, null, null);
			} else {
				//check if it's there, if so continue
				if (!nodeExists) {
					return;
				}
			}
		} catch (DocumentException e) {
			LOG.error("Error parsing fs response in parseDataSet: "+e.toString());
			return;
		}
		
		//add pfile for further processing by momar
		String outname = path+File.separator+"presentation_"+presentationId+".txt";
		File filecheck = new File(outname);
		if (!filecheck.exists()) {
			LOG.info("NEED TO WRITE PRESENTATIONFILE presentation_"+presentationId);
			String pfile = "DOMAIN=dans\n";
			pfile += "USER="+datasetId+"\n";
			pfile += "COLLECTION="+projectId+"\n";
			pfile += "PRESENTATION="+presentationId+"\n";
			pfile += "FILES="+filename+"\n";
			pfile += "RESTRICTED="+confinement+"\n";
			pfile += "TITLE="+FsEncoding.encode(pTitle)+"\n";
			pfile += "DESCRIPTION="+FsEncoding.encode(pDesc)+"\n";
			pfile += "VIDEO="+videoId+"\n";
			try {
				PrintWriter out = new PrintWriter(outname);
				out.println(pfile);
				out.close();
			} catch(Exception e) {
				LOG.error("Error writing file = "+outname);
			}
		}		
	}
	
	public void destroy() {
		running = false;
		Thread.currentThread().interrupt();
	}
}
