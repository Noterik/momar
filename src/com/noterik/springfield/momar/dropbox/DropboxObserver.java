package com.noterik.springfield.momar.dropbox;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.Node;
import org.springfield.mojo.ftp.FtpHelper;
import org.springfield.mojo.interfaces.ServiceInterface;
import org.springfield.mojo.interfaces.ServiceManager;

import com.noterik.springfield.momar.homer.*;
import com.noterik.springfield.momar.tools.*;
import com.noterik.springfield.momar.util.Chmod;

public class DropboxObserver implements MargeObserver {
	/** The DropboxObserver log4j Logger */
	private static final Logger LOG = Logger.getLogger(DropboxObserver.class);
	
	private String id = null;
	private String path = null;
	private String collection = null;
	private String mount = null;
	private String imagemount = null;
	private String method = null;
	private String md5check = null;
	private String fetcher = null;
	private String fetcher_source = null;
	private Boolean active = false;
	private OAIFetcher oaiFetcher = null;
	private EasyFetcher easyFetcher = null;
	private FtpFetcher ftpFetcher = null;
	
	public DropboxObserver(Element dropbox) {
		try {
			//System.out.println("DR="+dropbox.asXML());
			id = dropbox.attributeValue("id");
			path = dropbox.selectSingleNode("properties/path").getText();
			collection = dropbox.selectSingleNode("properties/collection").getText();
			mount = dropbox.selectSingleNode("properties/mount").getText();
			imagemount = dropbox.selectSingleNode("properties/imagemount").getText();
			if (imagemount.equals("none")) imagemount = null; // ugly
			method = dropbox.selectSingleNode("properties/method").getText();
			md5check = dropbox.selectSingleNode("properties/md5check").getText();
			Node tmp = dropbox.selectSingleNode("properties/active");
			if (tmp!=null) {
				if (tmp.getText().equals("true")) {
					active = true;
				}
			}

			if (active) {
				LOG.info("ID="+id);
				LOG.info("PATH="+path);
				LOG.info("COLLECTION="+collection);
				LOG.info("MOUNT="+mount);
				LOG.info("ACTIVE="+active);
				LOG.info("METHOD=*"+method+"*");
				LOG.info("MD5CHECK="+md5check);
				LazyMarge.addTimedObserver("/domain/internal/triggers/dropbox/"+id,6,this);

				Node fn = dropbox.selectSingleNode("properties/fetcher");
				if (fn!=null) {
					fetcher = fn.getText();
					if (fetcher.equals("oai")) {
						fetcher_source = dropbox.selectSingleNode("properties/fetcher_source").getText();
						oaiFetcher = new OAIFetcher(fetcher_source,collection,path);
					} else if (fetcher.equals("easy")) {
						fetcher_source = dropbox.selectSingleNode("properties/fetcher_source").getText();
						easyFetcher = new EasyFetcher(fetcher_source,collection,path);
					} else if (fetcher.equals("ftp")) {
						fetcher_source = dropbox.selectSingleNode("properties/fetcher_source").getText();
						//new OAIFetcher(fetcher_source,collection,path);
						LOG.info("WANT TO START FTP");
						ftpFetcher = new FtpFetcher(fetcher_source,collection,path);
					}
				}
			}
		} catch(Exception e) {
			LOG.error("Missing or incorrect property in dropbox");
		}
	}
	
	public void remoteSignal(String from,String smethod,String url) {
		if (method.equals("file")) {
			performFileMethod();
		} else if (method.equals("presentationfile")) {
			performPresentationFileMethod();
		}
	}
	
	private void performPresentationFileMethod() {
		// scan directory
		File dir = new File(path);
		String[] files = dir.list();
		for (int i=0;i<files.length;i++) {
			String filename = files[i];
			if (filename.indexOf("presentation_")!=-1) {
				try {
					BufferedReader br = new BufferedReader(new FileReader(dir+File.separator+filename));
					//all variables from the file are stored in this map
					HashMap<String, String> vars = new HashMap<String, String>();
					String line = br.readLine();
					while (line != null) {
						int pos = line.indexOf("=");
						if (pos > -1) {
							vars.put(line.substring(0, pos).toLowerCase(), line.substring(pos+1));
						}						
						line = br.readLine();
					}
					br.close();
					
					String fileIdentifier = filename.substring(filename.indexOf("presentation_")+13, filename.length()-4);	
					LOG.info("FILE IDENTIFIER="+fileIdentifier);					
		
					// Do a check on the source files
					if(vars.get("files") != null) {
						String[] sources = vars.get("files").split(",");
						boolean process = checkSources(path,sources);
						LOG.info("PROCESS="+process);
						if(process) {
							performFixedIngest(vars, fileIdentifier);
							try {
								File f2 = new File(dir+File.separator+filename);
								f2.delete();
							} catch(Exception e2) {
								LOG.error("Can't delete file : "+dir+File.separator+filename);
							}
						}
					}
				} catch(Exception e) {
					LOG.error("Can't open file : "+dir+File.separator+filename);
				}
			}
		}
	}
	
	private void performFileMethod() {
		ArrayList<String> checklist = new ArrayList<String>();
		// first get the collection we check against
		ServiceInterface smithers = ServiceManager.getService("smithers");
		if (smithers==null) return;
		String response = smithers.get(collection+"/presentation",null,null);
		try {
			// get all the md5 values to check against
			Document doc = DocumentHelper.parseText(response);
			for(Iterator<Element> iter = doc.getRootElement().elementIterator("presentation"); iter.hasNext(); ) {
				Element e = iter.next();
				Node p = e.selectSingleNode("properties/md5result");
			//	System.out.println("NNNN="+p);
				if (p!=null) {
					checklist.add(p.getText());
				}
			}
		} catch (Exception e) {
			LOG.error("DropboxObserver : Can't parse collection");
		}

		// scan directory
		File dir = new File(path);
		System.out.println("SCAN="+dir);
		String[] files = dir.list();
		for (int i=0;i<files.length;i++) {
			String filename = files[i];
			String md5 = getMD5(filename);
			if (filename.indexOf(".downloading")!=-1 || checklist.contains(md5)) {
				System.out.println("OLD FILE="+filename+" MD5="+getMD5(filename));
				// should we clean it up ?
			} else {
				System.out.println("NEW FILE="+filename+" MD5="+getMD5(filename));
				performIngest(path,filename,md5);
			}
		}
	}
	
	private boolean performFixedIngest(HashMap<String, String> vars, String fileIdentifier) {
		String fdomain = vars.get("domain");
		String fuser = vars.get("user");
		String fcollection = vars.get("collection");
		String fpresentation = vars.get("presentation");
		String fvideo = vars.get("video");
		
		String[] filesarray = vars.get("files").split(",");
		String videoplaylistpart = "";
		
		//If a video is defined and we only have one file, add this file as raw
		if (fvideo != null && filesarray.length == 1) {
			String body = "<fsxml>";
			body += "<properties>";
			body+="<title>"+vars.get("title")+" ("+filesarray[0]+")</title>";
			body+="<restricted>"+vars.get("restricted")+"</restricted>";
			body+="<description>"+vars.get("description")+"</description>";
			body += "</properties>";
			body += "<rawvideo id='1'>";
			body+="<properties>";
			body+=addOriginalRawvideoProperties();
			body+="</properties>";
			body += "</rawvideo>";
			body += "</fsxml>";
			
			ServiceInterface smithers = ServiceManager.getService("smithers");
			if (smithers==null) return false;
			String result = smithers.put("/domain/"+fdomain+"/user/"+fuser+"/video/"+fvideo+"/properties",body,"text/xml");
			int pos = result.indexOf("/video/");
			if (pos==-1) return false; // something went wrong
		} else {		
			//otherwise just make new video nodes for every file
			for (int i=0;i<filesarray.length;i++) {
				String newbody = "<fsxml>";
				newbody+="<properties>";
				newbody+="<title>"+vars.get("title")+" ("+filesarray[i]+")</title>";
				newbody+="<restricted>"+vars.get("restricted")+"</restricted>";
				newbody+="<description>"+vars.get("description")+"</description>";
				newbody+="</properties>";
				newbody+="<rawvideo id=\"1\">";
				newbody+="<properties>";
				newbody+=addOriginalRawvideoProperties();
				newbody+="</properties>";
				newbody+="</rawvideo></fsxml>";
	
				ServiceInterface smithers = ServiceManager.getService("smithers");
				if (smithers==null) return false;
				String result = smithers.post("/domain/"+fdomain+"/user/"+fuser+"/video",newbody,"text/xml");
				int pos = result.indexOf("/video/");
				if (pos==-1) return false; // something went wrong
	
				// so video tag is made and inserted
				String code = result.substring(pos+7);
				pos = code.indexOf("<");
				code = code.substring(0,pos);
	
				// extract images
				if (imagemount!=null && !imagemount.equals("")) createAndMoveScreenshots("/domain/"+fdomain+"/user/"+fuser,path,filesarray[i],code);
	
				// move the file to the correct 
				moveToMount("/domain/"+fdomain+"/user/"+fuser,path,filesarray[i],code);
			    videoplaylistpart+="<video id=\""+(i+1)+"\" referid=\"/domain/"+fdomain+"/user/"+fuser+"/video/"+code+"\"><properties></properties></video>";
			}
		}
		
		// create presentation node		
		String newbody = "<fsxml>";
    	newbody+="<properties>";
    	newbody+="<title>"+vars.get("title")+"</title>";
    	newbody+="<description>"+vars.get("description")+"</description>";
    	newbody+="<md5result>"+fpresentation+"</md5result>"; // is urn in this case !!!
    	newbody+="<restricted>"+vars.get("restricted")+"</restricted>";
       	newbody+="</properties>";
    	newbody+="<videoplaylist id=\"1\">";
       	newbody+="<properties></properties>";
       	newbody+=videoplaylistpart;
       	newbody+="</videoplaylist></fsxml>";
       	ServiceInterface smithers = ServiceManager.getService("smithers");
		if (smithers==null) return false;
		String result = smithers.post("/domain/"+fdomain+"/user/"+fuser+"/presentation",newbody,"text/xml");
		int pos = result.indexOf("/presentation/");
		if (pos==-1) return false; // something went wrong
		String code = result.substring(pos+14);
		pos = code.indexOf("<");
		code = code.substring(0,pos);

		// post the collection/presentation node
		newbody= "<fsxml>";
		newbody+="<attributes>";
		newbody+="<referid>/domain/"+fdomain+"/user/"+fuser+"/presentation/"+code+"</referid>";
		newbody+="</attributes>";
    	newbody+="<properties>";
    	newbody+="</properties></fsxml>";
		result = smithers.post("/domain/"+fdomain+"/user/"+fuser+"/collection/"+fcollection+"/presentation",newbody,"text/xml");
       	String collectionPresentation = result.substring(result.indexOf("<uri>")+5, result.indexOf("</uri>"));
    	
    	// create dns entry
    	createDNSEntry(collectionPresentation, fileIdentifier, fdomain);
    	// now its all done lets also copy the other raws in and start the action
		//addProfileRawvideos(videourl);
		return false;
	}

	private void createDNSEntry(String entry, String identifier, String domain) {
		//for now only for dans
		if (domain.equals("dans")) {
			LOG.debug("DOMAIN = DANS");
			String newbody = "<fsxml>";
			newbody+="<properties>";
			newbody+="<refer>"+entry+"</refer>";
			newbody+="</properties>";
			newbody+="</fsxml>";
			String selector = "!da";
			LOG.debug("SEND TO /dns/"+selector+"/"+identifier);
			LOG.debug(newbody);
			ServiceInterface smithers = ServiceManager.getService("smithers");
			if (smithers==null) return;
			String response = smithers.put("/dns/"+selector+"/"+identifier, newbody, "text/xml");
		}
	}
	
	private boolean performIngest(String filepath,String filename,String md5) {
		// get the url to the user part
		int pos = collection.indexOf("/collection/");
		if (pos==-1) return false;
		String userurl = collection.substring(0,pos);
		System.out.println("PRE-URL="+userurl);

		// create the video/rawvideo part
		String newbody = "<fsxml>";
    	newbody+="<properties>";
    	newbody+="<title>title based on filename : "+filename+"</title>";
    	newbody+="<description>Default description for filename : "+filename+"</description>";
       	newbody+="</properties>";
    	newbody+="<rawvideo id=\"1\">";
       	newbody+="<properties>";
       	newbody+=addOriginalRawvideoProperties();
       	newbody+="</properties>";
    	newbody+="</rawvideo></fsxml>";
    	String videourl = userurl+"/video";
    	ServiceInterface smithers = ServiceManager.getService("smithers");
		if (smithers==null) return false;
		String result = smithers.post(videourl,newbody,"text/xml");
		//System.out.println("BLA="+result);
		pos = result.indexOf("/video/");
		if (pos==-1) return false; // something went wrong
		
		String code = result.substring(pos+7);
		pos = code.indexOf("<");
		code = code.substring(0,pos);
		//System.out.println("VCODE="+code);
		videourl+="/"+code+"/";
		
		// move the file to the correct mount
		moveToMount(userurl,filepath,filename,code);
		
		// create presentation node		
		newbody = "<fsxml>";
    	newbody+="<properties>";
    	newbody+="<title>"+filename+"</title>";
    	newbody+="<description>Default description for filename : "+filename+"</description>";
    	newbody+="<md5result>"+md5+"</md5result>";
       	newbody+="</properties>";
    	newbody+="<videoplaylist id=\"1\">";
       	newbody+="<properties></properties>";
///       	newbody+="<video id=\"1\" referid="><attributes>"+userurl+"/video/"+code+"</attributes><properties></properties></video>";
       	newbody+="<video id=\"1\" referid=\""+userurl+"/video/"+code+"\"><properties></properties></video>";

       	newbody+="</videoplaylist></fsxml>";
       	result = smithers.post(userurl+"/presentation",newbody,"text/xml");
		pos = result.indexOf("/presentation/");
		if (pos==-1) return false; // something went wrong
		code = result.substring(pos+14);
		pos = code.indexOf("<");
		code = code.substring(0,pos);
		//System.out.println("CODE="+code);
		// post the collection/presentation node
		newbody= "<fsxml>";
		newbody+="<attributes>";
		newbody+="<referid>"+userurl+"/presentation/"+code+"</referid>";
		newbody+="</attributes>";
    	newbody+="<properties>";
    	newbody+="</properties></fsxml>";
		smithers.post(collection+"/presentation",newbody,"text/xml");
		// now its all done lets also copy the other raws in and start the action
		addProfileRawvideos(videourl);

		return false;
	}
	
	private boolean checkSources(String path, String[] sources) {
		System.out.println("SOURCES LENGTH="+sources.length);
		if(sources.length<=0) return false;
		
		for(int i=0; i<sources.length; i++) {
			String sourceFile = sources[i].trim();
			File src = new File(path+File.separator+sourceFile);
			System.out.println("SOURCE="+sourceFile);
			if(!src.exists()) return false;
		}		
		return true;
	}

	private void addProfileRawvideos(String baseurl) {
		// needs to be copied from correct profiles in filesystem
		String body= "<fsxml>";
       	body += "<rawvideo id=\"2\"><properties>";
		body += "<audiocodec>mp4a</audiocodec>";
		body += "<wantedaudiobitrate>96k</wantedaudiobitrate>";
		body += "<wantedbitrate>800000</wantedbitrate>";
		body += "<height>720</height>";
		body += "<wantedkeyframerate>25</wantedkeyframerate>";
		body += "<wantedheight>720</wantedheight>";
		body += "<videobitrate>803437</videobitrate>";
		body += "<framerate>25.000</framerate>";
		body += "<wantedwidth>1280</wantedwidth>";
		body += "<audiochannels>1</audiochannels>";
		body += "<videocodec>H264</videocodec>";
		body += "<pixelaspect>1.7778</pixelaspect>";
		body += "<format>H.264</format>";
		body += "<wantedframerate>25</wantedframerate>";
		body += "<extension>mp4</extension>";
		body += "<reencode>true</reencode>";
		body += "<filesize>407606698</filesize>";
		body += "<duration>3666.48</duration>";
		body += "<samplerate>24000</samplerate>";
		body += "<mount>"+mount+"</mount>";
		body += "<transferred>false</transferred>";
		body += "<audiobitrate>86048</audiobitrate>";
		body += "<batchfile>html5_h264</batchfile>";
    	body+="</properties></rawvideo></fsxml>";
    	ServiceInterface smithers = ServiceManager.getService("smithers");
		if (smithers==null) return;
		String response = smithers.put(baseurl+"properties",body,"text/xml");
	}
		
	private String addOriginalRawvideoProperties() {
		String body = "<format>MP4</format>";
		body += "<extension>mp4</extension>";
		body += "<mount>"+mount+"</mount>";
		body += "<original>true</original>";
		return body;
	}

	private boolean createAndMoveScreenshots(String userpart,String filepath,String filename, String videoid) {
		System.out.println("MAKESCREENSHOTS="+imagemount);
		MountProperties mp = LazyHomer.getMountProperties(imagemount);
		System.out.println("MP="+mp.getProtocol()+" P="+mp.getPath());
		MomarProperties myp = LazyHomer.getMyMomarProperties();
		String tmpdir = myp.getTemporaryDirectory();
		
		Boolean ok = ScreenShotExtractor.extractScreenshots(filepath+File.separator+filename,tmpdir, "320x240", "1");
		if (ok) {
			if (mp.getProtocol().equals("file")) {				
				String fullpath = mp.getPath()+userpart+File.separator+"video"+File.separator+videoid+File.separator+"rawvideo"+File.separator+"1";
				ScreenShotExtractor.moveScreenshotsInCorrectDirs(tmpdir+File.separator, "1", fullpath);
			} else if (mp.getProtocol().equals("ftp")) {			
				String rFolder = mp.getPath()+userpart+File.separator+"video"+File.separator+videoid+File.separator+"rawvideo"+File.separator+"1";
				ok = ScreenShotExtractor.ftpScreenshotsInCorrectDirs(tmpdir+File.separator, "1",mp,rFolder);
			}
			//create video node
			createVideoNode(userpart+File.separator+"video"+File.separator+videoid, "http://dans.noterik.com/stills"+userpart+File.separator+"video"+File.separator+videoid, "1");
		}
		return true;
	}

	private boolean moveToMount(String userpart,String filepath,String filename, String videoid) {
		System.out.println("MOUNT="+mount);
		MountProperties mp = LazyHomer.getMountProperties(mount);
		System.out.println("MP="+mp.getProtocol()+" P="+mp.getPath());
		
		if (mp.getProtocol().equals("file")) {
			String fullpath = mp.getPath()+userpart+File.separator+"video"+File.separator+videoid+File.separator+"rawvideo"+File.separator+"1";
			System.out.println("FULLPATH="+fullpath);
            File dir = new File(fullpath);
            Boolean created = false;//dir.mkdirs();
            if (dir.exists()) {	
            	created = true;
            } else {
            	created = dir.mkdirs();
            }
            if (created) {
            	System.out.println("dir : "+fullpath+" created"); 
            	int result = Chmod.chmod(777, dir.getPath());
            	System.out.println("chmod dir = "+result);
                File file = new File(filepath+File.separator+filename);
                if (file.exists()) {
                	System.out.println("file exists "+filepath+File.separator+filename);
                	//appears to have troubles with using a shared mount
                	//from javadoc File.renameTo "The rename operation might not be able to move a file from one filesystem to another"
                	boolean success = file.renameTo(new File(fullpath+File.separator+"raw.mp4"));
                	System.out.println("moving file to "+fullpath+File.separator+"raw.mp4 result ="+success);
                } else {
                	System.out.println("file does not exists "+filepath+File.separator+filename);
                }
            } else {
               	System.out.println("dir : "+fullpath+" failed");        	
            }
		} else if (mp.getProtocol().equals("ftp")) {
			System.out.println("GET FTP PROPERTIES");
			mp = LazyHomer.getMountProperties(mount);
			String server = mp.getHostname();
			String username = mp.getAccount();
			String password = mp.getPassword();
			String rFolder = mp.getPath()+userpart+File.separator+"video"+File.separator+videoid+File.separator+"rawvideo"+File.separator+"1";
//			String lFolder = filepath+File.separator+filename;
			String lFolder = filepath;

			String rFilename = "raw.mp4";		
			System.out.println("ftp sending to server: "+server+", username: "+username+", password: "+password+", rFolder: "+rFolder+", lFolder: "+lFolder+", filename: "+rFilename);
			// send
			boolean ok = FtpHelper.commonsSendFile(server, username, password, rFolder, lFolder, rFilename, filename, true);
			return ok;
		}
		
		return true;
	}
	
	private String getMD5(String body) {
		try {
			MessageDigest m=MessageDigest.getInstance("MD5");
			m.update(body.getBytes(),0,body.length());
			String md5 = ""+new BigInteger(1,m.digest()).toString(16);
			return md5;
		} catch(Exception e) {
			System.out.println("Can't create md5");
			return null;
		}
	}
	
	private void createVideoNode(String path, String videoUri, String interval) {
		String body = "<fsxml><properties><size>320x240</size><interval>"+interval+"</interval><redo>false</redo>";
		body += "<uri>"+videoUri+"</uri><rawvideo>"+path+"/rawvideo/1</rawvideo></properties></fsxml>";
		
		ServiceInterface smithers = ServiceManager.getService("smithers");
		if (smithers==null) return;
		smithers.put(path+"/screens/1/properties",body,"text/xml");
	}
	
	public void destroy() {
		if (easyFetcher != null) {
			easyFetcher.destroy();
		}
		if (oaiFetcher != null) {
			oaiFetcher.destroy();
		}
		if (ftpFetcher != null) {
			ftpFetcher.destroy();
		}
	}
}