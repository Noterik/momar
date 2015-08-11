package com.noterik.springfield.momar.dropbox;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.URL;
import java.net.URLConnection;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Iterator;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.Node;
import org.springfield.mojo.interfaces.ServiceInterface;
import org.springfield.mojo.interfaces.ServiceManager;

import com.noterik.springfield.momar.homer.*;
import com.sun.tools.internal.ws.wsdl.document.Documentation;

public class OAIFetcher extends Thread {

	private String path = null;
	private String collection = null;
	private String fetcher_source = null;
	private static boolean running = false;
	
	public OAIFetcher(String s,String c,String p) {
		path = p;
		fetcher_source = s;
		collection = c;
		if (!running) {
			running = true;
			//start();
		}
		fetcher_source="localhost:8080/all_openimages.xml";
		path = path +"_temp";
	}
	
	public void run() {
		while (running) {
			try {
				ArrayList<String> checklist = new ArrayList<String>();
				// first get the collection we check against
				System.out.println("URL="+collection);
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
					System.out.println("Problem in OAIFetcher : Can't parse collection");
				}
				System.out.println("CHECKING AGAINST COLLECTION="+collection+" size="+checklist.size());
				// now we have the checklist so lets do the OAI query
				
				String oairesult = "";
				
		        URL u = new URL("http://"+fetcher_source);
		        BufferedReader in = new BufferedReader(new InputStreamReader(u.openStream()));
		        String inputLine;
		        while ((inputLine = in.readLine()) != null) {
		            oairesult+=inputLine;
		        }
		        in.close();
		       // System.out.println("XML="+oairesult);
				
				Document oai = DocumentHelper.parseText(oairesult);
				//System.out.println("3XXX="+oai.asXML().substring(0,200));
				for(Iterator<Element> iter = oai.getRootElement().elementIterator("ListRecords"); iter.hasNext(); ) {
					//System.out.println("1XXXX="+iter.hashCode());
					Element n = (Element)iter.next();
					for(Iterator<Element> iter2 = n.elements().iterator(); iter2.hasNext(); ) {
						Element record = iter2.next();
						OAIRecord orec = new OAIRecord(); 
						for(Iterator<Element> iter3 = record.elements().iterator(); iter3.hasNext(); ) {
							Element prop = iter3.next();

							if (prop.getName().equals("header")) {
								for(Iterator<Element> iter4 = prop.elements().iterator(); iter4.hasNext(); ) {
									Element prop2 = iter4.next();

									if (prop2.getName().equals("identifier")) {
										orec.identifier = prop2.getText();
									}
								}
							} else if (prop.getName().equals("metadata")) {
								for(Iterator<Element> iter5 = prop.elements().iterator(); iter5.hasNext(); ) {
									Element prop2 = iter5.next();
									for(Iterator<Element> iter6 = prop2.elements().iterator(); iter6.hasNext(); ) {
										Element prop3 = iter6.next();
										//System.out.println("WOOO="+prop3.getName());
										if (prop3.getName().equals("title")) {
											orec.title = prop3.getText();
										}
										if (prop3.getName().equals("description")) {
											orec.description = prop3.getText();
										}
										if (prop3.getName().equals("format")) {
											orec.addFilename(prop3.getText());
										}
									}
								}
							}
						}
						String filepath = orec.getFilenameOfType("mp4");
						if (filepath!=null) {
							//System.out.println("FILEPATH="+filepath);
							int pos = filepath.lastIndexOf("/");
							if (pos!=-1) {
								String filename = filepath.substring(pos+1);
							//	System.out.println("FILENAME="+filename);
								String md5 = getMD5(filename);
								if (!checklist.contains(md5)) {
									getFileByHttp(filepath,filename,orec,path);
									// write xml record from orec
									// rename the filename we are done
					                File file = new File(path+File.separator+filename+".downloading");
					                if (file.exists()) {
					                	file.renameTo(new File(path+File.separator+filename));
					                }
									sleep(6*1000); // lets do one per min.
								} else {
									System.out.println("Already in collection : "+filepath);
								}
							}
						}
						
					}
				}
				sleep(5*1000);
			} catch(Exception e) {
				System.out.println("Problem in OAIFetcher");
				e.printStackTrace();
				try {
					sleep(5*1000);
				} catch(Exception e2) {}
			}
		}
	}
	
	private void getFileByHttp(String path,String filename,OAIRecord record,String dropbox) {
		System.out.println("SAVING : "+path+" TO DROPBOX :"+dropbox+" AS "+filename+".downloading");
		OutputStream out = null;
		URLConnection  con = null;

		InputStream in = null;
		try {
			URL Url;
			byte[] buf;
			int ByteRead;
			int ByteWritten=0;
			Url= new URL(path);
			out = new BufferedOutputStream(new FileOutputStream(dropbox+File.separator+filename+".downloading"));

			con = Url.openConnection();
			in = con.getInputStream();
			buf = new byte[10240];
			while ((ByteRead = in.read(buf)) != -1) {
				out.write(buf, 0, ByteRead);
				ByteWritten += ByteRead;
				System.out.print(".");
			}
			System.out.println("download done "+filename);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void performFileMethod() {
		/*
		ArrayList<String> checklist = new ArrayList<String>();
		// first get the collection we check against
		System.out.println("URL="+collection);
		String response = LazyHomer.sendRequest("GET",collection+"/presentation",null,null);
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
			System.out.println("DropboxObserver : Can't parse collection");
		}
		*/
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
	
	public void destroy() {
		running = false;
	}
}
