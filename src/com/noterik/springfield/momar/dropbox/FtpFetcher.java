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

import com.noterik.springfield.momar.homer.*;
import com.noterik.springfield.tools.ftp.FtpHelper;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPListParseEngine;

public class FtpFetcher extends Thread {

	private String path = null;
	private String collection = null;
	private String fetcher_source = null;
	private static boolean running = false;
	private int maxfilesindropbox = 10;
	
	public FtpFetcher(String s,String c,String p) {
		path = p;
		fetcher_source = s;
		collection = c;
		if (!running) {
			running = true;
			start();
		}
	//	path = path +"_temp";
	}
	
	public void run() {
		while (running) {
			try {
				ArrayList<String> checklist = new ArrayList<String>();
				// first get the collection we check against
				System.out.println("FtpFetcher URL="+collection);
				String response = LazyHomer.sendRequest("GET",collection+"/presentation",null,null);
				try {
					// get all the md5 values to check against
					Document doc = DocumentHelper.parseText(response);
					for(Iterator<Element> iter = doc.getRootElement().elementIterator("presentation"); iter.hasNext(); ) {
						Element e = iter.next();
						Node p = e.selectSingleNode("properties/md5result");
						if (p!=null) {
							checklist.add(p.getText());
						}
					}
				} catch (Exception e) {
					System.out.println("Problem in FtpFetcher : Can't parse collection");
				}
				System.out.println("CHECKING AGAINST COLLECTION="+collection+" size="+checklist.size());
				// now we have the checklist so lets do the ftp query
				
				// find out the connect info
				String hostname = "";
				String ftppath = null;
				String account = null;
				String password = null;

				int pos = fetcher_source.indexOf("@");
				if (pos!=-1) {
					hostname = fetcher_source.substring(pos);
					int pos2 = hostname.indexOf("/");
					if (pos2!=-1) {
						ftppath = hostname.substring(pos2);
						hostname = hostname.substring(1,pos2);
					}
					// now get account and password
					account = fetcher_source.substring(0,pos);
					account = account.substring(6);
					pos2 = account.indexOf(":");
					if (pos2!=-1) {
						password = account.substring(pos2+1);
						account = account.substring(0,pos2);
					} else {
						System.out.println("Missing password in ftp url");
					}
				}
				
				System.out.println("HOSTNAME2="+hostname);
				System.out.println("FTPPATH="+ftppath);
				System.out.println("ACCOUNT="+account);
				System.out.println("PASSWORD="+password);	
				
				
				FTPClient client = new FTPClient();
				client.connect(hostname);
				boolean loggedin = client.login(account,password);
				if (!loggedin) { System.out.println("FtpServer: can not login : "+fetcher_source); return; }
				client.enterLocalPassiveMode();
				if (ftppath!=null) client.changeWorkingDirectory(ftppath);
				// loop through remote folder
				FTPListParseEngine engine = client.initiateListParsing();
				// loop through files
				boolean done = false;
				while(engine.hasNext() && !done) {
					FTPFile[] files = engine.getNext(10);
					for(FTPFile file : files) {
						if(!file.isDirectory()) {
							// do we need more ?
							if (localFileCount(path)<maxfilesindropbox) {
								if (file.getName().charAt(0)!='.') {
									System.out.println("downloading="+file.getName()+" C="+localFileCount(path));
									boolean success = FtpHelper.commonsGetFile(hostname, account, password, ftppath, path, file.getName(), file.getName()+".downloading");
									if (success) {
										File lfile = new File(path+File.separator+file.getName()+".downloading");
										if (lfile.exists()) {
											lfile.renameTo(new File(path+File.separator+file.getName()));
										}
									}
								}
							} else {
								done = true;
							}
						}
					}
				}
			
				sleep(5*1000);
			} catch(Exception e) {
				System.out.println("Problem in FtpFetcher");
				e.printStackTrace();
				try {
					sleep(5*1000);
				} catch(Exception e2) {}
			}
		}
	}
	
	private int localFileCount(String path) {
		File dir = new File(path);
		if (dir.isDirectory()) {
			return dir.listFiles().length;
		}
		return 0;
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
