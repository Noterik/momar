package com.noterik.springfield.momar.dropbox;

import java.util.ArrayList;

public class OAIRecord {
	public String identifier;
	public String title;
	public String description;
	public ArrayList<String> filenames = new ArrayList<String>();
	
	public void addFilename(String f) {
		filenames.add(f);
	}
	
	public String getFilenameOfType(String type) {
		for (int i=0;i<filenames.size();i++) {
			String fn = filenames.get(i);
			if (fn.indexOf("."+type)!=-1) {
				return fn;
			}
		}
		return null;
	}
}
