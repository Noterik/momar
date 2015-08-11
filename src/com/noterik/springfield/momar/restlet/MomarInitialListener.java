/* 
* MomarInitialListener.java
* 
* Copyright (c) 2015 Noterik B.V.
* 
* This file is part of momar-git, related to the Noterik Springfield project.
*
* momar-git is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* momar-git is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with momar-git.  If not, see <http://www.gnu.org/licenses/>.
*/
package com.noterik.springfield.momar.restlet;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.noterik.springfield.momar.homer.LazyHomer;

/**
 * MomarInitialListener.java
 *
 * @author Pieter van Leeuwen
 * @copyright Copyright: Noterik B.V. 2015
 * @package com.noterik.springfield.momar.restlet
 * 
 */
public class MomarInitialListener implements ServletContextListener {
	
	public void contextInitialized(ServletContextEvent event) {
		System.out.println("Momar: context created");
		
		ServletContext servletContext = event.getServletContext();
		
		//load LazyHomer		
		LazyHomer lh = new LazyHomer();
		lh.init(servletContext.getRealPath("/"));
	}
	
	public void contextDestroyed(ServletContextEvent event) {
		//destroy LazyHomer
		LazyHomer.destroy();		
		
		System.out.println("Momar: context destroyed");
	}
}
