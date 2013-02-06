/*
Fractal is a light-weight group communication system. Copyright (C) 2005, Sprint Project.    
This library is free software; you can redistribute it and/or modify it under     
the terms of the GNU Lesser General Public License as published by the Free Software 	
Foundation; either version 2.1 of the License, or (at your option) any later version.
This library is distributed in the hope that it will be useful, but WITHOUT      
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A      
PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.      
You should have received a copy of the GNU Lesser General Public License along      
with this library; if not, write to the Free Software Foundation, Inc.,
59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 
file attached to it to see how you can use, edit or distribute it.
 */

package net.sourceforge.fractal.replication.database;

import net.sourceforge.fractal.MutedStream;


/**
 * 
 * @author Nicolas Schiper
 * @author Pierre Sutra
 *
 */

public abstract class CertificationProtocol  extends MutedStream {

	
	protected PSTORE pstore;
	
	
	public CertificationProtocol(PSTORE db) {
		pstore=db;
	}
		
	public abstract void submit(TpcbTransaction t);
	
	protected void commit(TpcbTransaction t){
		pstore.commit(t);
	}
	
	protected void abort(TpcbTransaction t){
		pstore.abort(t);
	}
	
}
