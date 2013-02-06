/*
    This library is free software; you can redistribute it and/or modify it under
    the terms of the GNU Lesser General Public License as published by the Free Software
	Foundation; either version 2.1 of the License, or (at your option) any later version.

    This library is distributed in the hope that it will be useful, but WITHOUT 
    ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A 
    PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License along 
    with this library; if not, write to the 
    Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 
*/


package net.sourceforge.fractal.utils;

public class Pair<F,S> {

    public Pair(F first, S second) {

            this.first = first;
            this.second = second;
    }

    public F first;
    public S second;
    
    @Override
    public boolean equals(Object o){
    	if(!(o instanceof Pair)) return false;
    	if(o == this) return true;
    	return ((Pair)o).first.equals(first) && ((Pair)o).second.equals(second);
    }
}
