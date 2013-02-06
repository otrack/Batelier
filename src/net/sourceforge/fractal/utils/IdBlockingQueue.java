/*
Fractal is a light-weight group communication system. Copyright (C) 2005, Sprint Project.      This library is free software; you can redistribute it and/or modify it under     the terms of the GNU Lesser General Public License as published by the Free Software 	Foundation; either version 2.1 of the License, or (at your option) any later version.      This library is distributed in the hope that it will be useful, but WITHOUT      ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A      PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.      You should have received a copy of the GNU Lesser General Public License along      with this library; if not, write to the      Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 
file attached to it to see how you can use it, edit or distributed.
*/


package net.sourceforge.fractal.utils;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;

public class IdBlockingQueue<T> extends LinkedBlockingQueue<T> {
    private static final long serialVersionUID = -5782330316068504011L;
    String id = "undef";

    public IdBlockingQueue(String id) {
        super();
        this.id = id;
    }

    public IdBlockingQueue(Collection<T> arg0, String id) {
        super(arg0);
        this.id = id;
    }

    public IdBlockingQueue(int arg0, String id) {
        super(arg0);
        this.id = id;
    }

    public String getId() {
        return id;
    }
}
