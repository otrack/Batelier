package net.sourceforge.fractal.utils;


import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public class IPUtils {
    
	private static final String IPADDRESS_PATTERN = 
		"^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
		"([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
		"([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
		"([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";
	
    public static Set<String> getLocalIpAddresses(){
        Set<String> result = new HashSet<String>();
        Enumeration<NetworkInterface> e1 = null;
        try {
            e1 = (Enumeration<NetworkInterface>)NetworkInterface.getNetworkInterfaces();
        } catch (SocketException e) {
            e.printStackTrace();
        }
        while(null != e1 && e1.hasMoreElements()) {
            NetworkInterface ni = e1.nextElement();
            result.addAll(getIPforInterface(ni));
        }
        return result;
    }
    
    public static Set<String> getIPforInterface(NetworkInterface ni){
    	Set<String> result = new HashSet<String>();
        Enumeration<InetAddress> e2 = ni.getInetAddresses();
        while(e2.hasMoreElements()) {
            InetAddress ia = e2.nextElement();
            if( ! ia.isAnyLocalAddress()	&& !ia.isLinkLocalAddress() && !ia.isMulticastAddress()
            	 && Pattern.matches(IPADDRESS_PATTERN,ia.getHostAddress())){
            	result.add(ia.getHostAddress().toString());
            }
        }
        return result;
    }
        
    public static void main(String... args)
        throws Exception
    {
        Enumeration<NetworkInterface> e1 = (Enumeration<NetworkInterface>)NetworkInterface.getNetworkInterfaces();
        while(e1.hasMoreElements()) {
            NetworkInterface ni = e1.nextElement();
            
            System.out.print(ni.getName());
            System.out.print(" : [");
            Enumeration<InetAddress> e2 = ni.getInetAddresses();
            while(e2.hasMoreElements()) {
                InetAddress ia = e2.nextElement();
                System.out.print(ia);
                if( e2.hasMoreElements()) {
                    System.out.print(",");
                }
            }
            System.out.println("]");
        }
        
        System.out.println(IPUtils.getLocalIpAddresses());
    }
}