package ZST;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.zeromq.ZMQ.Socket;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;


public class ZstPeerlink {
	
	//Message constants
	public static String REPLY_ADDRESS = "zst_key_rep_address";
	public static String PUBLISHER_ADDRESS = "zst_key_pub_address";
	public static String NAME = "zst_key_name";
	public static String METHOD_LIST = "zst_method_list";
	
	public String getName(){ return m_name; }
	private String m_name;
	
	public String getReplyAddress() { return m_replyAddress; }
	private String m_replyAddress;
    
    public String getPublisherAddress() { return m_publisherAddress; }
    private String m_publisherAddress;
	
    public Map<String, ZstMethod> getMethods() { return m_methods; }
    private Map<String, ZstMethod> m_methods;
    
    public Socket getRequestSocket(){ return m_request; }
    public void setRequestSocket(Socket s){ m_request = s; }
    private Socket m_request;
    
    public Socket getSubSocket(){ return m_subscriber; }
    public void setSubSocket(Socket s){ m_subscriber = s; }
    private Socket m_subscriber;
    
    public ZstPeerlink(String name, String replyAddress, String publisherAddress)
    {
    	init(name, replyAddress, publisherAddress, new HashMap<String, ZstMethod>());
    }
    
    public ZstPeerlink(String name, String replyAddress, String publisherAddress, Map<String, ZstMethod> methods)
    {
    	init(name, replyAddress, publisherAddress, methods);
    }
    
    private void init(String name, String replyAddress, String publisherAddress, Map<String, ZstMethod> methods)
    {
    	m_name = name;
		m_replyAddress = replyAddress;
		m_publisherAddress = publisherAddress;
		m_methods = methods;
    }
    
    public void disconnect()
    {
    	if(m_request != null)
    		m_request.close();
    	if(m_subscriber != null)
    		m_subscriber.close();
    }
    
    public Map<String, Object> asMap()
    {
    	Map<String, Object> outDict = new HashMap<String, Object>();
    	Map<String, Map<String, Object> > methods = new HashMap<String, Map<String,Object>>();
    	
    	for (Map.Entry<String, ZstMethod> method : m_methods.entrySet())
    		methods.put(method.getKey(), method.getValue().asMap());
    	
    	outDict.put(NAME, m_name);
    	outDict.put(PUBLISHER_ADDRESS, m_publisherAddress);
    	outDict.put(REPLY_ADDRESS, m_replyAddress);
    	outDict.put(METHOD_LIST, methods);
    	
    	return outDict;
    }
    
    public static Map<String, ZstPeerlink> buildLocalPeerlinks(JsonObject peers)
    {
    	Map<String, ZstPeerlink> outPeerLinks = new HashMap<String, ZstPeerlink>();
    	
    	for (Entry<String, JsonElement> peer : peers.entrySet()){
    		JsonParser parser = new JsonParser();
    		JsonObject peerObj = (JsonObject) parser.parse(peer.getValue().toString());
    		JsonObject inMethods = (JsonObject) parser.parse(peerObj.get(METHOD_LIST).toString());
    		
    		Map<String, ZstMethod> methodList = ZstMethod.buildLocalMethods(inMethods);	
    		outPeerLinks.put(peer.getKey(), new ZstPeerlink(
    				peerObj.get(NAME).toString(), 
    				peerObj.get(REPLY_ADDRESS).toString(), 
    				peerObj.get(PUBLISHER_ADDRESS).toString(), 
    				methodList));
    	}
    	
    	return outPeerLinks;
    }
    
   
}
