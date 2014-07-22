package ZST;

import java.lang.reflect.Method;


import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ZstMethod {
	public static String READ = "read";
	public static String WRITE = "write";
    public static String RESPONDER = "responder";
	public static String METHOD_LIST = "zst_method_list";
	public static String METHOD_NAME = "zst_method_name";
	public static String METHOD_ORIGIN_NODE = "zst_method_orig";
	public static String METHOD_ARGS = "zst_method_args";
	public static String METHOD_ACCESSMODE = "zst_method_Accessmode";
	public static String METHOD_OUTPUT = "zst_method_output";
	
	private String m_name;
	public String getName(){ return m_name; }
	
	private String m_node;
	public String getNode(){ return m_node; }
	
	private String m_accessMode;
	public String getAccessMode(){ return m_accessMode; }
	
	private Map<String, Object> m_args;
	public Map<String, Object> getArgs(){ return m_args; }
	
	private Object m_output;
	public Object getOutput(){ return m_output; }
	
	private Method m_callback;
	public Method getCallback(){ return m_callback; };

	/*
	 * Constructors
	 */
	public ZstMethod(String name, String node)
	{
		this(name, node, READ, null, null);
	}
	
	public ZstMethod(String name, String node, Map<String, Object> args)
	{
		this(name, node, READ, args, null);
	}
	
	public ZstMethod(String name, String node, String accessMode, Map<String, Object> args, Method callback)
	{
		m_name = name;
		m_node = node;
		m_accessMode = accessMode;
		m_callback = callback;
		m_output = "";
        
        if (args != null)
            m_args = args;
        else
            m_args = new HashMap<String, Object>();
	}
	
	public ZstMethod clone()
	{
		return new ZstMethod(m_name, m_node, m_accessMode, m_args, m_callback);
	}
	
	public Map<String, Object> asDict()
	{
		Map<String, Object> m = new HashMap<String, Object>();
		m.put(METHOD_NAME, m_name);
		m.put(METHOD_ORIGIN_NODE, m_node);
		m.put(METHOD_ACCESSMODE, m_accessMode);
		m.put(METHOD_ARGS, m_args);
		return m;
	}
	
	public static Boolean compareArgLists(Map<String, Object> args1, Map<String, Object> args2)
	{
		if(args1 == null && args2 == null)
			return true;
		
		if(args1 != null){
			for (Map.Entry<String, Object> arg : args1.entrySet()) {
				if(args2 != null){
					if(!args2.containsKey(arg.getKey()))
						return false;
				}
			}
			return true;
		}
		return false;
	}
	
//	public static ZstMethod mapToZstMethod(Map<String, Object> methodData)
//	{
//		if(methodData.isEmpty())
//			return null;
//		
//		Map<String, Object> methodsArgs = null;
//		if(methodData.get(METHOD_ARGS) != null){
//			JsonParser parser = new JsonParser();
//			JsonObject obj = (JsonObject)parser.parse(methodData.get(METHOD_ARGS).toString());
//		}
//
//	}
}
