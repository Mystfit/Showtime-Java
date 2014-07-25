package ZST;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
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

	public String getName() {
		return m_name;
	}

	private String m_name;

	public String getNode() {
		return m_node;
	}

	private String m_node;

	public String getAccessMode() {
		return m_accessMode;
	}

	private String m_accessMode;

	public Map<String, Object> getArgs() {
		return m_args;
	}

	private Map<String, Object> m_args;

	public Object getOutput() {
		return m_output;
	}

	public void setOutput(Object value) {
		m_output = value;
	}

	private Object m_output;

	public Method getCallback() {
		return m_callback;
	};

	private Method m_callback;

	public Object getCallbackObject() {
		return m_callbackObject;
	}

	private Object m_callbackObject;

	/*
	 * Constructors
	 */
	public ZstMethod(String name, String node) 
	{
		this(name, node, READ, null, null, null);
	}

	public ZstMethod(String name, String node, String accessMode, Map<String, Object> args) 
	{
		this(name, node, accessMode, args, null, null);
	}

	public ZstMethod(String name, String node, String accessMode, Map<String, Object> args, Object callbackObject, Method callback) 
	{
		m_name = name;
		m_node = node;
		m_accessMode = accessMode;
		m_callback = callback;
		m_callbackObject = callbackObject;
		m_output = "";

		if (args != null)
			m_args = args;
		else
			m_args = new HashMap<String, Object>();
	}

	public ZstMethod clone() 
	{
		return new ZstMethod(m_name, m_node, m_accessMode, m_args,
				m_callbackObject, m_callback);
	}

	public Object run(ZstMethod methodData) 
	{
		Object out = null;

		try {
			out = m_callback.invoke(m_callbackObject, methodData);
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return out;
	}

	public Map<String, Object> asMap() 
	{
		Map<String, Object> m = new HashMap<String, Object>();
		m.put(METHOD_NAME, m_name);
		m.put(METHOD_ORIGIN_NODE, m_node);
		m.put(METHOD_ACCESSMODE, m_accessMode);
		m.put(METHOD_ARGS, m_args);
		m.put(METHOD_OUTPUT, m_output);
		return m;
	}

	public static Boolean compareArgLists(Map<String, Object> args1, Map<String, Object> args2) 
	{
		if (args1 == null && args2 == null)
			return true;

		if (args1 != null) {
			for (Map.Entry<String, Object> arg : args1.entrySet()) {
				if (args2 != null) {
					if (!args2.containsKey(arg.getKey()))
						return false;
				}
			}
			return true;
		}
		return false;
	}

	public static Map<String, ZstMethod> buildLocalMethods(JsonObject inMethods) 
	{
		Map<String, ZstMethod> outMethods = new HashMap<String, ZstMethod>();
		for (Entry<String, JsonElement> method : inMethods.entrySet()) {
			JsonObject methodObj = method.getValue().getAsJsonObject();
			outMethods.put(method.getKey(), jsonObjToZstMethod(methodObj));
		}

		return outMethods;
	}

	public static ZstMethod jsonObjToZstMethod(JsonObject methodData) 
	{
		if (methodData == null || methodData.entrySet().isEmpty())
			return null;
		
		//System.out.print(methodData.get(METHOD_ARGS));
		 
		JsonObject methodArgObj = null;
		if(methodData.has(METHOD_ARGS))
			if(!(methodData.get(METHOD_ARGS).equals(JsonNull.INSTANCE)))
				methodArgObj = methodData.get(METHOD_ARGS).getAsJsonObject();

		Map<String, Object> methodsArgs = null;
		if (methodArgObj != null) {
			methodsArgs = new HashMap<String, Object>();
			for (Map.Entry<String, JsonElement> methodArg : methodArgObj.entrySet()) {
				methodsArgs.put(methodArg.getKey(), methodArg.getValue().getAsString());
			}
		}
		
		//Handle null accessmodes
		String accessMode = null; 
		if(!methodData.get(METHOD_ACCESSMODE).equals(JsonNull.INSTANCE))
			accessMode = methodData.get(METHOD_ACCESSMODE).getAsString();

		ZstMethod localMethod = new ZstMethod(
				methodData.get(METHOD_NAME).getAsString(), 
				methodData.get(METHOD_ORIGIN_NODE).getAsString(),
				accessMode, 
				methodsArgs);
		if(!methodData.get(METHOD_OUTPUT).equals(JsonNull.INSTANCE)){
			localMethod.setOutput(methodData.get(METHOD_OUTPUT).toString());
		}

		return localMethod;
	}
}
