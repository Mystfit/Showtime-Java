package ZST;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;

import com.google.gson.JsonObject;

import sun.reflect.Reflection;
import zmq.Ctx;
import zmq.PollerBase;
import zmq.Req;
import zmq.SocketBase;

/**
 * This is a template class and can be used to start a new processing library or tool.
 * Make sure you rename this class as well as the name of the example package 'template' 
 * to your own library or tool naming convention.
 * 
 * @example Hello 
 * 
 * (the tag @example followed by the name of an example included in folder 'examples' will
 * automatically include the example in the javadoc.)
 *
 */

public class ZstNode extends Thread {

	// Constants
    // ---------
	
    // Replies
    public static String REPLY = "zst_reply";
    public static String OK = "zst_ok";

    // Methods
    public static String REPLY_REGISTER_METHOD = "reply_register_method";
    public static String REPLY_REGISTER_NODE = "reply_register_node";
    public static String REPLY_NODE_PEERLINKS = "reply_node_peerlinks";
    public static String REPLY_METHOD_LIST = "reply_list_methods";
    public static String REPLY_ALL_PEER_METHODS = "reply_all_peer_methods";
    public static String DISCONNECT_PEER = "disconnect_peer";


    // Member variables
    // ----------------
    protected String m_nodeId;
    
    protected Map<String, ZstMethod> m_internalNodeMethods;
    protected String m_stageAddress;
    protected int m_stagePort;
    protected String m_replyAddress;
    protected String m_publisherAddress;
    
    public void setVerbose(boolean verbose){m_verbose = verbose; }
    private boolean m_verbose;

    public Map<String, ZstMethod> getMethods() { return m_methods; }
    protected Map<String, ZstMethod> m_methods;
    public Map<String, ZstPeerlink> getPeers() { return m_peers; }
    protected Map<String, ZstPeerlink> m_peers;


    // Zmq variables
    protected ZContext m_ctx;
    protected Socket m_reply;
    protected Socket m_publisher;
    protected Socket m_subscriber;
    protected Socket m_stage;
    protected Poller m_poller;
    protected List<Socket> m_incomingSockets;
    
    //Thread variables
    private Boolean m_exitFlag = false;
    
    //Constructor for stages
	public ZstNode(String nodeId, int stagePort){
		m_stagePort = stagePort;
		init(nodeId, "");
	}
	
	public ZstNode(String nodeId, String stageAddress) {		
		init(nodeId, stageAddress);
	}
	
	private void init(String nodeId, String stageAddress)
	{
		m_nodeId = nodeId;
		m_methods = new HashMap<String, ZstMethod>();
		m_internalNodeMethods = new HashMap<String, ZstMethod>();
		m_peers = new HashMap<String, ZstPeerlink>();
		m_incomingSockets = new ArrayList<Socket>();
		m_stageAddress = stageAddress;
		initNetwork();
		
		
	}
	
	
	/**
	 * Setup local sockets
	 */
	private void initNetwork()
	{
		m_ctx = new ZContext();

		m_reply = m_ctx.createSocket(ZMQ.REP);		
		m_reply.setLinger(0);
		m_reply.setReceiveTimeOut(2);
		
		m_publisher = m_ctx.createSocket(ZMQ.PUB);	
		m_publisher.setLinger(0);
		
		m_subscriber = m_ctx.createSocket(ZMQ.SUB);	
		m_subscriber.setLinger(0);
		m_subscriber.subscribe(new byte[0]);
		
		String address = null;
		int publisherPort = -1;
		
		try {
			address = "tcp://" + InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		m_publisher.bind(address + ":*");
		m_publisherAddress = (String)zmq.ZMQ.zmq_getsockoptx(m_publisher.base(), zmq.ZMQ.ZMQ_LAST_ENDPOINT);
			
		
		if(!m_stageAddress.isEmpty()){
			m_reply.bind(address + ":*");
			m_replyAddress = (String)zmq.ZMQ.zmq_getsockoptx(m_reply.base(), zmq.ZMQ.ZMQ_LAST_ENDPOINT);
			
			m_stage = m_ctx.createSocket(ZMQ.REQ);	
			m_stage.setLinger(0);
			m_stage.connect(m_stageAddress);
									
			if(m_verbose) {
				System.out.println("Stage located at " + (String)zmq.ZMQ.zmq_getsockoptx(m_stage.base(), zmq.ZMQ.ZMQ_LAST_ENDPOINT));
				System.out.println("Node reply on address " + m_replyAddress);
				System.out.println("Node publisher on address " + m_publisherAddress);
			}
			System.out.println("Showtime active!");
		} else {
			m_reply.bind("tcp://*:" + m_stagePort);
		}
		
		m_incomingSockets.add(m_reply);
		m_incomingSockets.add(m_subscriber);
		
		m_poller = new Poller(2);
		m_poller.register(m_reply, Poller.POLLIN);
		m_poller.register(m_subscriber, Poller.POLLIN);
		
		try {
			registerInternalMethods();
		} catch (NoSuchMethodException e1) {
			e1.printStackTrace();
		} catch (SecurityException e1) {
			e1.printStackTrace();
		}
	}
	
	
	/**
	 * Registers internal methods this node owns
	 * @throws 		SecurityException 
	 * @throws 		NoSuchMethodException 
	 */
	private void registerInternalMethods() throws NoSuchMethodException, SecurityException
	{
		Method replyRegisterNode_callback = ZstNode.class.getDeclaredMethod("replyRegisterNode", new Class[]{ZstMethod.class});
		m_internalNodeMethods.put(REPLY_REGISTER_NODE, new ZstMethod(
				REPLY_REGISTER_NODE, 
				m_nodeId, 
				ZstMethod.RESPONDER, 
				null,
				this,
				replyRegisterNode_callback));
		
		Method replyRegisterMethod_callback = ZstNode.class.getDeclaredMethod("replyRegisterMethod", new Class[]{ZstMethod.class});
		m_internalNodeMethods.put(REPLY_REGISTER_METHOD, new ZstMethod(
				REPLY_REGISTER_METHOD, 
				m_nodeId, 
				ZstMethod.RESPONDER, 
				null,
				this,
				replyRegisterMethod_callback));
		
		Method replyNodePeerlinks_callback = ZstNode.class.getDeclaredMethod("replyNodePeerlinks", new Class[]{ZstMethod.class});
		m_internalNodeMethods.put(REPLY_NODE_PEERLINKS, new ZstMethod(
				REPLY_NODE_PEERLINKS, 
				m_nodeId, 
				ZstMethod.RESPONDER, 
				null,
				this,
				replyNodePeerlinks_callback));
		
		Method replyMethodList_callback =ZstNode.class.getDeclaredMethod("replyMethodList", new Class[]{ZstMethod.class});
		m_internalNodeMethods.put(REPLY_METHOD_LIST, new ZstMethod(
				REPLY_METHOD_LIST, 
				m_nodeId, 
				ZstMethod.RESPONDER, 
				null,
				this,
				replyMethodList_callback));
		
		Method replyAllPeerMethods_callback = ZstNode.class.getDeclaredMethod("replyAllPeerMethods", new Class[]{ZstMethod.class});
		m_internalNodeMethods.put(REPLY_ALL_PEER_METHODS, new ZstMethod(
				REPLY_ALL_PEER_METHODS, 
				m_nodeId, 
				ZstMethod.RESPONDER, 
				null,
				this,
				replyAllPeerMethods_callback));
		
		Method disconnectPeer_callback = ZstNode.class.getDeclaredMethod("disconnectPeer", new Class[]{ZstMethod.class});
		m_internalNodeMethods.put(DISCONNECT_PEER, new ZstMethod(
				DISCONNECT_PEER, 
				m_nodeId, 
				ZstMethod.WRITE, 
				null,
				this,
				disconnectPeer_callback));
	}
	
	
	/**
	 * Main listen loop. Receives poll events and calls local methods
	 */
	public void listen()
	{
		System.out.println("Node listening for requests...");
		while (!m_exitFlag){
			int incomingPolls = m_poller.poll();
			if(incomingPolls > 0){
				for(int i = 0; i < m_incomingSockets.size(); i++){
					if(m_poller.pollin(i)){
						receiveMessage(ZstIo.recv(m_poller.getSocket(i)));
					}
				}
			}
		}
		System.out.println("Exiting listen loop");
	}
	
	
	/**
	 * Message caller. Runs local methods from messages
	 */
	private void receiveMessage(MethodMessage recv) {
		if(m_verbose) System.out.print("Recieved method '" + recv.method + "'");
		
		if (recv.data != null)
        {
            if (recv.data.getOutput() != null)
                if(m_verbose) System.out.print("' for '" + recv.data.getNode() + "' with value '" + recv.data.getOutput().toString());
            if(m_verbose) System.out.println("'");
	
			//Is this a method this node owns?
			if(recv.data.getNode() == m_nodeId){
				Map<String, ZstMethod> methodList = null;
				if(m_internalNodeMethods.containsKey(recv.method)){
					methodList = m_internalNodeMethods;
				} else if(m_methods.containsKey(recv.method)){
					methodList = m_methods;
				}
				
				try {
					ZstMethod method = methodList.get(recv.method);
					Object callbackObj = methodList.get(recv.method).getCallbackObject();
					Method callback = methodList.get(recv.method).getCallback();
					callback.invoke(callbackObj, recv.data);
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			//Or does this method belong to another node?
			} else {
				//Run local callbacks for remote method
		        if (recv.data != null)
		        {
		            if (m_peers.containsKey(recv.data.getNode()))
		            {
		            	ZstPeerlink peer = m_peers.get(recv.data.getNode());
		                if (peer.getMethods().containsKey(recv.method))
		                {
		                	ZstMethod peerMethod = m_peers.get(recv.data.getNode()).getMethods().get(recv.method);
		                    if (peerMethod.getLocalCallbackMethod() != null && peerMethod.getLocalCallbackObject() != null)
		                    {
		                    	Object callbackObj = peerMethod.getLocalCallbackObject();
		                    	try {
		                    		peerMethod.getLocalCallbackMethod().invoke(callbackObj, recv.data);
								} catch (Exception e) {
									e.printStackTrace();
								}
		                    }
		                }
		            }
		        }
			}
        }
	}
	
	
	/**
	 * Remote peer is announcing that it's leaving. Remove from local lists.
	 * @return 		null
	 */
	private Object disconnectPeer(ZstMethod methodData){
		if(m_verbose) System.out.println("Peer '" + methodData.getNode() + "' is leaving.");
	     if (m_peers.containsKey(methodData.getNode()))
	     {
	         try{
	             m_subscriber.disconnect(m_peers.get(methodData.getNode()).getPublisherAddress());
	         } catch (ZMQException e) {
	             throw e;
	         }
	
	         m_peers.get(methodData.getNode()).disconnect();
	         m_peers.remove(methodData.getNode());
	     }
	     return null;
	}
	
	/**
	 * Thread function
	 */
	public void run()
	{
		listen();
	}
	
	/**
	 * Close this node
	 * @return 		Success flag
	 */
	public Boolean close()
	{
		//Tell stage we're leaving
        ZstIo.send(m_publisher, DISCONNECT_PEER, new ZstMethod(DISCONNECT_PEER, m_nodeId));

		m_exitFlag = true;
		for(Socket s : m_incomingSockets){
			m_poller.unregister(s);
			//s.close();
		}
		
		m_ctx.destroy();
		
		return true;
	}
	
	
	
	// Node registration
    //------------------

	/**
	 * Request stage to register this node
	 * @return 		Success flag
	 */
    public Boolean requestRegisterNode(){
        return requestRegisterNode(m_stage);
    }
    
    
	/**
	 * Request remote node to register this node
	 * @param Socket 		Socket to register through
	 */
    public Boolean requestRegisterNode(Socket socket){
    	if(m_verbose) System.out.println("REQ-->: Requesting remote node to register our addresses. Reply:" + m_replyAddress + ", Publisher:" + m_publisherAddress);
        
        Map<String, Object> requestArgs = new HashMap<String, Object>();
        requestArgs.put(ZstPeerlink.REPLY_ADDRESS, m_replyAddress);
        requestArgs.put(ZstPeerlink.PUBLISHER_ADDRESS, m_publisherAddress);
        ZstMethod request = new ZstMethod(REPLY_REGISTER_NODE, m_nodeId, ZstMethod.RESPONDER, requestArgs);

        ZstIo.send(socket, REPLY_REGISTER_NODE, request);
        MethodMessage message = ZstIo.recv(socket);
        
        if (message.method.equals(OK)){
        	if(m_verbose) System.out.println("REP<--: Remote node acknowledged our addresses. Reply:" + m_replyAddress + ", Publisher:" + m_publisherAddress);
			return true;
		} else { 
			if(m_verbose) System.out.println("REP<--:Remote node returned " + message.method + " instead of " + OK);
		}
		return false;
    }
    
    
	/**
	 * Reply to request for node registration
	 * @param methodData  	Incoming methodData.
	 * @return 				Null
	 */
    protected Object replyRegisterNode(ZstMethod methodData)
    {
    	if(m_peers.containsKey(methodData.getNode()))
    		System.out.println("'" + m_nodeId + "' already registered. Overwriting");
    	
    	 String nodeId = methodData.getNode();
         m_peers.put(nodeId, new ZstPeerlink(
             nodeId,
             methodData.getArgs().get(ZstPeerlink.REPLY_ADDRESS).toString(),
             methodData.getArgs().get(ZstPeerlink.PUBLISHER_ADDRESS).toString()));

         subscribeToNode(m_peers.get(nodeId));
         ZstIo.send(m_reply, OK);
         System.out.println("Registered node '" + nodeId + "'. Reply:" + m_peers.get(nodeId).getReplyAddress() + ", Publisher:" + m_peers.get(nodeId).getPublisherAddress());
         return null;
    }
    
    
    /**
	 * Subscribe to messages from remote node
	 * @param methodData  		Incoming methodData.
	 */
    public void subscribeToNode(ZstPeerlink peer)
    {
    	m_subscriber.connect(peer.getPublisherAddress());
        m_subscriber.subscribe(new byte[0]);
        m_peers.put(peer.getName(), peer);	

       System.out.println("Connected to peer on " + peer.getPublisherAddress());
    }
    
    
    /**
	 * Requests a remote node to subscribe to our requests
	 * @param methodDdata  		Incoming methodData.
	 */
    public void connectToPeer(ZstPeerlink peer)
    {
        Socket socket = m_ctx.createSocket(ZMQ.REQ);
        socket.setLinger(0);
        //socket.setReceiveTimeOut(2);

        socket.connect(peer.getReplyAddress());
        
		if(requestRegisterNode(socket)){
			if(!m_peers.containsKey(peer.getName()))
				m_peers.put(peer.getName(), peer);
			peer.setRequestSocket(socket);
		}
    }
    
    
    //Remote method registration
    //------------------------------------
    /**
	 * Requests a local method on a remote node
	 * @param method 			Method name.
	 * @param accessMode		Access mode for method (READ, WRITE, RESPONDER).
	 */
    public void requestRegisterMethod(String method, String accessMode){
        requestRegisterMethod(method, accessMode, null, null, null, m_stage);
    }

    
    /**
	 * Requests a local method on a remote node
	 * @param method		  	Method name.
	 * @param accessMode  		Access mode for method (READ, WRITE, RESPONDER).
	 * @param args 				Arguments for method.
	 */
    public void requestRegisterMethod(String method, String accessMode, Map<String, Object> args){
        requestRegisterMethod(method, accessMode, args, null, null, m_stage);
    }

    
    /**
	 * Requests a local method on a remote node
	 * @param method  			Method name.
	 * @param accessMode  		Access mode for method (READ, WRITE, RESPONDER).
	 * @param args 				Arguments for method.
	 */
    public void requestRegisterMethod(String method, String accessMode, String[] args){
        Map<String, Object> argsDict = new HashMap<String, Object>();
        for(int i = 0; i < args.length; i++)
            argsDict.put(args[i], "");
        requestRegisterMethod(method, accessMode, argsDict, null, null, m_stage);
    }

    
    /**
	 * Requests a local method on a remote node
	 * @param method  			Method name.
	 * @param accessMode  		Access mode for method (READ, WRITE, RESPONDER).
	 * @param args 				Arguments for method.
	 * @param socket 			Socket to send request through.
	 */
    public void requestRegisterMethod(String method, String accessMode, Map<String, Object> args, Socket socket){
        requestRegisterMethod(method, accessMode, args, null, null, socket);
    }

    
    /**
	 * Requests a local method on a remote node
	 * @param method  			Method name.
	 * @param accessMode  		Access mode for method (READ, WRITE, RESPONDER).
	 * @param callbackobj 		Object to run callback on.
	 * @param callback 			Callback method.
	 */
    public void requestRegisterMethod(String method, String accessMode, Object callbackObject, Method callback){
        requestRegisterMethod(method, accessMode, null, callbackObject, callback, m_stage);
    }
    
    
    /**
	 * Requests a local method on a remote node
	 * @param method  			Method name.
	 * @param accessMode  		Access mode for method (READ, WRITE, RESPONDER).
	 * @param args 				Arguments for method.
	 * @param callbackobj 		Object to run callback on.
	 * @param callback 			Callback method.
	 */
    public void requestRegisterMethod(String method, String accessMode, Map<String, Object> args, Object callbackObject, Method callback){
        requestRegisterMethod(method, accessMode, args, callbackObject, callback, m_stage);
    }

    /**
     * Registers a local method on a remote node</summary>
     * @param method  			Method name.
	 * @param accessMode  		Access mode for method (READ, WRITE, RESPONDER).
	 * @param args 				Arguments for method.
	 * @param callbackobj 		Object to run callback on.
	 * @param callback 			Callback method.
     * @param socket 			socket
     */
    public void requestRegisterMethod(String method, String accessMode, Map<String, Object> args, Object callbackObject, Method callback, Socket socket)
    {
    	if(m_verbose) System.out.println("REQ-->: Registering method " + method + " with remote node and args " + args.toString());

        //Register local copy of our method first
        m_methods.put(method, new ZstMethod(method, m_nodeId, accessMode, args, callbackObject, callback));

        //Register method copy on remote node
        ZstIo.send(socket, REPLY_REGISTER_METHOD, m_methods.get(method));
        MethodMessage msg = ZstIo.recv(socket);

        if (msg.method.equals(OK))
        	if(m_verbose) System.out.println("REP<--: Remote node acknowledged our method '" + method + "'");
        else
        	if(m_verbose) System.out.println("REP<--:Remote node returned " + msg.method + " instead of " + OK);
    }
    
    
    /**
	 * Reply to another node's method registration request
	 * @param methodData  	Method data.
	 */
    protected Object replyRegisterMethod(ZstMethod methodData)
    {
        if (m_peers.get(methodData.getNode()).getMethods().containsKey(methodData.getName()))
            System.out.println("'" + methodData.getName() + "' already registered on node " + methodData.getNode() + ". Overwriting");

        m_peers.get(methodData.getName()).getMethods().put(methodData.getNode(), methodData.clone());

        ZstIo.send(m_reply, OK);
        System.out.println("Registered method '" + methodData.getName() + "'. Origin:" + methodData.getNode() + ", AccessMode:" + methodData.getAccessMode() + ", Args:" + methodData.getArgs());
        return null;
    }
    
    
    
    // Node peerlink accessors
    //------------------------
    /**
     * Request a dictionary of peers nodes linked to the remote node
     * @return 		Map of names/peers
     */
    public Map<String, ZstPeerlink> requestNodePeerlinks()
    {
        return requestNodePeerlinks(m_stage);
    }

    
    /**
     * Request a dictionary of peers nodes linked to the remote node
     * @return 		Map of names/peers
     */
    public Map<String, ZstPeerlink> requestNodePeerlinks(Socket socket)
    {
        ZstIo.send(socket, REPLY_NODE_PEERLINKS);
        MethodMessage message = ZstIo.recv(socket);
        String peerData = message.data.getOutput().toString();
        JsonObject peerlinkObj = (JsonObject) ZstIo.jsonParser.parse(peerData);
        return ZstPeerlink.buildLocalPeerlinks(peerlinkObj);
    }
    
    /**
     * Reply to another node's request for this node's linked peers
     * @return 		null
     */
    protected Object replyNodePeerlinks(ZstMethod methodData)
    {
        Map<String, Object> peerDict = new HashMap<String, Object>();
        
        for(Entry<String, ZstPeerlink> peer : m_peers.entrySet() )
            peerDict.put(peer.getKey(), peer.getValue().asMap());
        ZstMethod request = new ZstMethod(REPLY_NODE_PEERLINKS, m_nodeId, "", null);
        request.setOutput(peerDict);
        ZstIo.send(m_reply, OK, request);
        return null;
    }
    
    
    public Map<String, ZstMethod> requestMethodList()
    {
        return requestMethodList(m_stage);
    }


    // Node method accessors
    //----------------------
   /**
    * Request a list of methods on a remote node
    * @return		 Map of methods
    */
    public Map<String, ZstMethod> requestMethodList(Socket socket)
    {
        ZstIo.send(socket, REPLY_METHOD_LIST);
        String jsonStr = ZstIo.recv(socket).data.getOutput().toString();
        JsonObject methodList = (JsonObject) ZstIo.jsonParser.parse(jsonStr);
        return ZstMethod.buildLocalMethods(methodList);
    }

    /**
     * Reply with a list of methods this node owns
     * @param 		methodData
     * @return 		null
     */
    public Object replyMethodList(ZstMethod methodData)
    {
        Map<String, Object> methodDict = new HashMap<String, Object>();
        for(Entry<String, ZstMethod> method : m_methods.entrySet())
            methodDict.put(method.getKey(), method.getValue().asMap());
        ZstMethod request = new ZstMethod(REPLY_NODE_PEERLINKS, m_nodeId, "", null);
        request.setOutput(methodDict);
        ZstIo.send(m_reply, OK, request);
        return null;
    }

    
    
    // Get all methods on all connected peers
    //----------------------
    /**
     * Request a dictionary of methods on a remote node
     * @return 		Map of methods
     */
    public Map<String, ZstMethod> requestAllPeerMethods()
    {
        return requestAllPeerMethods(m_stage);
    }

    /**
     * Request a list of all available methods provided by all connected peers on the remote node</summary>
     * @param 		socket
     * @return 		Map of methods
     */
    public Map<String, ZstMethod> requestAllPeerMethods(Socket socket)
    {
        ZstIo.send(socket, REPLY_METHOD_LIST);
        String jsonStr = ZstIo.recv(socket).data.getOutput().toString();
        JsonObject peerMethodList = (JsonObject)ZstIo.jsonParser.parse(jsonStr);
        return ZstMethod.buildLocalMethods(peerMethodList);
    }

    /**
     * Reply with a list of all available methods provided by all connected peers on the remote node
     * @param 		methodData
     * @return 		null
     */
    public Object replyAllPeerMethods(ZstMethod methodData)
    {
        Map<String, Object> methodDict = new HashMap<String, Object>();
        for(Entry<String, ZstPeerlink> peer : m_peers.entrySet())
        {
            for(Entry<String, ZstMethod> method : peer.getValue().getMethods().entrySet())
                methodDict.put(method.getKey(), method.getValue().asMap());
        }
        ZstMethod request = new ZstMethod(REPLY_NODE_PEERLINKS, m_nodeId, "", null);
        request.setOutput(methodDict);
        ZstIo.send(m_reply, OK, request);
        return null;
    }
    
    
    
    // Method publishing / controlling
    // -------------------------------
    /**
     * Updates a local method by name
     * @param method 		Method name
     * @param method 		Method value
     */
    public void updateLocalMethodByName(String method, Object methodValue)
    {
		if(m_methods.containsKey(method)){
			updateLocalMethod(m_methods.get(method), methodValue);
		}
    }
    
    /**
     * Updates a local method
     * @method 				Method name
     * @methodValue 		Method value
     */
    public void updateLocalMethod(ZstMethod method, Object methodValue)
    {
    	//Check we own this method
    	if(method.getNode() == m_nodeId){
    		Socket socket;
    		if(method.getAccessMode() == ZstMethod.RESPONDER)
    			socket = m_reply;
    		else
    			socket = m_publisher;
    		
    		method.setOutput(methodValue);
    		
    		ZstIo.send(socket, method.getName(), method);
    	}
    }
    
    
    /**
     * Updates a method on a remote node by name
     * @param name 		Method name
     * @return 			Remote method response (if method is a responder)
     */
    public ZstMethod updateRemoteMethodByName(String name)
    {
    	return updateRemoteMethodByName(name, null);
    }
    
    /**
     * Updates a method on a remote node by name
     * @param name		Method name		
     * @param args		Method arguments
     * @return			Remote method response (if method is a responder)
     */
    public ZstMethod updateRemoteMethodByName(String name, Map<String, Object> args)
    {
    	if(m_methods.containsKey(name))
    	{
    		ZstMethod method = m_methods.get(name);
    		return updateRemoteMethod(method, args);
    	}
    	return null;
    }
    
    /**
     * Updates a method on a remote node
     * @param name		Method name		
     * @return			Remote method response (if method is a responder)
     */
    public ZstMethod updateRemoteMethod(ZstMethod method)
    {
    	return updateRemoteMethod(method, null);
    }
    
    /**
     * Updates a method on a remote node
     * @param name		Method name		
     * @param args		Method arguments
     * @return			Remote method response (if method is a responder)
     */
    public ZstMethod updateRemoteMethod(ZstMethod method, Map<String, Object> args)
    {
    	Socket socket;
    	if(method.getAccessMode().equals(ZstMethod.RESPONDER))
    		socket = m_peers.get(method.getNode()).getRequestSocket();
    	else
    		socket = m_publisher;
    	
    	if(m_peers.containsKey(method.getNode())){
    		if(ZstMethod.compareArgLists(m_peers.get(method.getNode()).getMethods().get(method.getName()).getArgs(), args)){
    			ZstMethod methodRequest = new ZstMethod(method.getName(), method.getNode(), method.getAccessMode(), args);
    			ZstIo.send(socket, method.getName(), methodRequest);
    			
    			if(methodRequest.getAccessMode().equals(ZstMethod.RESPONDER)){
    				return ZstIo.recv(socket).data;
    			}
    		}
    	}
    	
    	return null;
    }
    
    
    /**
     * Subscribe to updates from a remote method
     * @param method 			Remote method to subscribe to 
     * @param callbackObject	Object to run callback on
     * @param callbackMethod	Callback to run
     */
    public void subscribeToMethod(ZstMethod method, Object callbackObject, Method callbackMethod)
    {
    	if(m_peers.containsKey(method.getNode()))
    	{
    		ZstMethod remoteMethod = m_peers.get(method.getNode()).getMethods().get(method.getName());
    		remoteMethod.setLocalCallbackObject(callbackObject);
    		remoteMethod.setLocalCallbackMethod(callbackMethod);
    	}
    }
        
}

