package ZST;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
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

public class ZstNode {

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
									
			System.out.println("Stage located at " + (String)zmq.ZMQ.zmq_getsockoptx(m_stage.base(), zmq.ZMQ.ZMQ_LAST_ENDPOINT));
			System.out.println("Node reply on address " + m_replyAddress);
			System.out.println("Node publisher on address " + m_publisherAddress);
		} else {
			m_reply.bind("tcp://*:" + m_stagePort);
		}
		
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
	 * @throws SecurityException 
	 * @throws NoSuchMethodException 
	 */
	private void registerInternalMethods() throws NoSuchMethodException, SecurityException
	{
		Method registerNode = getClass().getDeclaredMethod("replyRegisterNode", new Class[]{ZstMethod.class});
		m_internalNodeMethods.put(REPLY_REGISTER_NODE, new ZstMethod(
				REPLY_REGISTER_NODE, 
				m_nodeId, 
				ZstMethod.READ, 
				null,
				this,
				registerNode));
	}
	
	
	/**
	 * Close this node
	 */
	public Boolean close()
	{
		m_poller.unregister(m_reply);
		m_poller.unregister(m_subscriber);
		m_ctx.destroy();
		
		return true;
	}
	
	

	/**
	 * Request stage to register this node
	 */
    public Boolean requestRegisterNode(){
        return requestRegisterNode(m_stage);
    }
    
    
	/**
	 * Request remote node to register this node
	 */
    public Boolean requestRegisterNode(Socket socket){
        System.out.println("REQ-->: Requesting remote node to register our addresses. Reply:" + m_replyAddress + ", Publisher:" + m_publisherAddress);
        if(socket == null)
        	socket = m_stage;
        
        Map<String, Object> requestArgs = new HashMap<String, Object>();
        requestArgs.put(ZstPeerlink.REPLY_ADDRESS, m_replyAddress);
        requestArgs.put(ZstPeerlink.PUBLISHER_ADDRESS, m_publisherAddress);
        ZstMethod request = new ZstMethod(REPLY_REGISTER_NODE, m_nodeId, "", requestArgs);

        ZstIo.send(socket, REPLY_REGISTER_NODE, request);
        MethodMessage message = ZstIo.recv(socket);
        
        if (message.method.equals(OK)){
            System.out.println("REP<--: Remote node acknowledged our addresses. Reply:" + m_replyAddress + ", Publisher:" + m_publisherAddress);
			return true;
		} else { 
			System.out.println("REP<--:Remote node returned " + message.method + " instead of " + OK);
		}
		return false;
    }
    
    
	/**
	 * Reply to request for node registration
	 *
	 * @methodData  Incoming methodData.
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
	 *
	 * @methodData  Incoming methodData.
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
	 *
	 * @methodData  Incoming methodData.
	 */
    public void connectToPeer(ZstPeerlink peer)
    {
        Socket socket = m_ctx.createSocket(ZMQ.REQ);
        socket.setLinger(0);
        socket.setReceiveTimeOut(2);

        socket.connect(peer.getReplyAddress());
        
		if(requestRegisterNode(socket)){
			if(!m_peers.containsKey(peer.getName()))
				m_peers.put(peer.getName(), peer);
			peer.setRequestSocket(socket);
		}
    }
    
    
    /**
	 * Requests a local method on a remote node
	 *
	 * @method  Method name.
	 * @accessMode  Access mode for method (READ, WRITE, RESPONDER).
	 */
    public void requestRegisterMethod(String method, String accessMode){
        requestRegisterMethod(method, accessMode, null, null, null, m_stage);
    }

    
    /**
	 * Requests a local method on a remote node
	 *
	 * @method  Method name.
	 * @accessMode  Access mode for method (READ, WRITE, RESPONDER).
	 * @args Arguments for method.
	 */
    public void requestRegisterMethod(String method, String accessMode, Map<String, Object> args){
        requestRegisterMethod(method, accessMode, args, null, null, m_stage);
    }

    
    /**
	 * Requests a local method on a remote node
	 *
	 * @method  Method name.
	 * @accessMode  Access mode for method (READ, WRITE, RESPONDER).
	 * @args Arguments for method.
	 */
    public void requestRegisterMethod(String method, String accessMode, String[] args){
        Map<String, Object> argsDict = new HashMap<String, Object>();
        for(int i = 0; i < args.length; i++)
            argsDict.put(args[i], "");
        requestRegisterMethod(method, accessMode, argsDict, null, null, m_stage);
    }

    
    /**
	 * Requests a local method on a remote node
	 *
	 * @method  Method name.
	 * @accessMode  Access mode for method (READ, WRITE, RESPONDER).
	 * @args Arguments for method.
	 * @socket Socket to send request through.
	 */
    public void requestRegisterMethod(String method, String accessMode, Map<String, Object> args, Socket socket){
        requestRegisterMethod(method, accessMode, args, null, null, socket);
    }

    
    /**
	 * Requests a local method on a remote node
	 *
	 * @method  Method name.
	 * @accessMode  Access mode for method (READ, WRITE, RESPONDER).
	 * @args Arguments for method.
	 * @socket Socket to send request through.
	 */
    public void requestRegisterMethod(String method, String accessMode, Map<String, Object> args, Object callbackObject, Method callback){
        requestRegisterMethod(method, accessMode, args, callbackObject, callback, m_stage);
    }

    /// <summary>Registers a local method on a remote node</summary>
    public void requestRegisterMethod(String method, String accessMode, Map<String, Object> args, Object callbackObject, Method callback, Socket socket)
    {
        System.out.println("REQ-->: Registering method " + method + " with remote node.");

        //Register local copy of our method first
        m_methods.put(method, new ZstMethod(method, m_nodeId, accessMode, args, callbackObject, callback));

        //Register method copy on remote node
        ZstIo.send(socket, REPLY_REGISTER_METHOD, m_methods.get(method));
        MethodMessage msg = ZstIo.recv(socket);

        if (msg.method.equals(OK))
            System.out.println("REP<--: Remote node acknowledged our method '" + method + "'");
        else
        	System.out.println("REP<--:Remote node returned " + msg.method + " instead of " + OK);
    }
    
    
    /**
	 * Reply to another node's method registration request
	 *
	 * @methoddata  Method data.
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
    
    /**
     * Request a dictionary of peers nodes linked to the remote node</summary>
     * @return
     */
    public Map<String, ZstPeerlink> requestNodePeerlinks()
    {
        return requestNodePeerlinks(m_stage);
    }

    /// <summary>Request a dictionary of peers nodes linked to the remote node</summary>
    public Map<String, ZstPeerlink> requestNodePeerlinks(Socket socket)
    {
        ZstIo.send(socket, REPLY_NODE_PEERLINKS);
        MethodMessage message = ZstIo.recv(socket);
        String peerData = message.data.getOutput().toString();
        JsonObject peerlinkObj = (JsonObject) ZstIo.jsonParser.parse(peerData);
        return ZstPeerlink.buildLocalPeerlinks(peerlinkObj);
    }
}

