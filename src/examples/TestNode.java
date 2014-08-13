package examples;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import ZST.ZstMethod;
import ZST.ZstNode;
import ZST.ZstPeerlink;

public class TestNode{
	public TestNode(){
		ZstNode node = new ZstNode("javatest", "tcp://curiosity.soad.vuw.ac.nz:6000");
        node.start();
		node.requestRegisterNode();
		
		Map<String, Object> nodeArgs = new HashMap<String, Object>();
		nodeArgs.put("woof", "bark");
		
		Method testCallback = null;
		try {
			testCallback = TestNode.class.getDeclaredMethod("testCallback", new Class[]{ZstMethod.class});
		} catch (Exception e) {
			e.printStackTrace();
		}
		
        node.requestRegisterMethod("bloople", ZstMethod.WRITE, nodeArgs, this, testCallback);
        
        Map<String, ZstPeerlink> peers = node.requestNodePeerlinks();
        
        node.subscribeToNode(peers.get("LiveNode"));        
        
		
		//ZstMethod.mapToZstMethod(fakeArgs);
		//node.close();
		//System.out.print(peers);
	}
	
	public void testCallback(ZstMethod methodData){
		System.out.println(methodData.getArgs().get("woof"));
	}
}
