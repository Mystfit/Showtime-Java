package examples;


import java.util.HashMap;
import java.util.Map;

import ZST.ZstMethod;
import ZST.ZstNode;
import ZST.ZstPeerlink;

public class ShowtimeTest 
{
	public static void main(String[] args) 
	{
		ZstNode node = new ZstNode("javatest", "tcp://curiosity.soad.vuw.ac.nz:6000");
		node.requestRegisterNode();
		
		Map<String, Object> nodeArgs = new HashMap<String, Object>();
		nodeArgs.put("woof", "bark");
        node.requestRegisterMethod("bloople", ZstMethod.READ, nodeArgs);
		
		//ZstMethod.mapToZstMethod(fakeArgs);
		Map<String, ZstPeerlink> peers = node.requestNodePeerlinks();
		node.close();
		System.out.print(peers);
		System.exit(0);
	}
	
	public void testCallback(){}
}
