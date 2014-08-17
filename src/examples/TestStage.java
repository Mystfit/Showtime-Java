package examples;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import ZST.ZstMethod;
import ZST.ZstNode;
import ZST.ZstPeerlink;

public class TestStage{
	public TestStage(){
		ZstNode node = new ZstNode("javaStage", 6000);
		node.setVerbose(true);
        node.start();
//        while(true){
//        	try {
//				Thread.sleep(1000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//        }
	}
}
