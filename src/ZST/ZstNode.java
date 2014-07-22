package ZST;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

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

	/**
	 * a Constructor, usually called in the setup() method in your sketch to
	 * initialize and start the library.
	 * 
	 * @example Hello
	 * @param theParent
	 */
	public ZstNode() {		
		ZContext ctx = new ZContext();
		Socket stage = ctx.createSocket(ZMQ.REQ);
		stage.connect("tcp://curiosity.soad.vuw.ac.nz:6000");
		stage.sendMore("reply_register_node");
		stage.send("{\"zst_method_args\": {\"zst_key_rep_address\": \"tcp://130.195.44.40:58771\", \"zst_key_pub_address\": \"tcp://130.195.44.40:58772\"}, \"zst_method_output\": null, \"zst_method_orig\": \"ProcessingTest\", \"zst_method_Accessmode\": null, \"zst_method_name\": \"reply_register_node\"}");
		System.out.println(stage.recvStr());
		System.out.println(stage.recvStr());
		ctx.close();	
	}
}

