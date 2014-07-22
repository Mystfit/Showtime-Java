package ZST;


import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

public class ZstIo {
	public static void send(Socket socket, String method)
	{
		ZstIo.send(socket, method, null);
	}
	
	public static void send(Socket socket, String method, ZstMethod methodData)
	{
		ZMsg message = new ZMsg();
		message.add(method);
		if(methodData != null)
		{
			//String data = 
		}
	
	}
}
