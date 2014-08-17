package ZST;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.zeromq.ZMsg;

public class ZstIo {
	public static Gson gson = new Gson();
	public static JsonParser jsonParser = new JsonParser();

	public static void send(Socket socket, String method) {
		ZstIo.send(socket, method, null);
	}

	public static void send(Socket socket, String method, ZstMethod methodData)
	{
		ZMsg message = new ZMsg();
		message.add(method);
		if(methodData != null)
		{
			String data = ZstIo.gson.toJson(methodData.asMap());
			message.add(data);
		} else {
			message.add("{}");
		}
		
		if(!message.send(socket))
			System.out.println("Sending failed!");
	}

	public static MethodMessage recv(Socket socket) {
		return recv(socket, false);
	}

	public static MethodMessage recv(Socket socket, Boolean noWait) {

		ZMsg message = null;
		if (!noWait)
			message = ZMsg.recvMsg(socket);
		else
			message = ZMsg.recvMsg(socket, ZMQ.DONTWAIT);

		String data = message.getLast().toString();
		JsonObject mData = (JsonObject) ZstIo.jsonParser.parse(data);

		return new MethodMessage(message.getFirst().toString(),
				ZstMethod.jsonObjToZstMethod(mData));
	}
}
