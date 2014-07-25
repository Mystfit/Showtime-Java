package ZST;

public class MethodMessage {
	public String method;
	public ZstMethod data;
	public MethodMessage(String methodName, ZstMethod methodData)
	{
		method = methodName;
		data = methodData;
	}
}
