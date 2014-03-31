import org.la4j.vector.sparse.CompressedVector;
public class TextContextPair {
	public CompressedVector contextVector = null;
	public String text = null;
	
	public TextContextPair(CompressedVector contextVector, String text){
		this.contextVector = contextVector;
		this.text = text;
	}
	
}
