import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Page  implements Serializable {

    Vector<Hashtable> list;
    int listNumber;
    String PageID;
    public Page(String tableName ){
        list =new Vector<Hashtable>();

        PageID=tableName;

    }

}