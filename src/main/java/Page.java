import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Page  implements Serializable {
    static int number=0;
    Vector<Hashtable> list;
    int listNumber;
    public Page( ){
        list =new Vector<Hashtable>();
        listNumber = ++number;

    }

}
