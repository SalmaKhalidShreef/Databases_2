import java.io.Serializable;
import java.util.Vector;

public class Table implements Serializable {
    String tableName;
    transient Vector<Page> pages;
    Vector<String> min ;
    Vector <String>max;
    String clusteringKey;
    Vector<String> colName;
    public Table (String name,String clustering){
        tableName = name;
        pages = new Vector<Page>();
        clusteringKey = clustering;
        min = new Vector<String>();
        max= new Vector<String>();

    }
}
