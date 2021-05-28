import java.util.Hashtable;
import java.util.Locale;
import java.util.Vector;

public class Index {
    String tableName;
    String[] colNames;
    Hashtable<String,Vector> ranges;
    String indexId;
    public Index(String tableName, String[] columnNames){
        this.tableName=tableName;
        colNames=columnNames;
        indexId="IDX_"+tableName;
        for (int i=0;i<columnNames.length;i++){
            indexId+="_"+columnNames[i];
        }
    ranges=new Hashtable<String,Vector>();
        for (int i=0;i<colNames.length;i++)
            ranges.put(columnNames[i],new Vector());
    }

}
