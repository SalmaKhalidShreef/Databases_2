import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;

public class DBApp implements DBAppInterface{

    @Override
    public void init() {

    }

    public void createTable(String tableName, String clusteringKey, Hashtable<String,String> colNameType,
                            Hashtable<String,String> colNameMin, Hashtable<String,String> colNameMax) throws DBAppException, IOException {
        FileWriter csvWriter = new FileWriter(tableName+".csv");
        csvWriter.write("Name");
        csvWriter.append(",");
        csvWriter.append("Role");
        csvWriter.append(",");
        csvWriter.append("Topic");
        csvWriter.append("\n");

    }

    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {

    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {

    }

    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {

    }

    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {

    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        return null;
    }
 public static void main (String[] args){
        System.out.print("lolo");
 }
}
