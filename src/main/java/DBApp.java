import org.junit.jupiter.api.parallel.Resources;

import java.io.*;
import java.util.*;


public class DBApp implements DBAppInterface{

    Vector<Table> tableList;
    Vector<String> tableNames;

    public DBApp (){
       Vector<Table> tableList= new Vector<Table>();
        tableNames = new Vector<>();

    }

    @Override
    public void init() {

        try
        {
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream("src/main/resources/Tables.bin");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(tableList);

            out.close();
            file.close();

            System.out.println("Object has been serialized");

        }

        catch(IOException ex)
        {
            System.out.println("IOException is caught");
        }

    }

    public void createTable(String tableName, String clusteringKey, Hashtable<String,String> colNameType,
                            Hashtable<String,String> colNameMin, Hashtable<String,String> colNameMax) throws DBAppException, IOException {
        Vector<String> colName=new Vector<>();
        try {
            String columnName = "";
            Set<String> columns = colNameType.keySet();
            for (String k : columns) {
                colName.add(k);
                columnName = k;
                if (!(colNameMax.containsKey(k) && colNameMin.containsKey(k)))
                    throw new DBAppException("invalid input");

            }
            FileWriter csvWriter = new FileWriter("src/main/resources/metadata.csv", true);
            csvWriter.write(getAttributes(tableName, clusteringKey, colNameType, colNameMin, colNameMax));
            csvWriter.flush();
            csvWriter.close();
            Table table = new Table(tableName, clusteringKey);
            this.tableList.add(table);
            this.tableNames.add(tableName);
            serializeTable(table);
        }
        catch(Exception e){
        System.out.print(e.getMessage());
        }
    /*    FileOutputStream fout=new FileOutputStream("src/main/resources/metadata.csv");
        String data =getAttributes(tableName,clusteringKey,colNameType,colNameMin,colNameMax);
        fout.write(data.getBytes(), 0, data.length());
*/
    }

    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {

    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        Vector<Table> tables = null;
        int maxRows = 0;
        try {
            maxRows = readConfig("MaximumRowsCountinPage");
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream("src/main/resources/Tables.bin");
            ObjectInputStream in = new ObjectInputStream(file);
            // Method for deserialization of object
            tables = (Vector<Table>) in.readObject();
            in.close();
            file.close();

            System.out.println("Object has been deserialized ");
        } catch (IOException ex) {
            System.out.println("IOException is caught");
        } catch (ClassNotFoundException ex) {
            System.out.println("ClassNotFoundException is caught");
        }
        Table target = null;
        for (int i = 0; i < tables.size(); i++) {
            if (tables.get(i).tableName.equals(tableName)) {
                target = tables.get(i);
                break;
            }
            if (target == null)
                throw new DBAppException("Table not found");
        }
        String currentClustering =colNameValue.get(target.clusteringKey).toString();

        //table has no pages case
        if (target.pages.size() == 0) {

            Page page = new Page(tableName + (target.pages.size()));
            checkDataTypes(tableName, colNameValue);
            page.list.add(colNameValue);
            serializePage(page);
            target.pages.add(page);
            target.pages.get(0).clusterings.add(colNameValue.get(target.clusteringKey).toString());
            target.min.add(colNameValue.get(target.clusteringKey).toString());
            target.max.add(colNameValue.get(target.clusteringKey).toString());
        }
        else{
            //finding the target page
            int pageIndex = 0;
            int k;
            for( k=0;k<target.pages.size();k++){
                if(currentClustering.compareTo(target.min.get(k))==-1){
                    String currentPath = "src/main/resources"+"/"+target.tableName+k+".bin";
                    Page currentPage=deserialize(currentPath);

                    if (currentPage.list.size()<maxRows){
                        pageIndex=k;
                        break;
                    }
                    else {
                        // shifting one row down and inserting in current page
                        if (!(target.pages.size()-1==k)&& target.pages.get(k+1).list.size()<maxRows){

                            currentPath = "src/main/resources"+"/"+target.tableName+k+".bin";
                           String nextPath = "src/main/resources"+"/"+target.tableName+(k+1)+".bin";
                            currentPage=deserialize(currentPath);
                           Page nextPage =deserialize(nextPath);
                            Hashtable targetElement = currentPage.list.get( currentPage.list.size()-1);
                           nextPage.list.insertElementAt(targetElement,0);
                            nextPage.clusterings.insertElementAt(targetElement.get(target.clusteringKey).toString(),0);
                            target.min.set(k+1,targetElement.get(target.clusteringKey).toString());
                            currentPage.list.remove(maxRows);
                            pageIndex=k;
                            serializePage(nextPage);
                            break;
                    }else{
                            // in CASE THAT THE CURRENT PAGE IS THE LAST PAGE , THEN WE CREATE NEW PAGE AND SHIFT ONE ROW DOWN
                            if(target.pages.size()-1==k){
                        Page newPage = new Page(target.tableName+target.pages.size());
                        target.pages.add(newPage);
                         currentPath = "src/main/resources"+"/"+target.tableName+k+".bin";
                             currentPage=deserialize(currentPath);
                            Hashtable targetElement = currentPage.list.get( currentPage.list.size()-1);
                            newPage.list.insertElementAt(targetElement,0);
                            newPage.clusterings.insertElementAt(targetElement.get(target.clusteringKey).toString(),0);
                            target.min.add(targetElement.get(target.clusteringKey).toString());
                            target.max.add(targetElement.get(target.clusteringKey).toString());
                            currentPage.list.remove(maxRows);
                            pageIndex=k;
                            serializePage(newPage);
                            break;
                        }else {
                            // Using overflow pages
                                 currentPath = "src/main/resources"+"/"+target.tableName+k+".bin";
                                 currentPage=deserialize(currentPath);
                                if(currentPage.overflowPage==null)
                                currentPage.overflowPage=new Page(currentPage.PageID+"Over");
                                currentPage.overflowPage.list.add(colNameValue);
                                currentPage.overflowPage.clusterings.add(colNameValue.get(target.clusteringKey).toString());
                                serializePage(currentPage.overflowPage);
                                serializePage(currentPage);
                                return;

                        }
                    }


                    }
                } else {
                    // REPEATING A SIMILAR CODE FOR DIFFERENT CASE

                if (currentClustering.compareTo(target.min.get(k))==1 && currentClustering.compareTo(target.max.get(k))==-1 ){
                    String currentPath = "src/main/resources"+"/"+target.tableName+k+".bin";
                    Page currentPage=deserialize(currentPath);

                        if (target.pages.get(k).list.size()<maxRows){
                            pageIndex=k;
                        break;}
                        else {
                            // shifting one row down and inserting in current page
                            if (!(target.pages.size()-1==k)&& target.pages.get(k+1).list.size()<maxRows){

                                 currentPath = "src/main/resources"+"/"+target.tableName+k+".bin";
                                String nextPath = "src/main/resources"+"/"+target.tableName+(k+1)+".bin";
                                 currentPage=deserialize(currentPath);
                                Page nextPage =deserialize(nextPath);
                                Hashtable targetElement = currentPage.list.get( currentPage.list.size()-1);
                                nextPage.list.insertElementAt(targetElement,0);
                                nextPage.clusterings.insertElementAt(targetElement.get(target.clusteringKey).toString(),0);
                                target.min.set(k+1,targetElement.get(target.clusteringKey).toString());
                                currentPage.list.remove(maxRows);
                                pageIndex=k;
                                serializePage(nextPage);
                                break;
                            }else {
                                // in CASE THAT THE CURRENT PAGE IS THE LAST PAGE , THEN WE CREATE NEW PAGE AND SHIFT ONE ROW DOWN
                                if (target.pages.size() - 1 == k) {
                                    Page newPage = new Page(target.tableName + target.pages.size());
                                    target.pages.add(newPage);
                                     currentPath = "src/main/resources" + "/" + target.tableName + k + ".bin";
                                     currentPage = deserialize(currentPath);
                                    Hashtable targetElement = currentPage.list.get(currentPage.list.size() - 1);
                                    newPage.list.insertElementAt(targetElement, 0);
                                    newPage.clusterings.insertElementAt(targetElement.get(target.clusteringKey).toString(), 0);
                                    target.min.add(targetElement.get(target.clusteringKey).toString());
                                    target.max.add(targetElement.get(target.clusteringKey).toString());
                                    currentPage.list.remove(maxRows);
                                    pageIndex = k;
                                    serializePage(newPage);
                                    break;

                                } else {
                                    // Using overflow pages
                                     currentPath = "src/main/resources" + "/" + target.tableName + k + ".bin";
                                     currentPage = deserialize(currentPath);
                                    if (currentPage.overflowPage == null)
                                        currentPage.overflowPage = new Page(currentPage.PageID + "Over");
                                    currentPage.overflowPage.list.add(colNameValue);
                                    currentPage.overflowPage.clusterings.add(colNameValue.get(target.clusteringKey).toString());
                                    serializePage(currentPage.overflowPage);
                                    serializePage(currentPage);
                                    return;

                                }
                            }}

                    } else {
                    //  HANDLING SOME DELETION CASES WHERE THERE EXIST SOME SPACE IN PREVIOUS PAGE
                    String currentPath = "src/main/resources"+"/"+target.tableName+k+".bin";
                    Page currentPage=deserialize(currentPath);

                    if (target.pages.size()-1>k){
                       if( target.min.get(k+1).compareTo(colNameValue.get(target.clusteringKey).toString())==1 )
                           if(currentPage.list.size()<maxRows){
                               pageIndex=k;
                               break;
                    }
                    }



                }

                //INSERTING INTO THE TARGET PAGE
            }
            }

            if (k==target.pages.size()){
                Page newPage = new Page(target.tableName + target.pages.size());
                newPage.list.add(colNameValue);
                newPage.clusterings.add(colNameValue.get(target.clusteringKey).toString());
                target.min.add(colNameValue.get(target.clusteringKey).toString());
                target.max.add(colNameValue.get(target.clusteringKey).toString());
                serializePage(newPage);}
             else{
                Page currentPage = target.pages.get(pageIndex);
                int x =0;
                int idx = Collections.binarySearch(currentPage.clusterings,colNameValue.get(target.clusteringKey).toString());
                if(idx>0)
                    throw new DBAppException("you entered a primary key that already exists");
                else{
                    x= -1-idx;

                    currentPage.list.insertElementAt(colNameValue,x);
                    //inserting the clustering key at the clusterings vector
                    currentPage.clusterings.insertElementAt((String) colNameValue.get(target.clusteringKey.toString()),x);

                    if(currentClustering.compareTo((String) target.min.get(0))==-1){
                        target.min.set(0,currentClustering);
                    }
                    if(currentClustering.compareTo(target.max.get(0))==1)
                        target.max.set(0,currentClustering);
                }
                }



                }
            }




    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {

    }

    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        Page p = getPage(tableName,columnNameValue);
        int rowIdx = getRow(p, columnNameValue, tableName);
        p.list.removeElementAt(rowIdx);

    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        return null;
    }

    public static String getAttributes ( String tableName, String clusteringKey, Hashtable<String,String> colNameType,
                                         Hashtable<String,String> colNameMin, Hashtable<String,String> colNameMax){
        String columnName = "";
        String columnType ="";
        String columnMin ="";
        String columnMax ="";
        String clustering;
        String result="" ;
        Set<String> columns  = colNameType.keySet();
        for(String k : columns){

                    columnName = k;
                    columnType = colNameType.get(k);
                    columnMax = colNameMax.get(k);
                    columnMin = colNameMin.get(k);
                    if (clusteringKey.equals(k))
                        clustering = "TRUE";
                    else
                        clustering = "FALSE";

                    result = result + '\n' + tableName + "," + columnName + "," + columnType + "," + clustering + "," + "FALSE" + ","
                            + columnMin + "," + columnMax;

                }

        return result;


    }

    public static void checkDataTypes (String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {

        //  #####   parsing a CSV file into vector of String[]  #####

        Object min;
        Object max;
        Vector<String[]> Data =new Vector<String[]>();
        String line = "";
        String splitBy = ",";
        int i =0;
        try
        {

            BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            while ((line = br.readLine()) != null)   //returns a Boolean value
            {
                String[] array= line.split(splitBy);
                Data.add(array);

            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
            for(int j =0 ;j<Data.size();j++){
                if(Data.get(j)[0].equals(tableName)) {
                    min = Data.get(j)[Data.get(j).length - 2];
                    String colName = Data.get(j)[1];
                    if (colNameValue.get(colName) != null) {
                        if (colNameValue.get(colName).toString().compareTo(min.toString()) == -1) {
                            throw new DBAppException("you entered value below minimum");
                        }
                        max = Data.get(j)[Data.get(j).length - 1];
                        if (colNameValue.get(colName).toString().compareTo(max.toString()) == 1) {
                            throw new DBAppException("you entered value above maximum");
                        }
                    }
                }
            }

        //  ######   looping over table attributes   #######
        String columnName="";
        Set<String> columns  = colNameValue.keySet();
        for(String k : columns){
            columnName = k;
            String dataType =getType(colNameValue.get(k));

            try {
                if (dataType.equals( "NA"))
                    throw new DBAppException("invalid Data Type");
                String CSVType="";

                for (int j=0;j<Data.size();j++){
                    String[] attributes =Data.get(j);
                    if (attributes[0].equals(tableName)&& attributes[1].equals(columnName))
                        CSVType =attributes[2];
                }
                if (!CSVType.equals(dataType))
                    throw new DBAppException("invalid Data Type");



            }catch (Exception e){
                System.out.print(e.getMessage());
            }



        }


    }

    public static String getType (Object o){
        if (o instanceof String)
            return "java.lang.String";
        else if(o instanceof Integer)
            return "java.lang.Integer";
        else if (o instanceof Double)
            return "java.lang.Double";
        else  if (o instanceof Date)
            return "java.util.Date";
        else
            return "NA";
    }

public static void serializePage(Page p){
        String pageName = p.PageID;
    try
    {
        //Saving of object in a file
        FileOutputStream file = new FileOutputStream("src/main/resources/"+pageName+".bin");
        ObjectOutputStream out = new ObjectOutputStream(file);

        // Method for serialization of object
        out.writeObject(p);

        out.close();
        file.close();

        System.out.println("Object has been serialized");

    }

    catch(IOException ex)
    {
        System.out.println("IOException is caught");
    }

}


public static Page deserialize(String filePath){
            Page page = null;
            try {
                // Reading the object from a file
                FileInputStream file = new FileInputStream(filePath);
                ObjectInputStream in = new ObjectInputStream(file);
                // Method for deserialization of object
                page = (Page) in.readObject();
                in.close();
                file.close();

                System.out.println("Object has been deserialized ");
            } catch (IOException ex) {
                System.out.println("IOException is caught");
            } catch (ClassNotFoundException ex) {
                System.out.println("ClassNotFoundException is caught");
            }
            return page;
        }


public int readConfig(String key) throws IOException {
        int nRows=0;
    FileReader reader=new FileReader("src/main/resources/DBApp.config");

    Properties p=new Properties();
    p.load(reader);
    return Integer.parseInt(p.getProperty(key));
}
    public static void serializeTable(Table table){
        String tableName = table.tableName;
        try
        {
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream("src/main/resources/"+tableName+".bin");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(table);

            out.close();
            file.close();

            System.out.println("Object has been serialized");

        }

        catch(IOException ex)
        {
            System.out.println("IOException is caught");
        }

    }
    public static Table deserializeTable(String tableName){
        Table table = null;
        String filePath = "src/main/resources/"+tableName+".bin";
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(filePath);
            ObjectInputStream in = new ObjectInputStream(file);
            // Method for deserialization of object
            table = (Table) in.readObject();
            in.close();
            file.close();

            System.out.println("Object has been deserialized ");
        } catch (IOException ex) {
            System.out.println("IOException is caught");
        } catch (ClassNotFoundException ex) {
            System.out.println("ClassNotFoundException is caught");
        }
        return table;
    }
    public Page getPage(String tableName , Hashtable<String,Object> colNameValue) throws DBAppException {
        Page p = null;
        //table doesnt exist
        if(!this.tableNames.contains(tableName))
            throw new DBAppException("table doesn't exist!");
        else {
            Table t = (Table) deserializeTable(tableName);
            String clustering = t.clusteringKey;
            //I have the primary key and can search with it
            if(colNameValue.get(clustering)!=null){
                int x =   Collections.binarySearch(t.min,colNameValue.get(clustering).toString());
                //the pk is the min at that page
                if(x>0){
                    deserialize("src/main/resources/"+tableName+x+".bin");
                    p = t.pages.get(x);
                }
                else{
                    int c = x-1;
                    deserialize("src/main/resources/"+tableName+c+".bin");

                    p=t.pages.get(x-1);
                }
            }
            else{
                //looping over all table pages
                for(int i =0 ; i<t.pages.size();i++){
                    String pageID =t.tableName+i;
                    String filePath = "src/main/resources/"+pageID+".bin";
                    p = deserialize(filePath);
                    //looping over each page
                    for(int j =0;j<p.list.size();j++){
                        String currRow = p.list.get(j).toString();
                        String entry = colNameValue.toString();
                        if(currRow.contains(entry))
                            return p;
                    }
                }
            }
        }

        return p;}
    public int getRow (Page p , Hashtable<String,Object> colNameValue,String clusteringKey) throws DBAppException {
        int idx=0;
        //table doesnt exist
         //I have the primary key and can search with it
            if(colNameValue.get(clusteringKey)!=null){
                //getting the index by the clustering key
                idx = Collections.binarySearch(p.clusterings,colNameValue.get(clusteringKey).toString());
                if(idx>0)
                    return idx;
                else if(p.overflowPage!=null &&p.overflowPage.list.size()!=0){
                    int x = Collections.binarySearch(p.overflowPage.clusterings, colNameValue.get(clusteringKey).toString());
                    if(x>0){
                        return x;
                    }
                    else
                        throw new DBAppException("The entered row doesn't exist");

                }
                else
                    throw new DBAppException("The entered row doesn't exist");
            }
            else{

                for(int i =0; i<p.list.size();i++){
                    String currRow = p.list.get(i).toString();
                    String entry = colNameValue.toString();
                    if(currRow.contains(entry))
                        return i;
                }
                if(p.overflowPage!=null &&p.overflowPage.list.size()!=0){
                    for(int i =0; i<p.overflowPage.list.size();i++){
                        String currRow = p.overflowPage.list.get(i).toString();
                        String entry = colNameValue.toString();
                        if(currRow.contains(entry))
                            return i;
                    }
                }

                }


        return idx;}
 public static void main (String[] args) throws DBAppException, IOException {
       Hashtable table = new Hashtable<String,String>();

     table.put("Pen", "soso");
     table.put("Book", "lolo");
     table.put("Clothes", "momo");
     table.put("Mobile", "koko");
     table.put("Booklet", "hoho");

     Hashtable table1 = new Hashtable<String,String>();

     table1.put("Pen", "soso2");
     table1.put("Book", "lolo2");
     table1.put("Clothes", "momo2");
     table1.put("Mobile", "koko2");
     table1.put("Booklet", "hoho2");

     Hashtable table2 = new Hashtable<String,String>();

     table2.put("Pen", "soso3");
     table2.put("Book", "lolo3");
     table2.put("Clothes", "momo3");
     table2.put("Mobile", "koko3");
     table2.put("Booklet", "hoho3");

     DBApp db =new DBApp();
   db.init();
     db.createTable("marvelous","Pen",table,table1,table2);


    /* FileWriter cw = new FileWriter("src/main/resources/metadata.csv",true);
     String[] line = {"4", "David", "USA"};
     //Writing data to the csv file
    cw.write("lolo");
     //close the file
     cw.close();*/
}}
