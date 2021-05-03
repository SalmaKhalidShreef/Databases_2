import org.junit.jupiter.api.parallel.Resources;
import org.junit.validator.ValidateWith;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class DBApp implements DBAppInterface {

    Vector<String> tableList;
    Vector<String> tableNames;

    public DBApp (){
        tableList= new Vector<String>();
        tableNames = new Vector<String>();

    }
    private static final String FOLDER ="src/main/resources/data";

    @Override
    public void init() {
        FileWriter csvWriter = null;
        //File data = new File("src/main/resources");
        File newFolder = new File(FOLDER);

        boolean created =  newFolder.mkdir();

        if(created)
            System.out.println("Folder was created !");
        else
            System.out.println("Unable to create folder");

        String title = "Table Name, Column Name, Column Type, ClusteringKey, Indexed, min, max";
        try {
            csvWriter = new FileWriter("src/main/resources/metadata.csv", true);

        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            csvWriter.write(title);
            csvWriter.flush();
            csvWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


        /*try
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
        }*/

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
            Vector <String> tableNames = deserializeVector("src/main/resources/data/tableNames.bin");
            if(tableNames.contains(tableName))
                throw new DBAppException("A table with this name already exists");
            Vector <String> tableList = deserializeVector("src/main/resources/data/tablesList.bin");
            tableList.add("src/main/resources/data/"+tableName+".bin");
            serializetableList(tableList);

            tableNames.add(tableName);
            serializetableNames(tableNames);
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
    /*    FileOutputStream fout=new FileOutputStream("src/main/resources/metadata.csv");
        String data =getAttributes(tableName,clusteringKey,colNameType,colNameMin,colNameMax);
        fout.write(data.getBytes(), 0, data.length());
*/
    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {

    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        colNotFound(tableName,colNameValue);
        Vector<String> tables = null;
        int maxRows = 0;
        try {
            maxRows = readConfig("MaximumRowsCountinPage");
        } catch (IOException e) {
            e.printStackTrace();
        }
        /*try {
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
        }*/
        Table target = null;
        tables = deserializeVector("src/main/resources/data/tablesList.bin");
        tableNames = deserializeVectorS("src/main/resources/data/tableNames.bin");
        System.out.println(tables.size());
        System.out.println(tableNames.size());
        for (int i = 0; i < tables.size(); i++) {
            if (tableNames.get(i).compareTo(tableName)==0) {
                String filePath =tables.get(i);
                System.out.println(filePath);
                target = DeserializeTable(filePath);
                break;
            }

            }
        if (target == null)
            throw new DBAppException("Table not found");
        String currentClustering =colNameValue.get(target.clusteringKey).toString();
        //table has no pages case
        if (target.pagesPath.size()==0) {
            Page page = new Page(tableName + "0");
            try {
                checkDataTypes(tableName, colNameValue);
            }
            catch (Exception ex){
                System.out.println(ex.getMessage());
            }
            page.list.add(colNameValue);
            page.clusterings.add(colNameValue.get(target.clusteringKey).toString());
            serializePage(page);
            //"src/main/resources/data/"+pageName+".bin"
            target.pagesPath.add("src/main/resources/data/"+page.PageID+".bin");
            target.min.add(colNameValue.get(target.clusteringKey).toString());
            target.max.add(colNameValue.get(target.clusteringKey).toString());
        }
        else{
            //finding the target page
            int pageIndex = 0;
            int k;
            for( k=0;k<target.pagesPath.size();k++){
                if(currentClustering.compareTo(target.min.get(k))==-1){
                    String currentPath = "src/main/resources/data"+"/"+target.tableName+k+".bin";
                    Page currentPage=deserialize(currentPath);
                    if(currentPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                        throw new DBAppException("there is an entry with this primary Key !");
                    if (currentPage.list.size()<maxRows){
                        pageIndex=k;
                        break;
                    }
                    else {
                        String nextPath = "src/main/resources/data"+"/"+target.tableName+(k+1)+".bin";
                        Page nextPage =deserialize(nextPath);
                        if(nextPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                            throw new DBAppException("there is an entry with this primary Key !");
                        // shifting one row down and inserting in current page
                        if (!(target.pagesPath.size()-1==k)&& nextPage.list.size()<maxRows){

                            currentPath = "src/main/resources/data"+"/"+target.tableName+k+".bin";
                            currentPage=deserialize(currentPath);
                            if(currentPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                                throw new DBAppException("there is an entry with this primary Key !");
                            Hashtable targetElement = currentPage.list.get( currentPage.list.size()-1);
                            if(nextPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                                throw new DBAppException("there is an entry with this primary Key !");
                           nextPage.list.insertElementAt(targetElement,0);
                            nextPage.clusterings.insertElementAt(targetElement.get(target.clusteringKey).toString(),0);
                            target.min.set(k+1,targetElement.get(target.clusteringKey).toString());
                            currentPage.list.remove(maxRows);
                            pageIndex=k;
                            serializePage(nextPage);
                            break;
                    }else{
                            // in CASE THAT THE CURRENT PAGE IS THE LAST PAGE , THEN WE CREATE NEW PAGE AND SHIFT ONE ROW DOWN
                            if(target.pagesPath.size()-1==k){
                        Page newPage = new Page(target.tableName+target.pagesPath.size());
                        target.pagesPath.add(newPage.PageID);
                         currentPath = "src/main/resources/data"+"/"+target.tableName+k+".bin";
                             currentPage=deserialize(currentPath);
                                if(currentPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                                    throw new DBAppException("there is an entry with this primary Key !");
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
                                 currentPath = "src/main/resources/data"+"/"+target.tableName+k+".bin";
                                 currentPage=deserialize(currentPath);
                                if(currentPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                                    throw new DBAppException("there is an entry with this primary Key !");
                                if(currentPage.overflowPage==null)
                                currentPage.overflowPage=new Page(currentPage.PageID+"Over");
                                /////////////////////////////////////////////////////////////
                                int x =0;
                                int idx = Collections.binarySearch(currentPage.overflowPage.clusterings, colNameValue.get(target.clusteringKey).toString());
                                if(idx>0)
                                    throw new DBAppException("you entered a primary key that already exists");
                                else {
                                    x = -1 - idx;

                                    currentPage.overflowPage.list.insertElementAt(colNameValue, x);
                                    //inserting the clustering key at the clusterings vector
                                    currentPage.overflowPage.clusterings.insertElementAt((String) colNameValue.get(target.clusteringKey.toString()), x);

                                }
                                /////////////////////////////////////////////////////////////////
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
                    if(currentPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                        throw new DBAppException("there is an entry with this primary Key !");
                        if (currentPage.list.size()<maxRows){
                            pageIndex=k;
                        break;}
                        else {
                            // shifting one row down and inserting in current page
                            String nextPath = "src/main/resources/data"+"/"+target.tableName+(k+1)+".bin";
                            Page nextPage =deserialize(nextPath);
                            if(nextPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                                throw new DBAppException("there is an entry with this primary Key !");
                            if (!(target.pagesPath.size()-1==k)&& nextPage.list.size()<maxRows){
                                 currentPage=deserialize(currentPath);
                                Hashtable targetElement = currentPage.list.get( currentPage.list.size()-1);
                                nextPage.list.insertElementAt(targetElement,0);
                                nextPage.clusterings.insertElementAt(targetElement.get(target.clusteringKey).toString(),0);
                                if(nextPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                                    throw new DBAppException("there is an entry with this primary Key !");
                                target.min.set(k+1,targetElement.get(target.clusteringKey).toString());
                                currentPage.list.remove(maxRows);
                                pageIndex=k;
                                serializePage(nextPage);
                                break;
                            }else {
                                // in CASE THAT THE CURRENT PAGE IS THE LAST PAGE , THEN WE CREATE NEW PAGE AND SHIFT ONE ROW DOWN
                                if (target.pagesPath.size() - 1 == k) {
                                    Page newPage = new Page(target.tableName + target.pagesPath.size());
                                    target.pagesPath.add(newPage.PageID);
                                     currentPath = "src/main/resources/data" + "/" + target.tableName + k + ".bin";
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
                                     currentPath = "src/main/resources/data" + "/" + target.tableName + k + ".bin";
                                     currentPage = deserialize(currentPath);
                                    if(currentPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                                        throw new DBAppException("there is an entry with this primary Key !");
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
                    String currentPath = "src/main/resources/data"+"/"+target.tableName+k+".bin";
                    Page currentPage=deserialize(currentPath);
                    if(currentPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                        throw new DBAppException("there is an entry with this primary Key !");
                    if (target.pagesPath.size()-1>k){
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

            if (k==target.pagesPath.size()){
                Page newPage = new Page(target.tableName + target.pagesPath.size());
                newPage.list.add(colNameValue);
                newPage.clusterings.add(colNameValue.get(target.clusteringKey).toString());
                target.min.add(colNameValue.get(target.clusteringKey).toString());
                target.max.add(colNameValue.get(target.clusteringKey).toString());
                serializePage(newPage);}
             else{
                 String path = target.pagesPath.get(pageIndex);
                Page currentPage = deserialize(path);
                if(currentPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                    throw new DBAppException("there is an entry with this primary Key !");
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

        colNotFound(tableName,columnNameValue);
        try {
            checkDataTypes(tableName,columnNameValue);
            int i;

            // String s="s/"
            Boolean flag = false;

            Table table = null;
            Vector<String> tableList= (Vector<String>) deserializeVector("src/main/resources/data/tablesList.bin");
            for (i = 0; i < tableList.size(); i++) {
                table = DeserializeTable("src/main/resources/data/"+tableName+".bin");
                if (table.tableName.equals(tableName)) {
                    if(columnNameValue.get(table.clusteringKey)!=null)
                        throw  new DBAppException("You can't update clustering key !");
                    flag = true;
                    break;
                }


            }
            if (flag) {
                Boolean flagfoundpage = false;
                int idx = -1;
                Page page;
                int j;
                for (j = 0; j < table.min.size(); j++) {
                    if (clusteringKeyValue.compareTo((table.min.get(j)).toString()) >= 0 &&
                            clusteringKeyValue.compareTo((table.max.get(j)).toString()) <= 0) {
                        page = deserialize("src/main/resources/data" + "/" + table.tableName + (j + 1) + ".bin");
                        //idx = Collections.binarySearch(page.clusterings,clusteringKeyValue);
                        if(!page.clusterings.contains(clusteringKeyValue)){
                            if(page.overflowPage!=null){
                                Page overflow = deserialize("src/main/resources/data" + "/" + table.tableName + (j + 1) +"Over"+ ".bin");
                                if(overflow.clusterings.contains(clusteringKeyValue)){
                                    String s = (String) (columnNameValue.keySet().toArray())[0];
                                    columnNameValue.replace(s, columnNameValue.get(s));
                                    flagfoundpage = true;
                                    break;
                                }
                            }
                        }
                        else if(page.clusterings.contains(clusteringKeyValue)) {
                            String s = (String) (columnNameValue.keySet().toArray())[0];
                            columnNameValue.replace(s, columnNameValue.get(s));
                            flagfoundpage = true;
                        }

                        serializePage(page);
                        break;
                    }
                }
                if (!flagfoundpage) {
                    throw new DBAppException("Row not found");
                }

            } else {
                throw new DBAppException("The table does not exist");
            }
            serializeTable(table);
        }catch (Exception e){
            System.out.println(e.getMessage());
        }

    }

    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
       // colNotFound(tableName,columnNameValue);
        Page p = getPage(tableName, columnNameValue);
        if (p != null) {
            try {
                int rowIdx = getRow(p, columnNameValue, tableName);
                p.list.removeElementAt(rowIdx);
            } catch (Exception x) {
                System.out.println(x.getMessage());
            }
        }
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

    public static void checkDataTypes (String tableName, Hashtable<String, Object> colNameValue) throws DBAppException, ParseException {

        //  #####   parsing a CSV file into vector of String[]  #####

        String min;
        String max;
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
        boolean tableFound = false;
        boolean colFound = false;
            for(int j =0 ;j<Data.size();j++){
                if(Data.get(j)[0].equals(tableName)) {
                    min = Data.get(j)[Data.get(j).length - 2];
                    tableFound=true;
                    String colName = Data.get(j)[1];
                    if (colNameValue.get(colName) != null) {
                        colFound= true;
                        if(colNameValue.get(colName) instanceof  Date){
                            Date d = (Date)colNameValue.get(colName);
                            DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd");
                            String strDate = dateFormat.format(d);
                            Date date1=new SimpleDateFormat("yyyy-mm-dd").parse(min);
                            Date date2 = new SimpleDateFormat("yyyy-mm-dd").parse(strDate);
                            //System.out.println(date1);
                            //System.out.println(date2);
                            if (date2.compareTo(date1) == -1)
                                throw new DBAppException("you entered date below minimum");
                            max = Data.get(j)[Data.get(j).length - 1];
                            Date datemax=new SimpleDateFormat("yyyy-mm-dd").parse(max);
                            if (date2.compareTo(datemax) == 1)
                                throw new DBAppException("you entered date above maximum");
                        }
                        else {
                            if (colNameValue.get(colName).toString().compareTo(min) == -1) {
                                throw new DBAppException("you entered value below minimum");
                            }
                            max = Data.get(j)[Data.get(j).length - 1];
                            if (colNameValue.get(colName).toString().compareTo(max) == 1) {
                                throw new DBAppException("you entered value above maximum");
                            }
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
        FileOutputStream file = new FileOutputStream("src/main/resources/data/"+pageName+".bin");
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
            FileOutputStream file = new FileOutputStream("src/main/resources/data/"+tableName+".bin");
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
    /*public static Table deserializeTable(String tableName){
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
    }*/
    public Page getPage(String tableName , Hashtable<String,Object> colNameValue) throws DBAppException {
        Page p = null;
        //table doesnt exist
        Vector<String> tableNames = deserializeVector("src/main/resources/data/tableNames.bin");
        if(!tableNames.contains(tableName))
            throw new DBAppException("table doesn't exist!");
        else {
            serializetableNames(tableNames);
             String filePath = "src/main/resources/data/"+tableName+".bin";
            Table t = (Table) DeserializeTable(filePath);
            String clustering = t.clusteringKey;
            //I have the primary key and can search with it
            if(colNameValue.get(clustering)!=null){
                int x =   Collections.binarySearch(t.min,colNameValue.get(clustering).toString());
                //the pk is the min at that page
                if(x>0){


                    p = deserialize("src/main/resources/data/"+tableName+x+".bin");
                }
                else{
                    int c = x-1;

                    p=deserialize("src/main/resources/data/"+tableName+c+".bin");

                }
            }
            else{
                //looping over all table pages
                for(int i =0 ; i<t.pagesPath.size();i++){
                    String pageID =t.tableName+i;
                    String filePath1 = "src/main/resources/data/"+pageID+".bin";
                    p = deserialize(filePath1);
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



    public Table DeserializeTable (String path){
        Table table = null;
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(path);
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
    public  void serializeTableFunction(Table t){
        try
        {
            Vector<String> tableList = deserializeVectorS("src/main/resources/data/tablesList.bin");
            int vectorSize=tableList.size();
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream("src/main/resources/data/"+t.tableName+vectorSize+".bin");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(t);

            out.close();
            file.close();

            System.out.println("Object has been serialized");

        }

        catch(IOException ex)
        {
            System.out.println("IOException is caught");
        }



    }
    public static void serializetableList(Vector<String> v ){
        try
        {
            int vectorSize=v.size();
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream("src/main/resources/data/tablesList.bin");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(v);

            out.close();
            file.close();

            System.out.println("Object has been serialized");

        }

        catch(IOException ex)
        {
            System.out.println("IOException is caught");
        }

    }

    public static Vector<String> deserializeVector(String filePath){
        Vector<String> v = new Vector<>();
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(filePath);
            ObjectInputStream in = new ObjectInputStream(file);
            // Method for deserialization of object
            v = (Vector<String>) in.readObject();
            in.close();
            file.close();

            System.out.println("Object has been deserialized ");
        } catch (IOException ex) {
            System.out.println("IOException is caught");
        } catch (ClassNotFoundException ex) {
            System.out.println("ClassNotFoundException is caught");
        }
        return v;


    }
    public static void serializetableNames(Vector<String> v ){
        try
        {
            int vectorSize=v.size();
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream("src/main/resources/data/tableNames.bin");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(v);

            out.close();
            file.close();

            System.out.println("Object has been serialized");

        }

        catch(IOException ex)
        {
            System.out.println("IOException is caught");
        }

    }
    public static Vector<String> deserializeVectorS(String filePath){
        Vector<String> v = new Vector<>();
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(filePath);
            ObjectInputStream in = new ObjectInputStream(file);
            // Method for deserialization of object
            v = (Vector<String>) in.readObject();
            in.close();
            file.close();

            System.out.println("Object has been deserialized ");
        } catch (IOException ex) {
            System.out.println("IOException is caught");
        } catch (ClassNotFoundException ex) {
            System.out.println("ClassNotFoundException is caught");
        }
        return v;


    }
    public void colNotFound (String tableName , Hashtable<String,Object> colNameValue) throws DBAppException {
        Vector<String[]> Data =new Vector<String[]>();
        String line = "";
        String splitBy = ",";
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
        HashSet<String> colNames = new HashSet<>();
        for(int i =0 ; i<Data.size();i++){
            if(Data.get(i)[0].equals(tableName)){
                colNames.add(Data.get(i)[1]);
            }
        }
        Set hash_set = colNameValue.keySet();

       if(!colNames.containsAll(hash_set)){
           throw new DBAppException("Columns entered not found in the table!");
       }
    }
    public static void main (String[] args) throws DBAppException, IOException, ParseException {
       /*Hashtable table = new Hashtable<String,String>();

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
        Date x = new Date(2000,4,5);
        DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd");
        String xx = dateFormat.format(x);
        Date d = new Date(1900,2,10);
        String stringdate = "2000-11-21";
        //Date mydate = new Date(stringdate);
        Date date1=new SimpleDateFormat("yyyy-MM-dd").parse(stringdate);
        String dd =dateFormat.format(d);
        System.out.println(date1.compareTo(d));
}}
