import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

public class DBApp1 {
	static int maxRecords;
	static String path = "/Users/mennakhaled/Desktop/";

	public DBApp1() throws IOException {
		maxRecords = 3;
		init();
	}

	public void init( ) throws IOException {
		File meta = new File(path+"MetaData.csv");
		Boolean f = meta.createNewFile();
		File Files = new File(path+"Files.csv");
		Boolean f1 = Files.createNewFile();
	}

	private int config() {
		Properties prop = new Properties();
		String fileName = path+"DBApp.config";
		try (FileInputStream fis = new FileInputStream(fileName)) {
			prop.load(fis);
		} catch (FileNotFoundException ex) {

		} catch (IOException ex) {

		}
		return(Integer.parseInt(prop.getProperty("MaximumRowsCountinTablePage")));
	}
	
	
	public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType, 
			Hashtable<String,String> htblColNameMin, Hashtable<String,String> htblColNameMax ) throws DBAppException, IOException {
		if(tableExists(strTableName))
			throw new DBAppException("table already exists");
		if(htblColNameType.containsKey(strClusteringKeyColumn) && validTypes(htblColNameType)) {
			Enumeration<String> keys = htblColNameType.keys();
			FileWriter fileWriter1 = new FileWriter(path+"MetaData.csv");
			while(keys.hasMoreElements()) {
				String key = keys.nextElement();
				String s= strTableName + ","+ key+ ","+htblColNameType.get(key).toString()+ ",";
				if(key.compareTo(strClusteringKeyColumn)==0)
					s+= "True";
				else
					s+= "False";
				s+=","+null+",";s+=null;
				s+=","+htblColNameMin.get(key).toString()+","+htblColNameMax.get(key).toString();
				fileWriter1.write(s + System.lineSeparator()); 
			}
			fileWriter1.close();
		}
		else 
			throw new DBAppException("clustering key does not exist");
	}
	
	public static boolean validTypes(Hashtable<String,String> htblColNameType) {
		Enumeration<String> values = htblColNameType.elements();		
		while(values.hasMoreElements()) {
			switch(values.nextElement()) {
			case "java.lang.Integer": return true; 
			case "java.lang.Date": return true; 
			case "java.lang.String": return true; 
			case "java.lang.Double": return true; 
			
			}
			return false;
		}
		return false;
	}

	public static boolean tableExists(String strTableName) throws IOException {
		FileReader fileReader = new FileReader( path+"MetaData.csv" ); 
		@SuppressWarnings("resource")
		BufferedReader reader = new BufferedReader( fileReader ); 
		String strCurrentLine = ""; 
		while( (strCurrentLine = reader.readLine() ) != null ) {
			String[]s = strCurrentLine.split(",");
			if(s[0].compareTo(strTableName)== 0) 
				return true;
		}
		return false;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

	public static void addInNewPage(String strTableName, Hashtable<String,Object> htblColNameValue) throws IOException {
		int c=numOfPages(strTableName);
		File f= new File(path+strTableName+c+1+".csv");
		Enumeration<Object> values = htblColNameValue.elements();
        String s="";
        while( values.hasMoreElements() ){
           s=s+values.nextElement()+"," ;
        }
        FileWriter w= new FileWriter(f);
        w.write(s+System.lineSeparator());
        w.close();
       // pageNum++;
	}
	
	
	
	public static boolean isEmpty(String fileName) throws IOException {
		int c=0;
		FileReader fileReader = new FileReader( fileName ); 
		@SuppressWarnings("resource")
		BufferedReader reader = new BufferedReader( fileReader ); 
		String strText = "", strCurrentLine = ""; 
		while( (strCurrentLine = reader.readLine() ) != null )
			c++;
		if(c==0)
			return true;
		else
			return false;
	}
	

	public static ArrayList<String> typesRead(String tableName) throws IOException, DBAppException {
		FileReader fileReader = new FileReader( path+"MetaData.csv" ); 
		@SuppressWarnings("resource")
		BufferedReader reader = new BufferedReader( fileReader ); 
		String strCurrentLine = ""; 
		ArrayList<String> res = new ArrayList<String>();
		while( (strCurrentLine = reader.readLine() ) != null ) {
			String[]s = strCurrentLine.split(",");
			if(s[0].compareTo(tableName)== 0) {
				res.add(s[1]);
				res.add(s[2]);
				res.add(s[4]);
				res.add(s[5]);
			}
			else 
				throw new DBAppException("table does not exist");
		}
		return res ;
	}



	public static boolean checkInsert(String strTableName, Hashtable<String,Object> htblColNameValue) 
			throws IOException, DBAppException, ParseException {
		Enumeration<String> e = htblColNameValue.keys();
		Enumeration<Object> e1 = htblColNameValue.elements();
		ArrayList<String> types = typesRead(strTableName);
		Boolean f = false;
		for(int i = 0 ; i < types.size()/4 ; i = i+4) {
			switch(types.get(i+1)) {
			case "java.lang.Integer": { 
				Object o = e1.nextElement();
				if(e.nextElement().compareTo(types.get(i)) == 0 && o instanceof Integer && 
						(Integer)(o) >= Integer.parseInt(types.get(i+2)) && 
						(Integer)(o) <= Integer.parseInt(types.get(i+3))  )
						f = true; 
				else 
					return false;
				
				} break;
			case "java.lang.Date": { 
				Object o = e1.nextElement();
				Date min = new SimpleDateFormat("dd/MM/yyyy").parse(types.get(i+2));
				Date max = new SimpleDateFormat("dd/MM/yyyy").parse(types.get(i+3));
				Date val = new SimpleDateFormat("dd/MM/yyyy").parse((String) o);
				if(e.nextElement().compareTo(types.get(i)) == 0 && o instanceof Date && 
						val.compareTo(min) >0 && val.compareTo(max)<0 )
						f = true; 
				else
					return false;
				} break;
				
			case "java.lang.String": { 
				String o = e1.nextElement().toString();
				if(e.nextElement().compareTo(types.get(i)) == 0 && o instanceof String && 
						o.compareTo(types.get(i+2)) <=0 && o.compareTo(types.get(i+3))>=0)
						f = true; 
				else 
					return false;
				} break;
			case "java.lang.Double": { 
				Double o = (double)(e1.nextElement());
				if(e.nextElement().compareTo(types.get(i)) == 0 && o instanceof Double && 
						o > (Double.parseDouble(types.get(i+2))) && o<(Double.parseDouble(types.get(i+3))))
						f = true; 
				else 
					return false;
				} break;
			default: return false;
			}
		}
		return f;
			
	}

	public static String checkSpace(String strTableName) throws IOException {
		FileReader fileReader = new FileReader( "MetaData.csv" ); 
		@SuppressWarnings("resource")
		BufferedReader reader = new BufferedReader( fileReader ); 
		String strCurrentLine = ""; 
		while( (strCurrentLine = reader.readLine() ) != null ) {
			String[]s = strCurrentLine.split(",");
			if(s[0].compareTo(strTableName)== 0) 
				if(!isFull(s[4])) 
					return s[4];	  
		}
		return "NA" ;
	}

	public static int numOfPages(String strTableName) throws IOException {
		FileReader fileReader = new FileReader(path+"Files.csv"); 
		@SuppressWarnings("resource")
		BufferedReader reader = new BufferedReader( fileReader ); 
		String strCurrentLine = ""; 
		int count = 0;
		while( (strCurrentLine = reader.readLine() ) != null ) {
			String[]s = strCurrentLine.split(",");
			if(s[0].compareTo(strTableName)== 0) 
				count++;
		}
		return count;
	}
	
	
	public static void findLocation(String strTableName, Hashtable<String,Object> htblColNameValue) throws IOException {
		String [] s = getClusteringKey(strTableName);
		ArrayList<String[]> unsorted = new ArrayList<String[]>();
		int pages = numOfPages(strTableName);
		for(int i = 1; i <=pages ; i++) {
			FileReader fileReader = new FileReader(strTableName+pages+".csv"); 
			@SuppressWarnings("resource")
			BufferedReader reader = new BufferedReader( fileReader ); 
			String strCurrentLine = ""; 
			while( (strCurrentLine = reader.readLine() ) != null ) {
				unsorted.add(strCurrentLine.split(","));
			}
		}
		Object insert = htblColNameValue.get(s[0]);
		Enumeration <Object> e = htblColNameValue.elements();
		String[] values = new String[htblColNameValue.size()];
		for(int i = 0; e.hasMoreElements(); i++) {
			values[i] = (String) e.nextElement();
		}
		
		ArrayList<String[]> sorted = new ArrayList<String[]>();
//		for (int i = 0; i< unsorted.size() ; i++) {
//			if (insert.compareTo((unsorted.get(i))[0])>0){
//				sorted.add(unsorted.get(i));
//			}
//			else {
//				sorted.add(values);
//				for(int j = i ; j< unsorted.size()+1;j++) {
//					sorted.add(unsorted.get(j));
//				}
//				break;
//				
//			}
//		}

		for(int i=0;i<sorted.size();i++) {
			for(int n = 0 ; n < pages ; n++ ) {
				FileWriter fileWriter1 = new FileWriter(strTableName+ pages+".csv");
				for(int j = 0 ; j < 200; j++) {
					fileWriter1.write(sorted.get(i) + System.lineSeparator()); 
				}
				fileWriter1.close();
			}
		}

	}

	public static void insertIntoPage(String FilePath, Hashtable<String,Object> htblColNameValue) throws IOException {
		Enumeration<Object> values = htblColNameValue.elements();
		String v = "";
		if(values.hasMoreElements())
			v+=values.nextElement();
		while(values.hasMoreElements()) {
			v+=","+values.nextElement();
		}
		FileReader fileReader = new FileReader(FilePath); 
		@SuppressWarnings("resource")
		BufferedReader reader = new BufferedReader( fileReader ); 
		String strCurrentLine = ""; 
		while( (strCurrentLine = reader.readLine() ) != null ) {
			String[]s = strCurrentLine.split(",");
			if(s[0].equals("")) {
				FileWriter w = new FileWriter(FilePath);
				w.write(v+System.lineSeparator());
				w.close();
			}
		}
	}
	
	public static int numOfRecords(String strTableName) throws IOException {
		int count = 0;
		for(int i = 0; i < numOfPages(strTableName) ; i++) {
			FileReader fileReader = new FileReader(path+strTableName+i+".csv" ); 
			@SuppressWarnings("resource")
			BufferedReader reader = new BufferedReader( fileReader );
			for(int j = 0 ; j < maxRecords ; j++) {
				if(reader.readLine() != "")
					count++;
			}
			fileReader.close();
			reader.close();
		}
		return count;
	} 

	public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) 
			throws DBAppException, IOException, ParseException {
		if(tableExists(strTableName)) {
			if(checkInsert(strTableName,htblColNameValue)) {
				if(numOfPages(strTableName)<200 && numOfRecords(strTableName)< maxRecords*200) {
					if(PKExists(strTableName,htblColNameValue)) 
						throw new DBAppException("value already exists");
					else
					{
						if(numOfRecords(strTableName) == 0) {
							String s = strTableName +", Table, "+strTableName+1+".csv, "+path+strTableName+1+".csv";
							File n = new File(s);
							Boolean b = n.createNewFile();
							FileWriter fileWriter1 = new FileWriter(path+"Files.csv");
							fileWriter1.close();
						}
					}
				}
				else 
					throw new DBAppException("table is full");
		
			}
			else 
				throw new DBAppException("invalid column or datatype");
		}
		else 
			throw new DBAppException("table does not exist");
	}


	public static String[] getClusteringKey(String strTableName) throws IOException {
		String clusteringKey="";
		int count=0;
		String []res=new String[2];
		FileReader fileReader = new FileReader(path+ "MetaData.csv" ); 
		@SuppressWarnings("resource")
		BufferedReader reader = new BufferedReader( fileReader ); 
		String strCurrentLine = "";
		while( (strCurrentLine = reader.readLine() ) != null ) {
			 String[]s = strCurrentLine.split(",");
			 if(s[0].compareTo(strTableName)== 0) {
				 if ((s[3]).equals("True")) {
					 clusteringKey=s[1]; 
				 	 break;
			 }
			 else count++;
		}
		}
		res[0]=clusteringKey;
		res[1]=count+"";
		return res;
	}
	public static boolean PKExists(String strTableName,Hashtable<String,Object> htblColNameValue) throws IOException, DBAppException {
		// we didn't sort so I need to check in each page of this table
		String clusteringKey=getclusteringKey(strTableName);
		Object clusteringKeyValue=htblColNameValue.get(clusteringKey);
		int pages=numOfPages(strTableName);
		Boolean f=false;
		for(int i=1;i<=pages;i++) {
			f=readFromCSV(path+strTableName+1+".csv",strTableName,clusteringKeyValue);
			if (f==true)
				break;
		}
		return f;
		
		
	}
	public static Boolean readFromCSV(String filename,String strTableName, Object clusteringKeyValue) throws IOException, DBAppException{
		FileReader fileReader = new FileReader(filename);//do another method to read all files 
		@SuppressWarnings("resource")
		BufferedReader reader = new BufferedReader( fileReader ); 
		String strCurrentLine = "";
		Boolean f=true;
		int index=keyIndex(strTableName);
		while( (strCurrentLine = reader.readLine() ) != null ) {
			 String[]s = strCurrentLine.split(",");
			 if (s[index].equals(clusteringKeyValue)) {
				 f=false;
				 throw new DBAppException("this value already exists");
			 }
		}
		return f;
	}

	public static void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String,Object> htblColNameValue ) throws DBAppException, IOException, ParseException {
			if(!tableExists(strTableName))
			throw new DBAppException("table does not exist");
					int index=keyIndex(strTableName);
					String cluster=getclusteringKey(strTableName);
					ArrayList<String> res=new ArrayList<>();
					Enumeration<String> keys = htblColNameValue.keys();
					ArrayList<String> key=new ArrayList<String>();
					while( keys.hasMoreElements() ){
						String t=keys.nextElement();
						key.add(t);
					}
					ArrayList <Integer> indeces=getIndeces(key);
					String page="";
					if (hasIndex(strTableName,cluster)) {
						page=getPageWithIndex(strTableName,strClusteringKeyValue);
					}
					else {
						page=getPage(strTableName,strClusteringKeyValue);
						}
					updateChecks(page,strTableName,htblColNameValue);
					FileReader fileReader = new FileReader(page);
					BufferedReader reader = new BufferedReader( fileReader ); 
					String strCurrentLine = "";
					while( (strCurrentLine = reader.readLine() ) != null ) {
						String[]s = strCurrentLine.split(",");
						if (s[index].equals(strClusteringKeyValue)) {
							for(int i=0;i<indeces.size();i++) {
								s[indeces.get(i)]=htblColNameValue.get(key.get(i))+"";
								String u="";
								for (int j=0;j<s.length;j++) {
									u+=s[j]+",";
								}
								res.add(u);
							}
						}
						else {
							res.add(strCurrentLine);
						}

					}
					reader.close();
					write(res,page);
					}
	public static String getPageWithIndex(String strTableName,String strClusteringKeyValue) throws IOException, ParseException, DBAppException {
		String cluster=getclusteringKey(strTableName);
		String fileName=strTableName+cluster;
		String keyType=getKeyType(strTableName);
		int page=0;
		FileReader fileReader = new FileReader(fileName);
		BufferedReader reader = new BufferedReader( fileReader ); 
		String strCurrentLine = "";
		while( (strCurrentLine = reader.readLine() ) != null ) {
			String[]s = strCurrentLine.split(",");
			if(keyType.equals("java.lang.Integer")) {
				if(Integer.parseInt(s[0])<=Integer.parseInt(strClusteringKeyValue)) {
					page=Integer.parseInt(s[1]);
				}
				else
					break;
			}
			if(keyType.equals("java.lang.Double")) {
				if(Double.parseDouble(s[0])<=Double.parseDouble(strClusteringKeyValue)) {
					page=Integer.parseInt(s[1]);
				}
				else
					break;
			}
			if(keyType.equals("java.lang.String")) {
				if(s[0].compareTo(strClusteringKeyValue)<=0) {
					page=Integer.parseInt(s[1]);
				}
				else
					break;
			}
			if(keyType.equals("java.lang.Date")) {
				Date d1=new SimpleDateFormat("dd/MM/yyyy").parse(s[0]);
				Date d2=new SimpleDateFormat("dd/MM/yyyy").parse(strClusteringKeyValue);
				if(d1.compareTo(d2)<=0) {
					page=Integer.parseInt(s[1]);
				}
				else
					break;
			}
		}
		reader.close();
		if (page==0) {
			return "";
		}
		else
			return path+strTableName+page+".csv";
	}
	public static String getPage(String strTableName,String strClusteringKeyValue) throws IOException {
		int index=keyIndex(strTableName);
		String page="";	
		for(int i=1;i<=numOfPages(strTableName);i++) {
			FileReader fileReader = new FileReader(path+strTableName+i+".csv");
			@SuppressWarnings("resource")
			BufferedReader reader = new BufferedReader( fileReader ); 
			String strCurrentLine = "";
			while( (strCurrentLine = reader.readLine() ) != null ) {
			 String[]s = strCurrentLine.split(",");
			 if (s[index].equals(strClusteringKeyValue)) {
				 page=path+strTableName+i+".csv";
				 break;
			 }
		}
}
		return page;
}
	public static ArrayList<Integer> getIndeces (ArrayList<String> col) throws IOException{
		ArrayList<Integer> indeces=new ArrayList<Integer>();
		FileReader fileReader = new FileReader(path+ "MetaData.csv" ); 
		@SuppressWarnings("resource")
		BufferedReader reader = new BufferedReader( fileReader ); 
		String strCurrentLine = "";
		for(int i=0;i<col.size();i++) {
			int count=0;
			while( (strCurrentLine = reader.readLine() ) != null ) {
				String[]s = strCurrentLine.split(",");
				if (s[1].equals(col.get(i))) {
				 indeces.add(count);
				 }
				else 
				count++;
		}
	}
		return indeces;
	}
	public static int keyIndex(String strTableName) throws IOException {
		int c=0;
		FileReader fileReader = new FileReader(path+"MetaData.csv"); 
		BufferedReader reader = new BufferedReader( fileReader );
		String strCurrentLine = "";
		while( (strCurrentLine = reader.readLine() ) != null ) {
			String[]s = strCurrentLine.split(",");
			if(s[0].compareTo(strTableName)== 0)
			{
				if (s[3].equals("TRUE")){
					break;
				}
				else
					c++;
			}
		}
		reader.close();
		return c;
		
	}
	public static String getclusteringKey(String strTableName) throws IOException{
		String str="";
		FileReader fileReader = new FileReader(path+"MetaData.csv"); 
		BufferedReader reader = new BufferedReader( fileReader );
		String strCurrentLine = ""; 
		while( (strCurrentLine = reader.readLine() ) != null ) {
			String[]s = strCurrentLine.split(",");
			if(s[0].compareTo(strTableName)== 0)
			{
				if (s[3].equals("TRUE")){
					str=s[1];
					break;
				}
			}
		}
		reader.close();
		return str;	
	}
	public static String getKeyType(String strTableName) throws IOException{
		String str="";
		FileReader fileReader = new FileReader(path+"MetaData.csv"); 
		BufferedReader reader = new BufferedReader( fileReader );
		String strCurrentLine = ""; 
		while( (strCurrentLine = reader.readLine() ) != null ) {
			String[]s = strCurrentLine.split(",");
			if(s[0].compareTo(strTableName)== 0)
			{
				if (s[3].equals(" True")){
					str=s[2];
					break;
				}
			}
		}
		reader.close();
		return str;	
	}
	public static void write(ArrayList<String> s, String page) throws IOException {
		FileWriter w = new FileWriter(page);
		for (int i=0;i<s.size();i++) {
		w.write(s.get(i)+System.lineSeparator());
		}
		w.close();
		}
			
	public static void updateChecks(String page, String tablename,Hashtable <String,Object> h) throws DBAppException, IOException, ParseException {
		if (page.equals(""))
			throw new DBAppException("invalid values");
		if (!validValues(tablename,h))
			throw new DBAppException("invalid U values");
		if (!validRange(tablename,h))
			throw new DBAppException("invalid R values");
		
	}
	public static boolean validValues(String strTableName,Hashtable <String,Object> h) throws IOException {
		Enumeration<String> keys = h.keys();
		ArrayList<String> k=new ArrayList<String>();
		while(keys.hasMoreElements()) {
			k.add(keys.nextElement());
		}
		for(int i=0;i<k.size();i++) {
			String s=getType(strTableName,k.get(i));
			if (!s.equals(h.get(k.get(i)).getClass().getName()))
				return false;
		}
		return true;
	}
	public static String getType(String strTableName,String column) throws IOException {
		String str="";
		FileReader fileReader = new FileReader(path+"MetaData.csv"); 
		BufferedReader reader = new BufferedReader( fileReader );
		String strCurrentLine = ""; 
		while( (strCurrentLine = reader.readLine() ) != null ) {
			String[]s = strCurrentLine.split(",");
			if(s[0].compareTo(strTableName)== 0)
			{
				if (s[1].equals(column)){
					str=s[2];
					break;
				}
			}
		}
		reader.close();
		return str;	
	}
	public static String getMin(String strTableName,String column) throws IOException {
		String str="";
		FileReader fileReader = new FileReader(path+"MetaData.csv"); 
		BufferedReader reader = new BufferedReader( fileReader );
		String strCurrentLine = ""; 
		while( (strCurrentLine = reader.readLine() ) != null ) {
			String[]s = strCurrentLine.split(",");
			if(s[0].compareTo(strTableName)== 0)
			{
				if (s[1].equals(column)){
					str=s[4];
					break;
				}
			}
		}
		reader.close();
		return str;	
	}
	public static String getMax(String strTableName,String column) throws IOException {
		String str="";
		FileReader fileReader = new FileReader(path+"MetaData.csv"); 
		BufferedReader reader = new BufferedReader( fileReader );
		String strCurrentLine = ""; 
		while( (strCurrentLine = reader.readLine() ) != null ) {
			String[]s = strCurrentLine.split(",");
			if(s[0].compareTo(strTableName)== 0)
			{
				if (s[1].equals(column)){
					str=s[5];
					break;
				}
			}
		}
		reader.close();
		return str;	
	}
	public static boolean validRange(String strTableName,Hashtable<String,Object> h) throws IOException, ParseException {
		Enumeration<String> keys = h.keys();
		ArrayList<String> k=new ArrayList<String>();		
		while(keys.hasMoreElements()) {
			k.add(keys.nextElement());
		}
		for(int i=0;i<k.size();i++) {
			String s=h.get(k.get(i))+"";
			String min=getMin(strTableName,k.get(i));
			String max=getMax(strTableName,k.get(i));
			if (k.get(i).getClass().getName().equals("java.lang.String")) {
			if (s.compareTo(min)<0 ||s.compareTo(max)>0|| min.length()>s.length()||max.length()<s.length()) {
				return false;
			}
			}
			if (k.get(i).getClass().getName().equals("java.lang.Integer")) {
				if (Integer.parseInt(s)>Integer.parseInt(max)||(Integer.parseInt(s)<Integer.parseInt(min))){
					return false;
				}
			}
			if (k.get(i).getClass().getName().equals("java.lang.Double")) {
				if (Double.parseDouble(s)>Double.parseDouble(max)||(Double.parseDouble(s)<Double.parseDouble(min)))
				{	
					return false;
				}
			}
			if (k.get(i).getClass().getName().equals("java.lang.Date")) {
				Date d1=new SimpleDateFormat("dd/MM/yyyy").parse(s);
				Date d2=new SimpleDateFormat("dd/MM/yyyy").parse(min);
				Date d3=new SimpleDateFormat("dd/MM/yyyy").parse(max);
				if (d1.compareTo(d2)<0 ||d1.compareTo(d3)>0) {
					return false;
				}
				}
			}
		return true;
	}
	public static String getClusterVal(String str, String strTableName) throws IOException {
		String [] sperated = str.split(",");
		return sperated[keyIndex(strTableName)];
	}
	public static void insert2(String strTableName,int n, String insert) throws IOException {
		ArrayList <String> s=new ArrayList<String>();
		//in case the next page is not full
		boolean f=false;
		for (int i=n;i<=2;i++) {
		String clusteringVal=getClusterVal(insert,strTableName);
		String page=path+strTableName+i+".csv";
		FileReader fileReader = new FileReader(page);
		@SuppressWarnings("resource")
		BufferedReader reader = new BufferedReader( fileReader ); 
		String strCurrentLine = "";
		while( (strCurrentLine = reader.readLine() ) != null ) {
			String str=getClusterVal(strCurrentLine,strTableName);
			if (str.compareTo(clusteringVal)>0 &&f==false) {
				s.add(insert);
				f=true;
			}
			s.add(strCurrentLine);
		}
		if (!isFull(page)) {
			break;
		}
		}
		int index=0;
		for(int j=n;j<=2;j++) {
		int count=0;
		String p1=path+strTableName+j+".csv";
		FileWriter w = new FileWriter(p1);
		while(index<s.size()) {	
		w.write(s.get(index)+System.lineSeparator());
		count++;
		index++;
		if (count==3) {
			break;
		}
		}
		w.close();
		if (index==s.size())
			break;
	}
	}

	public static boolean isFull(String fileName) throws IOException {
		int c=0;
		FileReader fileReader = new FileReader( fileName ); 
		BufferedReader reader = new BufferedReader( fileReader ); 
		String strText = "", strCurrentLine = ""; 
		while( (strCurrentLine = reader.readLine() ) != null )
			c++;
		if(c==3)
			return true;
		else
			return false;
	}
	
	
	public void createIndex(String strTableName,String strColName) throws DBAppException, IOException{
		String indexName=strTableName+"_"+strColName;
		String fileName=strTableName+strColName;
		String key=getclusteringKey(strTableName);
		ArrayList<String> arr=new ArrayList<>();
		FileReader fileReader = new FileReader( path+"MetaData.csv" ); 
		BufferedReader reader = new BufferedReader( fileReader ); 
		String strCurrentLine = "";
		while( (strCurrentLine = reader.readLine() ) != null ) {
			String[]s = strCurrentLine.split(",");
			if (s[1].equals(strColName)) {
				s[4]=indexName;
				s[5]="SparseIndex";
				String u="";
				for (int j=0;j<s.length;j++) {
					u+=s[j]+",";
				}
				arr.add(u);
			}
			else
				arr.add(strCurrentLine);
		}
		reader.close();
		write(arr,path+"MetaData.csv");
		String str=indexName+",SparseIndex,"+fileName+".csv"+","+path;
		Path path1 = Paths.get(path + "Files.csv");
		Files.write(path1, str.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
		if (key.equals(strColName)) {
			indexOnClustering(strTableName,fileName);
		}
		
			
	}
	public static ArrayList <String> read(String tableName) throws  IOException{
		ArrayList<String> arr=new ArrayList<>();
		for (int i=1;i<=numOfPages(tableName);i++) {
		FileReader fileReader = new FileReader(path+tableName+i+".csv"); 
		BufferedReader reader = new BufferedReader( fileReader ); 
		String strCurrentLine = "";
		while( (strCurrentLine = reader.readLine() ) != null ) {
			arr.add(strCurrentLine);
		}
		reader.close();
	}
		return arr;
	}
	public static void indexOnClustering(String strTableName,String fileName) throws IOException {
		ArrayList<String> s=new ArrayList<>();
		File index=new File(path+fileName+".csv");
		index.createNewFile();
		if (numOfRecords(strTableName)!=0) {
			int c=1;
			while(c<=numOfPages(strTableName)) {
				FileReader fileReader = new FileReader( path+strTableName+c+".csv"); 
				BufferedReader reader = new BufferedReader( fileReader );
				String str=reader.readLine();
				reader.close();
				String reference=getClusterVal(str,strTableName)+","+c;
				s.add(reference);
				c++;
			}
			write(s,path+fileName+".csv");
		}
	}
	public static void indexOnNonClustering(ArrayList<String>index,String strTableName ,String fileName) throws IOException {
		int c=0;
		int indexPages=0;
		for(int i=1;i<=numOfPages(strTableName);i++) {
			File dense=new File(path+fileName+i+".csv");
			dense.createNewFile();
			FileWriter fileWriter = new FileWriter(path+fileName+i+".csv");
			for(int j=c;j<index.size();j++) {
				fileWriter.write(index.get(j));
				if(j==maxRecords-1) {
					c=j+1;
					break;
				}
				c=j+1;
			}
			fileWriter.close();
			if(c==index.size()) {
				break;
			}
			indexPages++;
		}
		File sparse=new File(path+fileName+".csv");
		sparse.createNewFile();
		ArrayList<String>s=new ArrayList<>();
		for(int i=1;i<=indexPages;i++) {	// should we add it in the files.csv???
			FileReader fileReader = new FileReader( path+fileName+i+".csv"); 
			BufferedReader reader = new BufferedReader( fileReader );
			String str=reader.readLine();
			reader.close();
			fileReader.close();
			String reference=str.split(",")[0]+","+i;
			s.add(reference);
		}
		write(s,path+fileName+".csv");
	}
	
	public static int getColIndex(String strTableName,String colname ) throws IOException {
		FileReader fileReader = new FileReader(path+ "MetaData.csv" ); 
		BufferedReader reader = new BufferedReader( fileReader ); 
		String strCurrentLine = "";
		int c=0;
		while( (strCurrentLine = reader.readLine() ) != null ) {
			String[]s = strCurrentLine.split(",");
			if(s[0].equals(strTableName)) {
				if(s[1].equals(colname))
					break;
				else
					c++;
			}
		}
		reader.close();
		return c;
	}
	public static boolean hasIndex(String strTableName, String colname) throws IOException {
		FileReader fileReader = new FileReader(path+ "MetaData.csv" ); 
		BufferedReader reader = new BufferedReader( fileReader ); 
		String strCurrentLine = "";
		while( (strCurrentLine = reader.readLine() ) != null ) {
			String[]s = strCurrentLine.split(",");
			if(s[0].equals(strTableName)) {
				if(s[1].equals(colname) && s[5].equals("SparseIndex")) {
					reader.close();
					return true;
				}
			}
		}
		reader.close();
		return false;
	}
	
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms,String[] strarrOperators)throws DBAppException, IOException, ParseException{
		ArrayList<String>res=new ArrayList<>();
		ArrayList<String>table=new ArrayList<>();
		ArrayList<Integer>temp=new ArrayList<>();
		ArrayList<String>t=new ArrayList<>();
		int c=0,count=0,index=0;
		String tableName="",column="",op="";
		Object value=null;
		boolean f=false; 
		if (arrSQLTerms.length-1!=strarrOperators.length) {
			throw new DBAppException("invalid expression");
		}
		for(int i=0;i<strarrOperators.length;i++) {
			if (strarrOperators[i].equals("AND"))
				count++;
		}
		while (c<arrSQLTerms.length &&index<arrSQLTerms.length) {
		if (count==strarrOperators.length) {	
		tableName=arrSQLTerms[index]._strTableName;
		column=arrSQLTerms[index]._strColumnName;
		value=arrSQLTerms[index]._objValue;
		op=arrSQLTerms[index]._strOperator;
		}
		else {
			tableName=arrSQLTerms[c]._strTableName;
			column=arrSQLTerms[c]._strColumnName;
			value=arrSQLTerms[c]._objValue;
			op=arrSQLTerms[c]._strOperator;
		}
		if (count==strarrOperators.length) {
			f=true;
			if (index==0) {
				if (hasIndex(tableName, column)) {
					if (column.equals(getclusteringKey(tableName))){
						res=selectWithIndex(column,tableName,value,op);
						t=selectWithIndex(column,tableName,value,op);
					}
					else {
					res=select1(column,tableName,value,op);
					t=select1(column,tableName,value,op);
					}
				}
				else {
					res=seqSearch(column,tableName,value,op);
					t=seqSearch(column,tableName,value,op);
				}
				index++;
			}
			else {
				if (hasIndex(tableName, column)) {
					if (column.equals(getclusteringKey(tableName))){
						t=selectWithIndex(column,tableName,value,op);
						res=andOp(res,t);
					}
					else {
						t=select1(column,tableName,value,op);
						res=andOp(res,t);
						}
				}
				else {
					t=seqSearch(column,tableName,value,op);
					res=andOp(res,t);
				}
				index++;
			}
		}
		else {
		try {
			table=read(tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (c==0) {
			for(int i=0;i<table.size();i++) {
			String columnValue=table.get(i).split(",")[getColIndex(tableName,column)];	
			temp.add(oper(op,column,value,columnValue));
			}
			c++;
		}
		else {
			for(int i=0;i<table.size();i++) {
				String columnValue=table.get(i).split(",")[getColIndex(tableName,column)];
				int j=oper(op,column,value,columnValue);
				switch (strarrOperators[c-1]) {
				case "AND":int and=temp.get(i) & j;temp.set(i,and);break;
				case "OR":int or=temp.get(i) | j;temp.set(i,or);break;
				case "XOR":int xor=temp.get(i) ^ j;temp.set(i,xor);break;
				default:throw new DBAppException("invalid operator");
				}
			}
			c++;
		}
		}
		}
		for(int i=0;i<temp.size()&& f==false;i++) {
			if(temp.get(i)==1) {
				res.add(table.get(i));
			}
		
		}
		return res.iterator();
}
	
	public static int oper(String operator, String column, Object Value, String tableValue) throws ParseException, DBAppException{
			if (operator.equals("=")) {
				if (Value.getClass().getName().equals("java.lang.Integer"))
					if (Integer.parseInt(tableValue)==(Integer)Value) {
						return 1;
				}
					else return 0;	
					else if (Value.getClass().getName().equals("java.lang.Double")) 
					if (Double.parseDouble(tableValue)==(Double)Value) {
						return 1;
				}
					else return 0;	
					else if (Value.getClass().getName().equals("java.lang.String"))
							if ((tableValue).equals(Value)) {
								return 1;
						}
							else return 0;	
					else if(Value.getClass().getName().equals("java.lang.Date")) {
						Date d1=new SimpleDateFormat("dd/MM/yyyy").parse(Value+"");
						Date d2=new SimpleDateFormat("dd/MM/yyyy").parse(tableValue);
						if ((d1).compareTo(d2)==0) {
							return 1;
					}
						else return 0;	
					}
			}
			else if (operator.equals("!=")) {
				if (Value.getClass().getName().equals("java.lang.Integer"))
					if (Integer.parseInt(tableValue)!=(Integer)Value) {
						return 1;
				}
					else return 0;	
					else if (Value.getClass().getName().equals("java.lang.Double")) 
					if (Double.parseDouble(tableValue)!=(Double)Value) {
						return 1;
				}
					else return 0;	
					else if (Value.getClass().getName().equals("java.lang.String"))
							if (!(tableValue).equals(Value)) {
								return 1;
						}
							else return 0;	
					else if(Value.getClass().getName().equals("java.lang.Date")) {
						Date d1=new SimpleDateFormat("dd/MM/yyyy").parse(Value+"");
						Date d2=new SimpleDateFormat("dd/MM/yyyy").parse(tableValue);
						if ((d1).compareTo(d2)!=0) {
							return 1;
					}
						else return 0;	
					}
			}
			else if (operator.equals(">")) {
				if (Value.getClass().getName().equals("java.lang.Integer"))
					if (Integer.parseInt(tableValue)>(Integer)Value) {
						return 1;
				}
					else return 0;	
					else if (Value.getClass().getName().equals("java.lang.Double")) 
					if (Double.parseDouble(tableValue)>(Double)Value) {
						return 1;
				}
					else return 0;	
					else if (Value.getClass().getName().equals("java.lang.String"))
							if ((tableValue).compareTo(Value+"")>0) {
								return 1;
						}
							else return 0;	
					else if(Value.getClass().getName().equals("java.lang.Date")) {
						Date d1=new SimpleDateFormat("dd/MM/yyyy").parse(tableValue);
						Date d2=new SimpleDateFormat("dd/MM/yyyy").parse(Value+"");
						if ((d1).compareTo(d2)>0) {
							return 1;
					}
						else return 0;	
					}
			}
			else if (operator.equals(">=")) {
				if (Value.getClass().getName().equals("java.lang.Integer"))
					if (Integer.parseInt(tableValue)>=(Integer)Value) {
						return 1;
				}
					else if (Value.getClass().getName().equals("java.lang.Double")) 
					if (Double.parseDouble(tableValue)==(Double)Value) {
						return 1;
				}
					else if (Value.getClass().getName().equals("java.lang.String"))
							if ((tableValue).compareTo(Value+"")>=0) {
								return 1;
						}
					else if(Value.getClass().getName().equals("java.lang.Date")) {
						Date d1=new SimpleDateFormat("dd/MM/yyyy").parse(tableValue);
						Date d2=new SimpleDateFormat("dd/MM/yyyy").parse(Value+"");
						if ((d1).compareTo(d2)>=0) {
							return 1;
					}
					}
			}
			else if (operator.equals("<")) {
				if (Value.getClass().getName().equals("java.lang.Integer"))
					if (Integer.parseInt(tableValue)<(Integer)Value) {
						return 1;
				}
					else if (Value.getClass().getName().equals("java.lang.Double")) 
					if (Double.parseDouble(tableValue)<(Double)Value) {
						return 1;
				}
					else if (Value.getClass().getName().equals("java.lang.String"))
							if ((tableValue).compareTo(Value+"")<0) {
								return 1;
						}
					else if(Value.getClass().getName().equals("java.lang.Date")) {
						Date d1=new SimpleDateFormat("dd/MM/yyyy").parse(tableValue);
						Date d2=new SimpleDateFormat("dd/MM/yyyy").parse(Value+"");
						if ((d1).compareTo(d2)<0) {
							return 1;
					}
						else return 0;	
					}
			}
			else if (operator.equals("<=")) {
				if (Value.getClass().getName().equals("java.lang.Integer"))
					if (Integer.parseInt(tableValue)<=(Integer)Value) {
						return 1;
				}
					else return 0;	
					else if (Value.getClass().getName().equals("java.lang.Double")) 
					if (Double.parseDouble(tableValue)<=(Double)Value) {
						return 1;
				}
					else return 0;	
					else if (Value.getClass().getName().equals("java.lang.String"))
							if ((tableValue).compareTo(Value+"")<=0) {
								return 1;
						}
							else return 0;	
					else if(Value.getClass().getName().equals("java.lang.Date")) {
						Date d1=new SimpleDateFormat("dd/MM/yyyy").parse(tableValue);
						Date d2=new SimpleDateFormat("dd/MM/yyyy").parse(Value+"");
						if ((d1).compareTo(d2)<=0) {
							return 1;
					}
						else return 0;	
					}
			}
				throw new DBAppException("invalid operator");
		}
	public static ArrayList<String> readFile(String fileName) throws IOException{
		ArrayList<String> res=new ArrayList<>();
		FileReader fileReader = new FileReader(fileName); 
		BufferedReader reader = new BufferedReader( fileReader );
		String strCurrentLine="";
		while( (strCurrentLine = reader.readLine() ) != null ) {
			res.add(strCurrentLine);
		}
		reader.close();
		fileReader.close();
		return res;
	}

	public static int compareValues(String type,String indexValue,String value) throws IOException, ParseException, DBAppException {
		if(type.equals("java.lang.Integer")) {
			if(Integer.parseInt(indexValue)==Integer.parseInt(value)) {
				return 0;
			}
			else if (Integer.parseInt(indexValue)<Integer.parseInt(value)){
				return -1;
			}
			else
				return 1;
		}
		if(type.equals("java.lang.Double")) {
			if(Double.parseDouble(indexValue)==Double.parseDouble(value)) {
				return 0;
			}
			else if (Double.parseDouble(indexValue)<Double.parseDouble(value)){
				return -1;
			}
			else
				return 1;
		}
		if(type.equals("java.lang.String")) {
			if(indexValue.compareTo(value)==0) {
				return 0;
			}
			else if (indexValue.compareTo(value)<0) {
				return -1;
			}
			else
				return 1;
		}
		if(type.equals("java.lang.Date")) {
			Date d1=new SimpleDateFormat("dd/MM/yyyy").parse(indexValue);
			Date d2=new SimpleDateFormat("dd/MM/yyyy").parse(value);
			if(d1.compareTo(d2)==0) {
				return 0;
			}
			else if (d1.compareTo(d2)<0) {
				return -1;
			}
			else
				return 1;
		}
		throw new DBAppException("invalid types");
	}
	public static ArrayList<String> selectWithIndex(String column, String table,Object value,String op) throws IOException, ParseException, DBAppException {
		
			String fileName=table+column;
			ArrayList<String>sparse=readFile(path+fileName+".csv");
			String type=getType(table,column);
			int page=0;
			int col=getColIndex(table,column);
			ArrayList<String> res=new ArrayList<>();
			ArrayList<String> temp=new ArrayList<>();
			for(int i=0;i<sparse.size();i++) {
				if(compareValues(type,sparse.get(i).split(",")[0],value+"")<=0) {
					page=Integer.parseInt(sparse.get(i).split(",")[1]);
				}
				else break;
			}
			if (page==0) {
				
			}
			switch (op) {
			case "=":{
				temp=readFile(path+table+page+".csv");
				for(int i=0;i<temp.size();i++) {
					if (temp.get(i).split(",")[col].equals(value+"")) {
						res.add(temp.get(i));
						break;
					}
				}
			};break;
			case "<":{boolean f=false;
				for(int i=1;i<=page && f==false;i++) {
					temp=readFile(path+table+i+".csv");
					for(int j=0;j<temp.size();j++) {
						if (compareValues(type,temp.get(j).split(",")[col],value+"")<0)
							res.add(temp.get(j));
						else {
							f=true;
							break;
						}
					}
				}
			};break;
			case "<=":{boolean f=false;
			for(int i=1;i<=page && f==false;i++) {
				temp=readFile(path+table+i+".csv");
				for(int j=0;j<temp.size();j++) {
					if (compareValues(type,temp.get(j).split(",")[col],value+"")<=0)
						res.add(temp.get(j));
					else {
						f=true;
						break;
					}
				}
			}
		};break;
			case ">":{boolean f=false;
			for(int i=page;i<=numOfPages(table) && f==false;i++) {
				temp=readFile(path+table+i+".csv");
				for(int j=0;j<temp.size();j++) {
					if (compareValues(type,temp.get(j).split(",")[col],value+"")>0)
						res.add(temp.get(j));
				}
			}
		};break;
			case ">=":{boolean f=false;
			for(int i=page;i<=numOfPages(table)&& f==false;i++) {
				temp=readFile(path+table+i+".csv");
				for(int j=0;j<temp.size();j++) {
					if (compareValues(type,temp.get(j).split(",")[col],value+"")>=0)
						res.add(temp.get(j));
				}
			}
		};break;
			case "!=":{
			for(int i=1;i<=numOfPages(table);i++) {
				temp=readFile(path+table+i+".csv");
				for(int j=0;j<temp.size();j++) {
					if (!temp.get(i).split(",")[col].equals(value+""))
						res.add(temp.get(j));
					
				}
			}
		};break;
			default: throw new DBAppException("invalid operator");	
			}
			return res;
		}
			
	public static ArrayList<String> select1(String column, String table,Object value,String op) throws IOException, ParseException, DBAppException {
		
		String fileName=table+column;
		ArrayList<String>sparse=readFile(path+fileName+".csv");
		String type=getType(table,column);
		int page=0;
		ArrayList<String> res=new ArrayList<>();
		ArrayList<String> temp=new ArrayList<>();
		for(int i=0;i<sparse.size();i++) {
			if(compareValues(type,sparse.get(i).split(",")[0],value+"")<0) {
				page=Integer.parseInt(sparse.get(i).split(",")[1]);
			}
			else break;
		}
		if (page==0) {
			
		}
		switch (op) {
		case "=":{boolean f=false;
			for(int j=page;j<=numOfPages(fileName) && f==false;j++) {
				temp=readFile(path+fileName+j+".csv");
			for(int i=0;i<temp.size();i++) {
				if (temp.get(i).split(",")[0].equals(value+"")) {
					res.add(temp.get(i));
					f=true;
				
				}
				if (compareValues(type,temp.get(i).split(",")[0],value+"")>0) {
					f=true;
					break;
				}
				}
			}
		};break;
		case "<":{
			for(int i=1;i<=page;i++) {
				temp=readFile(path+fileName+i+".csv");
				for(int j=0;j<temp.size();j++) {
					if (compareValues(type,temp.get(j).split(",")[0],value+"")<0)
						res.add(temp.get(j));
					
				}
			}
		};break;
		case "<=":{boolean f=false;
		for(int i=1;i<=numOfPages(fileName) && f==false;i++) {
			temp=readFile(path+fileName+i+".csv");
			for(int j=0;j<temp.size();j++) {
				if (compareValues(type,temp.get(j).split(",")[0],value+"")<=0)
					res.add(temp.get(j));
				else {
					f=true;
					break;
				}
			}
		}
	};break;
		case ">":{boolean f=false;
		for(int i=page;i<=numOfPages(fileName) && f==false;i++) {
			temp=readFile(path+fileName+i+".csv");
			for(int j=0;j<temp.size();j++) {
				if (compareValues(type,temp.get(j).split(",")[0],value+"")>0)
					res.add(temp.get(j));
				
			}
		}
	};break;
		case ">=":{boolean f=false;
		for(int i=page;i<=numOfPages(fileName)&& f==false;i++) {
			temp=readFile(path+fileName+i+".csv");
			for(int j=0;j<temp.size();j++) {
				if (compareValues(type,temp.get(j).split(",")[0],value+"")>=0)
					res.add(temp.get(j));
				
			}
		}
	};break;
		case "!=":{
		for(int i=1;i<=numOfPages(fileName);i++) {
			temp=readFile(path+fileName+i+".csv");
			for(int j=0;j<temp.size();j++) {
				if (!temp.get(i).split(",")[0].equals(value+""))
					res.add(temp.get(j));
				
			}
		}
	};break;
		default: throw new DBAppException("invalid operator");	
		}
		ArrayList<String>rows=new ArrayList<>();
		for(int i=0;i<res.size();i++) {
			String ref=res.get(i).split(",")[1];
			String p=ref.split("_")[0];
			String row=ref.split("_")[1];
			ArrayList<String>t=readFile(path+table+p+".csv");
			for(int j=0;j<t.size();j++) {
				if (getClusterVal(t.get(j),table).equals(row)) {
					rows.add(t.get(j));
				}
			}
		}
		return rows;
	}
	public static ArrayList<String> seqSearch(String column, String table,Object value,String op) throws IOException, ParseException, DBAppException {
		ArrayList<String>t=read(table);
		ArrayList<String>res=new ArrayList<>();
		int col=getColIndex(table,column);
		String type=getType(table,column);
		switch(op) {
		case "=":
			for(int i=0;i<t.size();i++) {
				if (t.get(i).split(",")[col].equals(value+"")) {
					res.add(t.get(i));
				}
			}
		;break;
		case "<":{
		for(int i=0;i<t.size();i++) {
			if (compareValues(type,t.get(i).split(",")[col] ,value+"")<0) {
				res.add(t.get(i));
			}
		}
		};break;
		case "<=":{
			for(int i=0;i<t.size();i++) {
				if (compareValues(type,t.get(i).split(",")[col] ,value+"")<0) {
					res.add(t.get(i));
				}
	}
		};break;

		case ">":{
			for(int i=0;i<t.size();i++) {
				if (compareValues(type,t.get(i).split(",")[col] ,value+"")>0) {
					res.add(t.get(i));
				}
	}
		};break;

		case ">=":{
			for(int i=0;i<t.size();i++) {
				if (compareValues(type,t.get(i).split(",")[col] ,value+"")>=0) {
					res.add(t.get(i));
				}
	}
		};break;

		case "!=":{
			for(int i=0;i<t.size();i++) {
				if (!t.get(i).split(",")[col].equals(value+"")) {
					res.add(t.get(i));
		}
	}
		};break;
		default:throw new DBAppException("invalid operator");
		}
		return res;
	}
	
	public static ArrayList<String> reverse(Enumeration<String> s){
		ArrayList<String>res=new ArrayList<>();
		while(s.hasMoreElements()) {
			res.add(s.nextElement());
	}
		Collections.reverse(res);
		return res;
	}
	public static ArrayList<String> andOp(ArrayList<String>temp,ArrayList<String>res){
		ArrayList<String>arr=new ArrayList<>();
			for (int j=0;j<temp.size();j++) {
				if (res.contains(temp.get(j))) {
					arr.add(temp.get(j));
				}
			}
			return arr;
	}
	
	public static void main(String[]args) throws IOException, DBAppException, ParseException {
		DBApp1 db = new DBApp1();
	//	db.init();
		Hashtable htblColNameType = new Hashtable( ); 
		htblColNameType.put("id", "java.lang.Integer"); 
		htblColNameType.put("name", "java.lang.String"); 
		
		Hashtable htblColNameMin = new Hashtable( ); 
		htblColNameMin.put("id", "0"); 
		htblColNameMin.put("name", "A"); 
		
		Hashtable htblColNameMax = new Hashtable( ); 
		htblColNameMax.put("id", "10"); 
		htblColNameMax.put("name", "Z"); 
		db.createTable( "hagoura", "id", htblColNameType, htblColNameMin,htblColNameMax);
		Hashtable h=new Hashtable();
		SQLTerm[] arrSQLTerms;
		arrSQLTerms = new SQLTerm[2];
		arrSQLTerms[0] = new SQLTerm();
		arrSQLTerms[1] = new SQLTerm();
		arrSQLTerms[0]._strTableName = "hagoura";
		arrSQLTerms[0]._strColumnName= "name";
		arrSQLTerms[0]._strOperator = "=";
		arrSQLTerms[0]._objValue = "menna";
		arrSQLTerms[1]._strTableName = "hagoura";
		arrSQLTerms[1]._strColumnName= "id";
		arrSQLTerms[1]._strOperator = "!=";
		arrSQLTerms[1]._objValue = new Integer( 1 );
		String[]strarrOperators = new String[1];
		strarrOperators[0] = "AND";
		Iterator select=db.selectFromTable(arrSQLTerms, strarrOperators);
		while(select.hasNext()) {
			System.out.println(select.next());
		}
	}
}

