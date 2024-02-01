import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Properties;

public class Table {
	String strTableName;
	String strClusteringKeyColumn;
	Hashtable<String,String> htblColNameType;
	Hashtable<String,String> htblColNameMin;
	Hashtable<String,String> htblColNameMax;
	int maxRecords;
	int numOfRecords;
	
	
	public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType, 
			Hashtable<String,String> htblColNameMin, Hashtable<String,String> htblColNameMax) {
		this.strTableName = strTableName;
		this.strClusteringKeyColumn = strClusteringKeyColumn;
		this.htblColNameType = htblColNameType;
		this.htblColNameMin = htblColNameMin;
		this.htblColNameMax = htblColNameMax;
		this.maxRecords = config();
		numOfRecords = 0;
		
		
	}

	private int config() {
		Properties prop = new Properties();
		String fileName = "DBApp.config";
		try (FileInputStream fis = new FileInputStream(fileName)) {
		    prop.load(fis);
		} catch (FileNotFoundException ex) {
		   
		} catch (IOException ex) {
		 
		}
		return(Integer.parseInt(prop.getProperty("DBApp.MaximumRowsCountinTablePage")));
	}

	public String getStrTableName() {
		return strTableName;
	}

	public void setStrTableName(String strTableName) {
		this.strTableName = strTableName;
	}

	public String getStrClusteringKeyColumn() {
		return strClusteringKeyColumn;
	}

	public void setStrClusteringKeyColumn(String strClusteringKeyColumn) {
		this.strClusteringKeyColumn = strClusteringKeyColumn;
	}

	public Hashtable<String, String> getHtblColNameType() {
		return htblColNameType;
	}

	public void setHtblColNameType(Hashtable<String, String> htblColNameType) {
		this.htblColNameType = htblColNameType;
	}

	public Hashtable<String, String> getHtblColNameMin() {
		return htblColNameMin;
	}

	public void setHtblColNameMin(Hashtable<String, String> htblColNameMin) {
		this.htblColNameMin = htblColNameMin;
	}

	public Hashtable<String, String> getHtblColNameMax() {
		return htblColNameMax;
	}

	public void setHtblColNameMax(Hashtable<String, String> htblColNameMax) {
		this.htblColNameMax = htblColNameMax;
	}

	public int getMaxRecords() {
		return maxRecords;
	}

	public void setMaxRecords(int maxRecords) {
		this.maxRecords = maxRecords;
	}
	
	
}
