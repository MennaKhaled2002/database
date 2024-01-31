import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
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

public class DBApp {
	static int maxRecords;
	static String path ="/Users/mennakhaled/Desktop/";

	public DBApp() throws IOException {
		maxRecords = config();
		init();
	}

	public void init() {
		try {
			File meta = new File(path + "MetaData.csv");
			meta.createNewFile();
			File Files1 = new File(path + "Files.csv");
			Files1.createNewFile();
			String s = "Name,Type, FileName, FileLocationonHarddisk" + System.lineSeparator();
			Path path1 = Paths.get(path + "Files.csv");
			Files.write(path1, s.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
			s = "Table Name,Column Name,Column Type,ClusteringKey,IndexName,IndexType,min,max" + System.lineSeparator();
			path1 = Paths.get(path + "MetaData.csv");
			Files.write(path1, s.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
		} catch (IOException e) {
		}
	}

	private int config() {
		Properties prop = new Properties();
		String fileName = path + "DBApp.config";
		try (FileInputStream fis = new FileInputStream(fileName)) {
			prop.load(fis);
		} catch (FileNotFoundException ex) {

		} catch (IOException ex) {

		}
		return (Integer.parseInt(prop.getProperty("MaximumRowsCountinTablePage")));
	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
			Hashtable<String, String> htblColNameMax) throws DBAppException {
		try {
			if (tableExists(strTableName))
				throw new DBAppException("table already exists");
			if (!htblColNameType.containsKey(strClusteringKeyColumn) || !validTypes(htblColNameType))
				throw new DBAppException("clustering key does not exist or types are not valid");
			Enumeration<String> keys = htblColNameType.keys();
			while (keys.hasMoreElements()) {
				String key = keys.nextElement();
				String s = strTableName + "," + key + "," + htblColNameType.get(key).toString() + ",";
				if (key.compareTo(strClusteringKeyColumn) == 0)
					s += "True";
				else
					s += "False";
				s += ",null,null," + htblColNameMin.get(key).toString() + "," + htblColNameMax.get(key).toString()
						+ System.lineSeparator();
				Path path1 = Paths.get(path + "MetaData.csv");
				Files.write(path1, s.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
			}
		} catch (IOException e) {
		}
	}

	public static boolean validTypes(Hashtable<String, String> htblColNameType) {
		Enumeration<String> values = htblColNameType.elements();
		while (values.hasMoreElements()) {
			switch (values.nextElement()) {
			case "java.lang.Integer":
				return true;
			case "java.lang.Date":
				return true;
			case "java.lang.String":
				return true;
			case "java.lang.Double":
				return true;

			}
			return false;
		}
		return false;
	}

	public static ArrayList<String> readPage(String FilePath) throws IOException {
		ArrayList<String> res = new ArrayList<String>();
		FileReader fileReader = new FileReader(FilePath);
		BufferedReader reader = new BufferedReader(fileReader);
		String strCurrentLine = "";
		while ((strCurrentLine = reader.readLine()) != null) {
			res.add(strCurrentLine);
		}
		fileReader.close();
		reader.close();
		return res;
	}

	public static boolean tableExists(String strTableName) throws IOException {
		ArrayList<String> csvRead = readPage(path + "MetaData.csv");
		for (int i = 0; i < csvRead.size(); i++) {
			String[] s = csvRead.get(i).split(",");
			if (s[0].compareTo(strTableName) == 0) {
				return true;
			}
		}
		return false;
	}

	public static ArrayList<String> tableHasIndex(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws IOException {
		Enumeration<String> k = htblColNameValue.keys();
		ArrayList<String> res = new ArrayList<String>();
		ArrayList<String> csvRead = readPage(path + "MetaData.csv");
		while (k.hasMoreElements()) {
			String column = k.nextElement();
			for (int i = 0; i < csvRead.size(); i++) {
				String[] s = csvRead.get(i).split(",");
				if (s[0].compareTo(strTableName) == 0)
					if (s[4].compareTo("null") != 0 && s[1].compareTo(column) == 0)
						res.add(s[1]);
			}
		}

		return res;
	}

	public static boolean hasClusteringIndex(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws IOException {
		Enumeration<String> k = htblColNameValue.keys();
		ArrayList<String> csvRead = readPage(path + "MetaData.csv");
		while (k.hasMoreElements()) {
			String column = k.nextElement();
			for (int i = 0; i < csvRead.size(); i++) {
				String[] s = csvRead.get(i).split(",");
				if (s[0].compareTo(strTableName) == 0)
					if (s[4].compareTo("null") != 0 && s[1].compareTo(column) == 0 && s[3].equals("True"))
						return true;
			}
		}
		return false;
	}

	public static void deleteWithClusterIndex(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws IOException, DBAppException, ParseException {
		Enumeration<Object> elements = htblColNameValue.elements();
		String clusterKey = getclusteringKey(strTableName);
		Object compare = htblColNameValue.get(clusterKey);
		ArrayList<String> indexFile = readPage(path + strTableName + clusterKey + ".csv");
		String keyType = getKeyType(strTableName);
		int reference = 0;
		for (int i = 0; i < indexFile.size(); i++) {
			String[] s = indexFile.get(i).split(",");
			if (keyType.equals("java.lang.String")) {
				String comp = compare.toString();
				if (s[0].compareTo(comp) > 0)
					break;
			} else if (keyType.equals("java.lang.Date")) {
				Date comp = new SimpleDateFormat("dd/MM/yyyy").parse(compare.toString());
				Date val = new SimpleDateFormat("dd/MM/yyyy").parse(s[0]);
				if (val.compareTo(comp) > 0)
					break;
			} else if (keyType.equals("java.lang.Integer")) {
				int comp = (int) compare;
				int val = Integer.parseInt(s[0]);
				if (val > comp)
					break;
			} else if (keyType.equals("java.lang.Double")) {
				Double comp = (Double) compare;
				Double val = Double.parseDouble(s[0]);
				if (val > comp)
					break;
			} else
				throw new DBAppException("invalid value");
			reference = Integer.parseInt(s[1]);
		}

		if (reference == 0)
			throw new DBAppException("value does not exist");

		ArrayList<Integer> a = getKeyPosition(htblColNameValue.keys(), strTableName);
		ArrayList<Object> en = new ArrayList<Object>();
		while (elements.hasMoreElements())
			en.add(elements.nextElement());

		ArrayList<String> csvRead = readPage(path + strTableName + reference + ".csv");
		int count = 0;
		for (int i = 0; i < csvRead.size(); i++) {
			String[] s = csvRead.get(i).split(",");
			boolean f = false;
			for (int n = 0; n < en.size(); n++) {
				String comp = en.get(n).toString();
				if (comp.compareTo(s[a.get(n) - 1]) != 0) {
					f = false;
					count++;
					break;
				} else
					f = true;
			}
			if (f) {
				if (count == 0 && csvRead.size() > 1) {
					csvRead.remove(count);
					String[] record = csvRead.get(0).split(",");
					indexFile.set(reference - 1, record[keyIndex(strTableName)] + "," + reference);
				} else if (count == 0 && csvRead.size() == 1) {
					indexFile.remove(reference - 1);
					deletePage(path + strTableName, reference, numOfPages(strTableName));
					for (int j = reference - 1; j < indexFile.size(); j++) {
						String change = indexFile.get(j);
						String[] split = change.split(",");
						split[1] = reference + "";
						change = s[0] + "," + s[1];
						indexFile.set(j, change);
					}
				} else {
					csvRead.remove(count);
				}
			}
		}
		write(indexFile, path + strTableName + clusterKey + ".csv");
		write(csvRead, path + strTableName + reference + ".csv");
		updateNonClusterIndex(strTableName);
	}

	public static void deleteWithNonClusterIndex(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws IOException, ParseException, DBAppException {
		ArrayList<String> indeces = tableHasIndex(strTableName, htblColNameValue);
		String colType = getType(strTableName, indeces.get(0));
		String index = indeces.get(0);
		Object o = htblColNameValue.get(index);
		String type = getType(strTableName, index);
		ArrayList<String> file = readPage(path + strTableName + index + ".csv");
		int reference = 0;
		for (int i = 0; i < file.size(); i++) {
			if (type.equals("java.lang.String")) {
				String[] s = file.get(i).split(",");
				String comp = o.toString();
				String curr = s[0];
				if (comp.compareTo(curr) >= 0) {
					reference = Integer.parseInt(s[1]);
					break;
				}
			} else if (type.equals("java.lang.Date")) {
				String[] s = file.get(i).split(",");
				Date comp = new SimpleDateFormat("dd/MM/yyyy").parse(o.toString());
				Date curr = new SimpleDateFormat("dd/MM/yyyy").parse(s[0]);
				if (comp.compareTo(curr) >= 0) {
					reference = Integer.parseInt(s[1]);
					break;
				}
			} else if (type.equals("java.lang.Ineteger")) {
				String[] s = file.get(i).split(",");
				int comp = Integer.parseInt(o.toString());
				int curr = Integer.parseInt(s[0]);
				if (comp >= curr) {
					reference = Integer.parseInt(s[1]);
					break;
				}
			} else if (type.equals("java.lang.Double")) {

				String[] s = file.get(i).split(",");

				Double comp = Double.parseDouble(o.toString());
				Double curr = Double.parseDouble(s[0]);
				if (comp >= curr) {
					reference = Integer.parseInt(s[1]);
					break;
				}
			} else
				throw new DBAppException("invalid type");
		}

		if (reference == 0)
			throw new DBAppException("values does not exist");
		else if (reference == 1)
			reference = 1;
		else
			reference -= 1;

		ArrayList<String> candidates = new ArrayList<String>();
		for (int i = reference; i < numOfPages(strTableName + "_" + index); i++) {
			ArrayList<String> p = readPage(path + strTableName + index + i + ".csv");
			for (int j = 0; j < p.size(); j++) {
				String[] str = p.get(j).split(",");
				if (colType.equals("java.lang.String")) {
					if (str[0].equals(o.toString())) {
						candidates.add(p.get(j));
					} else if (str[0].compareTo(o.toString()) > 0)
						break;
				} else if (colType.equals("java.lang.Double")) {
					Double curr = Double.parseDouble(str[0]);
					Double comp = Double.parseDouble(o.toString());
					if (curr.equals(comp)) {
						candidates.add(p.get(j));
					} else if (curr.compareTo(comp) > 0)
						break;
				} else if (colType.equals("java.lang.Integer")) {
					Integer curr = Integer.parseInt(str[0]);
					Integer comp = Integer.parseInt(o.toString());
					if (curr.equals(comp)) {
						candidates.add(p.get(j));
					} else if (curr.compareTo(comp) > 0)
						break;
				} else if (colType.equals("java.lang.Date")) {
					Date comp = new SimpleDateFormat("dd/MM/yyyy").parse(str[0]);
					Date curr = new SimpleDateFormat("dd/MM/yyyy").parse(o.toString());
					if (curr.equals(comp)) {
						candidates.add(p.get(j));
					} else if (curr.compareTo(comp) > 0)
						break;
				}
			}
		}
		int min = 0;
		for (int i = 0; i < candidates.size(); i++) {
			String[] dense = candidates.get(i).split(",");
			String[] ref = dense[1].split("_");
			ArrayList<String> record = readPage(path + strTableName + ref[0] + ".csv");
			for (int j = 0; j < record.size(); j++) {
				String cluster = getClusterVal(record.get(j), strTableName);
				if (cluster.equals(ref[1])) {
					String[] s = record.get(j).split(",");
					Enumeration<Object> e = htblColNameValue.elements();
					ArrayList<Integer> ind = getKeyPosition(htblColNameValue.keys(), strTableName);
					ArrayList<Object> en = new ArrayList<Object>();
					while (e.hasMoreElements())
						en.add(e.nextElement());
					boolean f = false;
					for (int n = 0; n < en.size(); n++) {
						String compare = en.get(n).toString();
						if (compare.compareTo(s[ind.get(n) - 1]) != 0) {
							f = false;
							break;
						} else
							f = true;
					}
					if (f) {
						if (min > Integer.parseInt(ref[0]))
							;
						min = Integer.parseInt(ref[0]);
						record.remove(j);
						write(record, path + strTableName + ref[0] + ".csv");
						if (isEmpty(path + strTableName + ref[0] + ".csv"))
							deletePage(path + strTableName, Integer.parseInt(ref[0]), numOfPages(strTableName));
						break;
					}
				}
			}
		}
		if (hasIndex(strTableName, getclusteringKey(strTableName))) {
			ArrayList<String> indexFile = readPage(path + strTableName + getclusteringKey(strTableName) + ".csv");
			updateClusterIndex(indexFile, min, strTableName,
					path + strTableName + getclusteringKey(strTableName) + ".csv");
		}

	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		try {
			if (!tableExists(strTableName))
				throw new DBAppException("table does not exist");
			htblColNameValue = lowerCase(strTableName, htblColNameValue);
			if (hasClusteringIndex(strTableName, htblColNameValue)) {
				deleteWithClusterIndex(strTableName, htblColNameValue);
				updateNonClusterIndex(strTableName);
			} else if (!tableHasIndex(strTableName, htblColNameValue).isEmpty()) {
				deleteWithNonClusterIndex(strTableName, htblColNameValue);
				updateNonClusterIndex(strTableName);
			} else {
				Enumeration<Object> e = htblColNameValue.elements();
				ArrayList<Integer> index = getKeyPosition(htblColNameValue.keys(), strTableName);
				searchDelete(strTableName, e, index);
				updateNonClusterIndex(strTableName);
			}
		} catch (IOException | ParseException e) {
		}
	}

	public static ArrayList<Integer> getKeyPosition(Enumeration<String> e, String strTableName) throws IOException {
		ArrayList<Integer> res = new ArrayList<Integer>();
		while (e.hasMoreElements()) {
			int count = 1;
			String f = e.nextElement();
			ArrayList<String> csvRead = readPage(path + "MetaData.csv");
			for (int i = 0; i < csvRead.size(); i++) {
				String[] s = csvRead.get(i).split(",");
				if (s[0].compareTo(strTableName) == 0) {
					if (s[1].compareTo(f) == 0)
						res.add(count);
					else
						count++;
				}
			}
		}
		return res;
	}

	public static void searchDelete(String strTableName, Enumeration<Object> e, ArrayList<Integer> a)
			throws IOException {
		ArrayList<Object> en = new ArrayList<Object>();
		while (e.hasMoreElements())
			en.add(e.nextElement());
		for (int i = 1; i <= numOfPages(strTableName); i++) {
			ArrayList<String> csvRead = readPage(path + strTableName + i + ".csv");
			int count = 0;
			for (int j = 0; j < csvRead.size(); j++) {
				String[] s = csvRead.get(j).split(",");
				boolean f = false;
				for (int n = 0; n < en.size(); n++) {
					String compare = en.get(n).toString();
					if (compare.compareTo(s[a.get(n) - 1]) != 0) {
						f = false;
						count++;
						break;
					} else
						f = true;
				}
				if (f) {
					if (hasIndex(strTableName, getclusteringKey(strTableName))) {
						ArrayList<String> index = readPage(
								path + strTableName + getclusteringKey(strTableName) + ".csv");
						if (count == 0 && csvRead.size() == 1) {
							csvRead.remove(count);
							index.remove(i - 1);
							for (int n = i - 1; n < index.size(); n++) {
								String change = index.get(n);
								String[] split = change.split(",");
								split[1] = n + 1 + "";
								change = split[0] + "," + split[1];
								index.set(n, change);
							}
						} else if (count == 0 && csvRead.size() > 1) {
							csvRead.remove(count);
							index.set(i - 1, csvRead.get(0).split(",")[keyIndex(strTableName)] + "," + (i));
						} else
							csvRead.remove(count);
						write(index, path + strTableName + getclusteringKey(strTableName) + ".csv");
					} else
						csvRead.remove(count);
					write(csvRead, path + strTableName + i + ".csv");
					if (isEmpty(path + strTableName + i + ".csv")) {
						deletePage(path + strTableName, i, numOfPages(strTableName));
						i--;
						break;
					}
					j--;
					count = 0;
				}
			}
		}
	}

	public static void deletePage(String strTableName, int pageNum, int pages) throws IOException {
		for (int n = pageNum; n < pages; n++) {
			int c = n + 1;
			ArrayList<String> f = readPage(strTableName + c + ".csv");
			write(f, strTableName + n + ".csv");
		}
		File del = new File(strTableName + pages + ".csv");
		del.delete();
		ArrayList<String> unsorted = readPage(path + "Files.csv");
		for (int i = 0; i < unsorted.size(); i++) {
			String[] s = unsorted.get(i).split(",");
			if ((s[3] + s[2]).compareTo(strTableName + pages + ".csv") == 0) {
				unsorted.remove(i);
				break;
			}
		}
		write(unsorted, path + "Files.csv");
	}

	public static boolean hasIndex(String strTableName, String colname) throws IOException {
		FileReader fileReader = new FileReader(path + "MetaData.csv");
		BufferedReader reader = new BufferedReader(fileReader);
		String strCurrentLine = "";
		while ((strCurrentLine = reader.readLine()) != null) {
			String[] s = strCurrentLine.split(",");
			if (s[0].equals(strTableName)) {
				if (s[1].equals(colname) && s[5].equals("SparseIndex")) {
					reader.close();
					return true;
				}
			}
		}
		reader.close();
		return false;
	}

	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException, ParseException {
		if (!tableExists(strTableName))
			throw new DBAppException("table does not exist");
		htblColNameValue = lowerCase(strTableName, htblColNameValue);
		int index = keyIndex(strTableName);
		String cluster = getclusteringKey(strTableName);
		ArrayList<String> res = new ArrayList<>();
		Enumeration<String> keys = htblColNameValue.keys();
		ArrayList<String> key = new ArrayList<String>();
		while (keys.hasMoreElements()) {
			String t = keys.nextElement();
			key.add(t);
		}
		ArrayList<Integer> indeces = getIndeces(key, strTableName);
		String page = "";
		if (hasIndex(strTableName, cluster)) {
			page = getPageWithIndex(strTableName, strClusteringKeyValue);
		} else {
			page = getPage(strTableName, strClusteringKeyValue);
		}
		updateChecks(page, strTableName, htblColNameValue);
		ArrayList<String> file = readPage(page);
		for (int i = 0; i < file.size(); i++) {
			String[] s = file.get(i).split(",");
			if (s[index].equals(strClusteringKeyValue)) {
				for (int j = 0; j < indeces.size(); j++) {
					s[indeces.get(j)] = htblColNameValue.get(key.get(j)) + "";
				}
				String result = String.join(",", s);
				res.add(result);
			} else {
				res.add(file.get(i));
			}
		}
		write(res, page);
		updateNonClusterIndex(strTableName);
	}

	public static String getPageWithIndex(String strTableName, String strClusteringKeyValue)
			throws IOException, ParseException, DBAppException {
		String cluster = getclusteringKey(strTableName);
		String fileName = strTableName + cluster;
		String keyType = getKeyType(strTableName);
		int page = 0;
		FileReader fileReader = new FileReader(path + fileName + ".csv");
		BufferedReader reader = new BufferedReader(fileReader);
		String strCurrentLine = "";
		while ((strCurrentLine = reader.readLine()) != null) {
			String[] s = strCurrentLine.split(",");
			if (keyType.equals("java.lang.Integer")) {
				if (Integer.parseInt(s[0]) <= Integer.parseInt(strClusteringKeyValue)) {
					page = Integer.parseInt(s[1]);
				} else
					break;
			}
			if (keyType.equals("java.lang.Double")) {
				if (Double.parseDouble(s[0]) <= Double.parseDouble(strClusteringKeyValue)) {
					page = Integer.parseInt(s[1]);
				} else
					break;
			}
			if (keyType.equals("java.lang.String")) {
				if (s[0].compareTo(strClusteringKeyValue) <= 0) {
					page = Integer.parseInt(s[1]);
				} else
					break;
			}
			if (keyType.equals("java.lang.Date")) {
				Date d1 = new SimpleDateFormat("dd/MM/yyyy").parse(s[0]);
				Date d2 = new SimpleDateFormat("dd/MM/yyyy").parse(strClusteringKeyValue);
				if (d1.compareTo(d2) <= 0) {
					page = Integer.parseInt(s[1]);
				} else
					break;
			}
		}
		reader.close();
		if (page == 0) {
			return "";
		} else
			return path + strTableName + page + ".csv";
	}

	public static String getPage(String strTableName, String strClusteringKeyValue) throws IOException {
		int index = keyIndex(strTableName);
		String page = "";
		for (int i = 1; i <= numOfPages(strTableName); i++) {
			ArrayList<String> csvRead = readPage(path + strTableName + i + ".csv");
			for (int j = 0; j < csvRead.size(); j++) {
				String[] s = csvRead.get(j).split(",");
				if (s[index].equals(strClusteringKeyValue)) {
					page = path + strTableName + i + ".csv";
					break;
				}
			}
		}
		return page;
	}

	public static ArrayList<Integer> getIndeces(ArrayList<String> col, String strTableName) throws IOException {
		ArrayList<Integer> indeces = new ArrayList<Integer>();
		ArrayList<String> csvRead = readPage(path + "MetaData.csv");
		for (int i = 0; i < col.size(); i++) {
			int count = 0;
			for (int j = 0; j < csvRead.size(); j++) {
				String[] s = csvRead.get(j).split(",");
				if (s[0].compareTo(strTableName) == 0)
					if (s[1].equals(col.get(i)))
						indeces.add(count);
					else
						count++;
			}
		}
		return indeces;
	}

	public static int keyIndex(String strTableName) throws IOException {
		ArrayList<String> csvRead = readPage(path + "MetaData.csv");
		int c = 0;
		for (int i = 0; i < csvRead.size(); i++) {
			String[] s = csvRead.get(i).split(",");
			if (s[0].compareTo(strTableName) == 0) {
				if (s[3].equals("True")) {
					break;
				} else
					c++;
			}
		}
		return c;
	}

	public static String getclusteringKey(String strTableName) throws IOException {
		ArrayList<String> csvRead = readPage(path + "MetaData.csv");
		for (int i = 0; i < csvRead.size(); i++) {
			String[] s = csvRead.get(i).split(",");
			if (s[0].compareTo(strTableName) == 0)
				if (s[3].equals("True"))
					return s[1];
		}
		return "";
	}

	public static String getKeyType(String strTableName) throws IOException {
		ArrayList<String> csvRead = readPage(path + "MetaData.csv");
		for (int i = 0; i < csvRead.size(); i++) {
			String[] s = csvRead.get(i).split(",");
			if (s[0].compareTo(strTableName) == 0)
				if (s[3].equals("True"))
					return s[2];
		}
		return "";
	}

	public static void write(ArrayList<String> s, String page) throws IOException {
		FileWriter w = new FileWriter(page);
		for (int i = 0; i < s.size(); i++) {
			w.write(s.get(i) + System.lineSeparator());
		}
		w.close();
	}

	public static void updateChecks(String page, String tablename, Hashtable<String, Object> h)
			throws DBAppException, IOException, ParseException {
		if (page.equals(""))
			throw new DBAppException("invalid values");
		if (!validValues(tablename, h))
			throw new DBAppException("invalid U values");
		if (!validRange(tablename, h))
			throw new DBAppException("invalid R values");
	}

	public static boolean validValues(String strTableName, Hashtable<String, Object> h) throws IOException {
		Enumeration<String> keys = h.keys();
		ArrayList<String> k = new ArrayList<String>();
		while (keys.hasMoreElements()) {
			k.add(keys.nextElement());
		}
		for (int i = 0; i < k.size(); i++) {
			String s = getType(strTableName, k.get(i));
			if (!s.equals(h.get(k.get(i)).getClass().getName()))
				return false;
		}
		return true;
	}

	public static String getType(String strTableName, String column) throws IOException {
		ArrayList<String> csvRead = readPage(path + "MetaData.csv");
		for (int i = 0; i < csvRead.size(); i++) {
			String[] s = csvRead.get(i).split(",");
			if (s[0].compareTo(strTableName) == 0)
				if (s[1].equals(column))
					return s[2];
		}
		return "";
	}

	public static String getMin(String strTableName, String column) throws IOException {
		ArrayList<String> csvRead = readPage(path + "MetaData.csv");
		for (int i = 0; i < csvRead.size(); i++) {
			String[] s = csvRead.get(i).split(",");
			if (s[0].compareTo(strTableName) == 0)
				if (s[1].equals(column))
					return s[6];
		}
		return "";
	}

	public static String getMax(String strTableName, String column) throws IOException {
		ArrayList<String> csvRead = readPage(path + "MetaData.csv");
		for (int i = 0; i < csvRead.size(); i++) {
			String[] s = csvRead.get(i).split(",");
			if (s[0].compareTo(strTableName) == 0)
				if (s[1].equals(column))
					return s[7];
		}
		return "";
	}

	public static boolean validRange(String strTableName, Hashtable<String, Object> h)
			throws IOException, ParseException {
		Enumeration<String> keys = h.keys();
		ArrayList<String> k = new ArrayList<String>();
		while (keys.hasMoreElements()) {
			k.add(keys.nextElement());
		}
		for (int i = 0; i < k.size(); i++) {
			String s = h.get(k.get(i)) + "";
			String min = getMin(strTableName, k.get(i));
			String max = getMax(strTableName, k.get(i));
			if (h.get(k.get(i)).getClass().getName().equals("java.lang.String")) {
				if (s.compareTo(min) < 0 || s.compareTo(max) > 0) {
					return false;
				}
			}
			if (h.get(k.get(i)).getClass().getName().equals("java.lang.Integer")) {
				if (Integer.parseInt(s) > Integer.parseInt(max) || (Integer.parseInt(s) < Integer.parseInt(min))) {
					return false;
				}
			}
			if (h.get(k.get(i)).getClass().getName().equals("java.lang.Double")) {
				if (Double.parseDouble(s) > Double.parseDouble(max)
						|| (Double.parseDouble(s) < Double.parseDouble(min))) {
					return false;
				}
			}
			if (h.get(k.get(i)).getClass().getName().equals("java.lang.Date")) {
				Date d1 = new SimpleDateFormat("dd/MM/yyyy").parse(s);
				Date d2 = new SimpleDateFormat("dd/MM/yyyy").parse(min);
				Date d3 = new SimpleDateFormat("dd/MM/yyyy").parse(max);
				if (d1.compareTo(d2) < 0 || d1.compareTo(d3) > 0) {
					return false;
				}
			}
		}
		return true;
	}

	public static boolean isFull(String fileName) throws IOException {
		ArrayList<String> csvRead = readPage(fileName);
		if (csvRead.size() == maxRecords)
			return true;
		else
			return false;
	}

	public static boolean isEmpty(String fileName) throws IOException {
		ArrayList<String> csvRead = readPage(fileName);
		return csvRead.isEmpty();
	}

	public static int numOfPages(String strTableName) throws IOException {
		ArrayList<String> csvRead = readPage(path + "Files.csv");
		int count = 0;
		for (int i = 0; i < csvRead.size(); i++) {
			String[] s = csvRead.get(i).split(",");
			if (s[0].compareTo(strTableName) == 0)
				count++;
		}
		return count;
	}

	public static String getClusterVal(String str, String strTableName) throws IOException {
		String[] sperated = str.split(",");
		return sperated[keyIndex(strTableName)];
	}

	public static void insert2(String strTableName, int n, String insert)
			throws IOException, DBAppException, ParseException {
		ArrayList<String> s = new ArrayList<String>();
		boolean f = false;
		for (int i = n; i <= numOfPages(strTableName); i++) {
			String clusteringVal = getClusterVal(insert, strTableName);
			String clusterType = getKeyType(strTableName);
			String page = path + strTableName + i + ".csv";
			ArrayList<String> csvRead = readPage(page);
			for (int j = 0; j < csvRead.size(); j++) {
				if (clusterType.equals("java.lang.String")) {
					String str = getClusterVal(csvRead.get(j), strTableName);
					if (str.compareTo(clusteringVal) > 0 && f == false) {
						s.add(insert);
						f = true;
					}
					s.add(csvRead.get(j));
				} else if (clusterType.equals("java.lang.Date")) {
					Date str = new SimpleDateFormat("dd/MM/yyyy").parse(getClusterVal(csvRead.get(j), strTableName));
					Date comp = new SimpleDateFormat("dd/MM/yyyy").parse(clusteringVal);
					if (str.compareTo(comp) > 0 && f == false) {
						s.add(insert);
						f = true;
					}
					s.add(csvRead.get(j));
				} else if (clusterType.equals("java.lang.Integer")) {
					int str = Integer.parseInt(getClusterVal(csvRead.get(j), strTableName));
					if (str > (Integer.parseInt(clusteringVal)) && f == false) {
						s.add(insert);
						f = true;
					}
					s.add(csvRead.get(j));
				} else if (clusterType.equals("java.lang.Double")) {
					Double str = Double.parseDouble(getClusterVal(csvRead.get(j), strTableName));
					if (str > (Double.parseDouble(clusteringVal)) && f == false) {
						s.add(insert);
						f = true;
					}
					s.add(csvRead.get(j));
				} else
					throw new DBAppException("invalid type");
			}
			if (!isFull(page)) {
				break;
			}
		}
		if (!f)
			s.add(insert);
		int index = 0;
		for (int j = n; j <= numOfPages(strTableName); j++) {
			int count = 0;
			String p1 = path + strTableName + j + ".csv";
			FileWriter w = new FileWriter(p1);
			while (index < s.size()) {
				w.write(s.get(index) + System.lineSeparator());
				count++;
				index++;
				if (count == maxRecords) {
					break;
				}
			}
			w.close();
			if (index == s.size())
				break;
			if (index < s.size() && j + 1 > numOfPages(strTableName)) {
				createNewFile(strTableName);
			}
		}

	}

	public static void insertUsingClusterIndex(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws IOException, ParseException, DBAppException {
		Enumeration<Object> e = htblColNameValue.elements();
		Enumeration<String> k = htblColNameValue.keys();
		String values = "";
		if (e.hasMoreElements()) {
			Object o = e.nextElement();
			String s = k.nextElement();
			String type = getType(strTableName, s);
			if (type.equals("java.lang.String")) {
				o = o.toString().toLowerCase();
			}
			values += o;
		}

		while (e.hasMoreElements()) {
			Object o = e.nextElement();
			String s = k.nextElement();
			String type = getType(strTableName, s);
			if (type.equals("java.lang.String")) {
				o = o.toString().toLowerCase();
			}
			values += "," + o;
		}
		if (numOfRecords(strTableName) == 0) {
			String fileName = strTableName + getclusteringKey(strTableName);
			String indexName = strTableName + "_" + getclusteringKey(strTableName);
			String s = strTableName + ",Table," + strTableName + 1 + ".csv," + path + System.lineSeparator();
			File n = new File(path + strTableName + 1 + ".csv");
			n.createNewFile();
			Path path1 = Paths.get(path + "Files.csv");
			Files.write(path1, s.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
			insertToSorted(path + strTableName + "1.csv", values, strTableName);
			File index = new File(path + fileName + ".csv");
			index.createNewFile();
			String string = indexName + ",SparseIndex," + fileName + ".csv" + "," + path + System.lineSeparator();
			Path path2 = Paths.get(path + "Files.csv");
			Files.write(path2, string.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
			ArrayList<String> add = new ArrayList<String>();
			add.add(getClusterVal(values, strTableName) + ",1");
			write(add, path + fileName + ".csv");
			updateNonClusterIndex(strTableName);
			return;
		} else {
			String clusteringKey = getclusteringKey(strTableName);
			Object o = htblColNameValue.get(clusteringKey);
			String keyType = getKeyType(strTableName);
			String p = path + strTableName + clusteringKey + ".csv";
			ArrayList<String> csvRead = readPage(p);
			int reference = 0;
			if (keyType.equals("java.lang.Integer")) {
				int value = (int) o;
				for (int i = 0; i < csvRead.size(); i++) {
					String[] s = csvRead.get(i).split(",");
					int compare = Integer.parseInt(s[0]);
					if (compare <= value)
						reference = Integer.parseInt(s[1]);
					else
						break;
				}
			} else if (keyType.equals("java.lang.Double")) {
				Double value = (Double) o;
				for (int i = 0; i < csvRead.size(); i++) {
					String[] s = csvRead.get(i).split(",");
					Double compare = Double.parseDouble(s[0]);
					if (compare <= value)
						reference = Integer.parseInt(s[1]);
					else
						break;
				}
			} else if (keyType.equals("java.lang.String")) {
				String value = o.toString();
				for (int i = 0; i < csvRead.size(); i++) {
					String[] s = csvRead.get(i).split(",");
					String compare = s[0];
					if (compare.equals(value) || compare.compareTo(value) < 0) {
						reference = Integer.parseInt(s[1]);
					} else
						break;
				}
			} else if (keyType.equals("java.lang.Date")) {
				Date value = new SimpleDateFormat("dd/MM/yyyy").parse(o.toString());
				for (int i = 0; i < csvRead.size(); i++) {
					String[] s = csvRead.get(i).split(",");
					Date compare = new SimpleDateFormat("dd/MM/yyyy").parse(s[0]);
					if (compare.equals(value) || compare.compareTo(value) < 0) {
						reference = Integer.parseInt(s[1]);
					} else
						break;
				}
			} else
				throw new DBAppException("invalid type");
			insertIntoPageCluster(reference, strTableName, values, csvRead, o, clusteringKey, p, htblColNameValue);
		}
	}

	public static ArrayList<String> getNonClusterIndeces(String strTableName) throws IOException {
		ArrayList<String> meta = readPage(path + "MetaData.csv");
		ArrayList<String> res = new ArrayList<String>();
		for (int i = 0; i < meta.size(); i++) {
			String[] str = meta.get(i).split(",");
			if (str[0].equals(strTableName) && str[3].equals("False") && !str[4].equals("null")) {
				res.add(str[1]);
			}
		}
		return res;
	}

	public static void updateNonClusterIndex(String strTableName) throws IOException {
		ArrayList<String> indeces = getNonClusterIndeces(strTableName);
		for (int i = 0; i < indeces.size(); i++) {
			ArrayList<String> files = readPage(path + "Files.csv");
			for (int j = 0; j < files.size(); j++) {
				String[] s = files.get(j).split(",");
				if (s[0].compareTo(strTableName + "_" + indeces.get(i)) == 0) {
					files.remove(j);
					File remove = new File(s[3] + s[2]);
					remove.delete();
					j--;
				}
			}
			write(files, path + "Files.csv");
			if (numOfRecords(strTableName) == 1) {
				ArrayList<String> table = readPage(path + strTableName + 1 + ".csv");
				int colIndex = getColIndex(strTableName, indeces.get(i));
				String insert = table.get(0).split(",")[colIndex];
				File dense = new File(path + strTableName + indeces.get(i) + 1 + ".csv");
				dense.createNewFile();
				String string = strTableName + "_" + indeces.get(i) + ",DenseIndex," + strTableName + indeces.get(i) + 1
						+ ".csv" + "," + path + System.lineSeparator();
				Path path1 = Paths.get(path + "Files.csv");
				Files.write(path1, string.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
				File sparse = new File(path + strTableName + indeces.get(i) + ".csv");
				sparse.createNewFile();
				String string1 = strTableName + "_" + indeces.get(i) + ",SparseIndex," + strTableName + indeces.get(i)
				+ ".csv" + "," + path + System.lineSeparator();
				Path path2 = Paths.get(path + "Files.csv");
				Files.write(path2, string1.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
				ArrayList<String> den = new ArrayList<String>();
				ArrayList<String> spar = new ArrayList<String>();
				den.add(insert + ",1_1");
				spar.add(insert + ",1");
				write(den, path + strTableName + indeces.get(i) + 1 + ".csv");
				write(spar, path + strTableName + indeces.get(i) + ".csv");
			} else {
				ArrayList<String> dense = denseIndex(strTableName, indeces.get(i));
				indexOnNonClustering(dense, strTableName, strTableName + indeces.get(i),
						strTableName + "_" + indeces.get(i));
			}
		}

	}

	public static void insertIntoPageCluster(int reference, String strTableName, String values,
			ArrayList<String> csvRead, Object o, String clusteringKey, String p,
			Hashtable<String, Object> htblColNameValue) throws IOException, ParseException, DBAppException {
		if (reference == 0) {
			if (!isFull(path + strTableName + 1 + ".csv")) {
				insertToSorted(path + strTableName + 1 + ".csv", values, strTableName);
				csvRead.set(0, o + ",1");
				write(csvRead, path + strTableName + clusteringKey + ".csv");
			} else {
				insert2(strTableName, 1, values);
				updateClusterIndex(csvRead, 1, strTableName, p);
			}
		} else if (reference == numOfPages(strTableName)) {
			if (!isFull(path + strTableName + reference + ".csv"))
				insertToSorted(path + strTableName + reference + ".csv", values, strTableName);
			else {
				createNewFile(strTableName);
				insert2(strTableName, reference, values);
				csvRead.add(minMax(path + strTableName + numOfPages(strTableName) + ".csv", strTableName)[0] + ","
						+ numOfPages(strTableName));
				write(csvRead, path + strTableName + clusteringKey + ".csv");
			}
		} else {
			if (!isFull(path + strTableName + reference + ".csv")) {
				insertToSorted(path + strTableName + reference + ".csv", values, strTableName);
			} else {
				insert2(strTableName, reference, values);
				updateClusterIndex(csvRead, reference, strTableName, p);
			}
		}
		updateNonClusterIndex(strTableName);
	}

	public static void updateClusterIndex(ArrayList<String> indexFile, int reference, String strTableName, String path1)
			throws IOException {
		if (reference - 1 >= 0 && reference - 1 < indexFile.size()) {
			while (indexFile.size() > reference - 1)
				indexFile.remove(reference - 1);
		}
		if (numOfRecords(strTableName) != 0) {
			while (reference <= numOfPages(strTableName)) {
				ArrayList<String> file = readPage(path + strTableName + reference + ".csv");
				String s = getClusterVal(file.get(0), strTableName) + "," + reference;
				indexFile.add(s);
				reference++;
			}
			write(indexFile, path1);
		}
	}

	public static void findLocation(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws IOException, ParseException, DBAppException {
		int pages = numOfPages(strTableName);
		Enumeration<Object> e = htblColNameValue.elements();
		Enumeration<String> k = htblColNameValue.keys();
		String values = "";
		if (e.hasMoreElements()) {
			Object o = e.nextElement();
			String s = k.nextElement();
			String type = getType(strTableName, s);
			if (type.equals("java.lang.String")) {
				o = o.toString().toLowerCase();
			}
			values += o;
		}

		while (e.hasMoreElements()) {
			Object o = e.nextElement();
			String s = k.nextElement();
			String type = getType(strTableName, s);
			if (type.equals("java.lang.String")) {
				o = o.toString().toLowerCase();
			}
			values += "," + o;
		}

		String comp = getClusterVal(values, strTableName);
		String type = getKeyType(strTableName);
		if (numOfRecords(strTableName) == 0) {
			insertToSorted(path + strTableName + "1.csv", values, strTableName);
			return;
		}

		if (type.compareTo("java.lang.Integer") == 0) {
			for (int i = 1; i <= pages; i++) {
				String[] curr = minMax(path + strTableName + i + ".csv", strTableName);
				String[] next = null;
				boolean flag = false;
				int nextMin = 0;
				if (i != pages) {
					flag = true;
					next = minMax(path + strTableName + (i + 1) + ".csv", strTableName);
					nextMin = Integer.parseInt(next[0]);
				}
				int currentMin = Integer.parseInt(curr[0]);
				int currentMax = Integer.parseInt(curr[1]);
				int compare = Integer.parseInt(comp);
				if (isFull(path + strTableName + i + ".csv")) {
					if (flag) {
						if (compare < nextMin) {
							insert2(strTableName, i, values);
							return;
						}
					} else if (!flag) {
						if (compare > currentMax) {
							createNewFile(strTableName);
							insertToSorted(path + strTableName + numOfPages(strTableName) + ".csv", values,
									strTableName);
							return;
						} else if ((compare > currentMin && compare < currentMax) || (compare < currentMin)) {
							createNewFile(strTableName);
							insert2(strTableName, i, values);
							return;
						}
					}
				} else if (!isFull(path + strTableName + i + ".csv")) {
					if (flag) {
						if ((currentMin == currentMax && compare > currentMin && compare < nextMin)
								|| (compare > currentMin && compare < nextMin) || compare < currentMin) {
							insertToSorted(path + strTableName + i + ".csv", values, strTableName);
							return;
						}
					} else if (!flag) {
						if (compare > currentMin || compare < currentMin) {
							insertToSorted(path + strTableName + i + ".csv", values, strTableName);
							return;
						}
					}
				}

			}
		} else if (type.compareTo("java.lang.String") == 0) {
			for (int i = 1; i <= pages; i++) {
				String[] curr = minMax(path + strTableName + i + ".csv", strTableName);
				String[] next = null;
				boolean flag = false;
				if (i != pages) {
					flag = true;
					next = minMax(path + strTableName + (i + 1) + ".csv", strTableName);
				}

				if (isFull(path + strTableName + i + ".csv")) {
					if (flag) {
						if (comp.compareTo(next[0]) < 0) {
							insert2(strTableName, i, values);
							return;
						}
					} else if (!flag) {
						if (comp.compareTo(curr[1]) > 0) {
							createNewFile(strTableName);
							insertToSorted(path + strTableName + numOfPages(strTableName) + ".csv", values,
									strTableName);
							return;
						} else if ((comp.compareTo(curr[0]) > 0 && comp.compareTo(curr[1]) < 0)
								|| (comp.compareTo(curr[0]) < 0)) {
							createNewFile(strTableName);
							insert2(strTableName, i, values);
							return;
						}
					}
				} else if (!isFull(path + strTableName + i + ".csv")) {
					if (flag) {
						if ((curr[0].compareTo(curr[1]) == 0 && comp.compareTo(curr[0]) > 0
								&& comp.compareTo(next[0]) < 0)
								|| (comp.compareTo(curr[0]) > 0 && comp.compareTo(next[0]) < 0)
								|| comp.compareTo(curr[0]) < 0) {
							insertToSorted(path + strTableName + i + ".csv", values, strTableName);
							return;
						}
					} else if (!flag) {
						if (comp.compareTo(curr[0]) > 0 || comp.compareTo(curr[0]) < 0) {
							insertToSorted(path + strTableName + i + ".csv", values, strTableName);
							return;
						}
					}
				}

			}
		} else if (type.compareTo("java.lang.Double") == 0) {
			for (int i = 1; i <= pages; i++) {
				String[] curr = minMax(path + strTableName + i + ".csv", strTableName);
				String[] next = null;
				boolean flag = false;
				Double nextMin = 0.0;
				if (i != pages) {
					flag = true;
					next = minMax(path + strTableName + (i + 1) + ".csv", strTableName);
					nextMin = Double.parseDouble(next[0]);
				}
				Double currentMin = Double.parseDouble(curr[0]);
				Double currentMax = Double.parseDouble(curr[1]);
				Double compare = Double.parseDouble(comp);
				if (isFull(path + strTableName + i + ".csv")) {
					if (flag) {
						if (compare < nextMin) {
							insert2(strTableName, i, values);
							return;
						}
					} else if (!flag) {
						if (compare > currentMax) {
							createNewFile(strTableName);
							insertToSorted(path + strTableName + numOfPages(strTableName) + ".csv", values,
									strTableName);
							return;
						} else if ((compare > currentMin && compare < currentMax) || (compare < currentMin)) {
							createNewFile(strTableName);
							insert2(strTableName, i, values);
							return;
						}
					}
				} else if (!isFull(path + strTableName + i + ".csv")) {
					if (flag) {
						if ((currentMin == currentMax && compare > currentMin && compare < nextMin)
								|| (compare > currentMin && compare < nextMin) || compare < currentMin) {
							insertToSorted(path + strTableName + i + ".csv", values, strTableName);
							return;
						}
					} else if (!flag) {
						if (compare > currentMin || compare < currentMin) {
							insertToSorted(path + strTableName + i + ".csv", values, strTableName);
							return;
						}
					}
				}

			}
		} else if (type.compareTo("java.lang.Date") == 0) {
			for (int i = 1; i <= pages; i++) {
				String[] curr = minMax(path + strTableName + i + ".csv", strTableName);
				String[] next = null;
				boolean flag = false;
				Date nextMin = null;
				if (i != pages) {
					flag = true;
					next = minMax(path + strTableName + (i + 1) + ".csv", strTableName);
					nextMin = new SimpleDateFormat("dd/MM/yyyy").parse(next[0]);
				}
				Date currentMin = new SimpleDateFormat("dd/MM/yyyy").parse(curr[0]);
				Date currentMax = new SimpleDateFormat("dd/MM/yyyy").parse(curr[1]);
				Date compare = new SimpleDateFormat("dd/MM/yyyy").parse(comp);

				if (isFull(path + strTableName + i + ".csv")) {
					if (flag) {
						if (compare.compareTo(nextMin) < 0) {
							insert2(strTableName, i, values);
							return;
						}
					} else if (!flag) {
						if (compare.compareTo(currentMax) > 0) {
							createNewFile(strTableName);
							insertToSorted(path + strTableName + numOfPages(strTableName) + ".csv", values,
									strTableName);
							return;
						} else if ((compare.compareTo(currentMin) > 0 && compare.compareTo(currentMax) < 0)
								|| (compare.compareTo(currentMin) < 0)) {
							createNewFile(strTableName);
							insert2(strTableName, i, values);
							return;
						}
					}
				} else if (!isFull(path + strTableName + i + ".csv")) {
					if (flag) {
						if ((currentMin.compareTo(currentMax) == 0 && compare.compareTo(currentMin) > 0
								&& compare.compareTo(nextMin) < 0)
								|| (compare.compareTo(currentMin) > 0 && compare.compareTo(nextMin) < 0)
								|| compare.compareTo(currentMin) < 0) {
							insertToSorted(path + strTableName + i + ".csv", values, strTableName);
							return;
						}
					} else if (!flag) {
						if (compare.compareTo(currentMin) > 0 || compare.compareTo(currentMin) < 0) {
							insertToSorted(path + strTableName + i + ".csv", values, strTableName);
							return;
						}
					}
				}
			}
		}
	}

	public static int insertToSorted(String filePath, String insert, String strTableName)
			throws IOException, ParseException {
		String ins = getClusterVal(insert, strTableName);
		int count = 0;
		ArrayList<String> sorted = readPage(filePath);
		String type = getKeyType(strTableName);
		if (type.compareTo("java.lang.Integer") == 0) {
			for (int i = 0; i < sorted.size(); i++) {
				int ins1 = Integer.parseInt(ins);
				int clu = Integer.parseInt(getClusterVal(sorted.get(i), strTableName));
				if (ins1 > clu)
					count++;
			}
		} else if (type.compareTo("java.lang.String") == 0) {
			for (int i = 0; i < sorted.size(); i++) {
				if (ins.compareTo(getClusterVal(sorted.get(i), strTableName)) > 0)
					count++;
			}
		} else if (type.compareTo("java.lang.Double") == 0) {
			for (int i = 0; i < sorted.size(); i++) {
				Double ins1 = Double.parseDouble(ins);
				Double clu = Double.parseDouble(getClusterVal(sorted.get(i), strTableName));
				if (ins1 > clu)
					count++;
			}
		} else if (type.compareTo("java.lang.Date") == 0) {
			for (int i = 0; i < sorted.size(); i++) {
				Date ins1 = new SimpleDateFormat("dd/MM/yyyy").parse(ins);
				Date clu = new SimpleDateFormat("dd/MM/yyyy").parse(getClusterVal(sorted.get(i), strTableName));
				if (ins1.after(clu))
					count++;
			}
		}
		sorted.add(count, insert);
		write(sorted, filePath);
		return count;
	}

	public static int getColIndex(String strTableName, String colname) throws IOException {
		FileReader fileReader = new FileReader(path + "MetaData.csv");
		BufferedReader reader = new BufferedReader(fileReader);
		String strCurrentLine = "";
		int c = 0;
		while ((strCurrentLine = reader.readLine()) != null) {
			String[] s = strCurrentLine.split(",");
			if (s[0].equals(strTableName)) {
				if (s[1].equals(colname))
					break;
				else
					c++;
			}
		}
		reader.close();
		return c;
	}

	public static String[] minMax(String filePath, String strTableName) throws IOException {
		String[] res = new String[2];
		ArrayList<String> csvRead = readPage(filePath);
		String min = getClusterVal(csvRead.get(0), strTableName);
		res[0] = min;
		if (csvRead.size() == 1)
			res[1] = min;
		else
			res[1] = getClusterVal(csvRead.get(csvRead.size() - 1), strTableName);
		return res;
	}

	public static void createNewFile(String strTableName) throws IOException {
		int page = numOfPages(strTableName) + 1;
		String s = strTableName + ",Table," + strTableName + page + ".csv," + path;
		File n = new File(path + strTableName + page + ".csv");
		n.createNewFile();
		Path path1 = Paths.get(path + "Files.csv");
		Files.write(path1, (s + System.lineSeparator()).getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
	}

	public static int numOfRecords(String strTableName) throws IOException {
		int count = 0;
		for (int i = 1; i <= numOfPages(strTableName); i++) {
			ArrayList<String> csvRead = readPage(path + strTableName + i + ".csv");
			count += csvRead.size();
		}
		return count;
	}

	public static int numOfColumns(String strTableName) throws IOException {
		ArrayList<String> meta = readPage(path + "MetaData.csv");
		int count = 0;
		for (int i = 0; i < meta.size(); i++) {
			String[] s = meta.get(i).split(",");
			if (s[0].equals(strTableName))
				count++;
		}
		return count;
	}

	public static Hashtable<String, Object> lowerCase(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws IOException {
		Enumeration<String> e = htblColNameValue.keys();
		Hashtable<String, Object> res = new Hashtable<String, Object>();
		while (e.hasMoreElements()) {
			String curr = e.nextElement();
			if (getType(strTableName, curr).equals("java.lang.String")) {
				String s = new String(htblColNameValue.get(curr).toString().toLowerCase());
				res.put(curr, s);
			} else
				res.put(curr, htblColNameValue.get(curr));
		}
		return res;
	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		try {
			if (!tableExists(strTableName))
				throw new DBAppException("table does not exist");
			if (numOfColumns(strTableName) != htblColNameValue.size())
				throw new DBAppException("can not insert null");
			if (!validRange(strTableName, htblColNameValue))
				throw new DBAppException("invalid column or datatype");
			if (numOfRecords(strTableName) == maxRecords * 200)
				throw new DBAppException("table is full");
			htblColNameValue = lowerCase(strTableName, htblColNameValue);
			if ((!tableHasIndex(strTableName, htblColNameValue).isEmpty())
					&& (hasClusteringIndex(strTableName, htblColNameValue))) {
				insertUsingClusterIndex(strTableName, htblColNameValue);
			} else {
				if (numOfRecords(strTableName) == 0) {
					String s = strTableName + ",Table," + strTableName + 1 + ".csv," + path + System.lineSeparator();
					File n = new File(path + strTableName + 1 + ".csv");
					n.createNewFile();
					Path path1 = Paths.get(path + "Files.csv");
					Files.write(path1, s.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
				} else {
					if (PKExists(strTableName, htblColNameValue))
						throw new DBAppException("value already exists");
				}
				findLocation(strTableName, htblColNameValue);
				updateNonClusterIndex(strTableName);
			}

		} catch (IOException | ParseException e) {
		}
	}

	public static boolean PKExists(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws IOException, DBAppException {
		String clusteringKey = getclusteringKey(strTableName);
		Object clusteringKeyValue = htblColNameValue.get(clusteringKey);
		int pages = numOfPages(strTableName);
		Boolean f = false;
		for (int i = 1; i <= pages; i++) {
			f = readFromCSV(path + strTableName + i + ".csv", strTableName, clusteringKeyValue);
			if (f == true)
				break;
		}
		return f;
	}

	public static Boolean readFromCSV(String filename, String strTableName, Object clusteringKeyValue)
			throws IOException, DBAppException {
		FileReader fileReader = new FileReader(filename);
		BufferedReader reader = new BufferedReader(fileReader);
		String strCurrentLine = "";
		Boolean f = false;
		int index = keyIndex(strTableName);
		while ((strCurrentLine = reader.readLine()) != null) {
			String[] s = strCurrentLine.split(",");

			if (s[index].equals(clusteringKeyValue)) {
				f = true;
				break;
			}
		}
		fileReader.close();
		reader.close();
		return f;
	}

	public void createIndex(String strTableName, String strColName) throws DBAppException, IOException {
		if (hasIndex(strTableName, strColName)) {
			throw new DBAppException("index already created");
		}
		String indexName = strTableName + "_" + strColName;
		String fileName = strTableName + strColName;
		String key = getclusteringKey(strTableName);
		ArrayList<String> arr = readPage(path + "MetaData.csv");
		for (int i = 0; i < arr.size(); i++) {
			String[] s = arr.get(i).split(",");
			if (s[1].equals(strColName)) {
				s[4] = fileName;
				s[5] = "SparseIndex";
				String u = "";
				for (int j = 0; j < s.length; j++)
					u += s[j] + ",";
				arr.set(i, u);
			}
		}
		write(arr, path + "MetaData.csv");
		if (key.equals(strColName)) {
			indexOnClustering(strTableName, fileName, indexName);
		} else {
			ArrayList<String> index = denseIndex(strTableName, strColName);
			indexOnNonClustering(index, strTableName, strTableName + strColName, indexName);
		}

	}

	public static void indexOnClustering(String strTableName, String fileName, String indexName) throws IOException {
		ArrayList<String> s = new ArrayList<>();
		if (numOfRecords(strTableName) != 0) {
			File index = new File(path + fileName + ".csv");
			index.createNewFile();
			String string = indexName + ",SparseIndex," + fileName + ".csv" + "," + path + System.lineSeparator();
			Path path1 = Paths.get(path + "Files.csv");
			Files.write(path1, string.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
			int c = 1;
			while (c <= numOfPages(strTableName)) {
				FileReader fileReader = new FileReader(path + strTableName + c + ".csv");
				BufferedReader reader = new BufferedReader(fileReader);
				String str = reader.readLine();
				reader.close();
				String reference = getClusterVal(str, strTableName).toLowerCase() + "," + c;
				s.add(reference);
				c++;
			}
			write(s, path + fileName + ".csv");
		}
	}

	public static ArrayList<String> denseIndex(String strTableName, String strColName) throws IOException {
		ArrayList<String> index = new ArrayList<>();
		int column = getColIndex(strTableName, strColName);
		for (int i = 1; i <= numOfPages(strTableName); i++) {
			ArrayList<String> page = readPage(path + strTableName + i + ".csv");
			for (int j = 0; j < page.size(); j++) {
				String str = (page.get(j).split(",")[column]).toLowerCase() + "," + i + "_"
						+ getClusterVal(page.get(j), strTableName);
				index.add(str);
			}
		}
		Collections.sort(index);
		return index;
	}

	public static void indexOnNonClustering(ArrayList<String> index, String strTableName, String fileName,
			String indexName) throws IOException {
		if (numOfRecords(strTableName) != 0) {
			int c = 0;
			int indexPages = 1;
			for (int i = 1; i <= numOfPages(strTableName); i++) {
				File dense = new File(path + fileName + i + ".csv");
				dense.createNewFile();
				String string = indexName + ",DenseIndex," + fileName + i + ".csv" + "," + path
						+ System.lineSeparator();
				Path path1 = Paths.get(path + "Files.csv");
				Files.write(path1, string.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
				ArrayList<String> file = new ArrayList<String>();
				for (int j = 0; c < index.size(); j++) {
					file.add(index.get(c));
					if (j == maxRecords - 1) {
						c += 1;
						break;
					}
					c += 1;
				}
				write(file, path + fileName + i + ".csv");
				if (c == index.size()) {
					break;
				} else
					indexPages++;
			}
			File sparse = new File(path + fileName + ".csv");
			sparse.createNewFile();
			String string = indexName + ",SparseIndex," + fileName + ".csv" + "," + path + System.lineSeparator();
			Path path1 = Paths.get(path + "Files.csv");
			Files.write(path1, string.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
			ArrayList<String> s = new ArrayList<>();
			for (int i = 1; i <= indexPages; i++) { // should we add it in the files.csv???
				FileReader fileReader = new FileReader(path + fileName + i + ".csv");
				BufferedReader reader = new BufferedReader(fileReader);
				String str = reader.readLine();
				reader.close();
				fileReader.close();
				String reference = str.split(",")[0] + "," + i;
				s.add(reference);
			}
			write(s, path + fileName + ".csv");
		}
	}

	public static ArrayList<String> read(String tableName) throws IOException {
		ArrayList<String> arr = new ArrayList<>();
		for (int i = 1; i <= numOfPages(tableName); i++) {
			FileReader fileReader = new FileReader(path + tableName + i + ".csv");
			BufferedReader reader = new BufferedReader(fileReader);
			String strCurrentLine = "";
			while ((strCurrentLine = reader.readLine()) != null) {
				arr.add(strCurrentLine);
			}
			reader.close();
		}
		return arr;
	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators)
			throws DBAppException, ParseException {
		ArrayList<String> res = new ArrayList<>();
		ArrayList<String> table = new ArrayList<>();
		ArrayList<Integer> temp = new ArrayList<>();
		ArrayList<String> t = new ArrayList<>();
		int c = 0, count = 0, index = 0;
		String tableName = "", column = "", op = "";
		Object value = null;
		boolean f = false;
		if (arrSQLTerms.length - 1 != strarrOperators.length) {
			throw new DBAppException("invalid expression");
		}
		for (int i = 0; i < strarrOperators.length; i++) {
			if (strarrOperators[i].equals("AND"))
				count++;
		}
		try {
			while (c < arrSQLTerms.length && index < arrSQLTerms.length) {
				if (count == strarrOperators.length) {
					tableName = arrSQLTerms[index]._strTableName;
					column = arrSQLTerms[index]._strColumnName;
					value = arrSQLTerms[index]._objValue;
					op = arrSQLTerms[index]._strOperator;
				} else {
					tableName = arrSQLTerms[c]._strTableName;
					column = arrSQLTerms[c]._strColumnName;
					value = arrSQLTerms[c]._objValue;
					op = arrSQLTerms[c]._strOperator;
				}
				if(getType(tableName, column).equals("java.lang.String")) 
					column = column.toLowerCase();
				if (count == strarrOperators.length) {
					f = true;
					if (index == 0) {
						if (hasIndex(tableName, column)) {
							if (column.equals(getclusteringKey(tableName))) {
								res = selectWithIndex(column, tableName, value, op);
								t = selectWithIndex(column, tableName, value, op);
							} else {
								res = select1(column, tableName, value, op);
								t = select1(column, tableName, value, op);
							}
						} else {
							res = seqSearch(column, tableName, value, op);
							t = seqSearch(column, tableName, value, op);
						}
						index++;
					} else {
						if (hasIndex(tableName, column)) {
							if (column.equals(getclusteringKey(tableName))) {
								t = selectWithIndex(column, tableName, value, op);
								res = andOp(res, t);
							} else {
								t = select1(column, tableName, value, op);
								res = andOp(res, t);
							}
						} else {
							t = seqSearch(column, tableName, value, op);
							res = andOp(res, t);
						}
						index++;
					}
				} else {
					try {
						table = read(tableName);
					} catch (IOException e) {
						e.printStackTrace();
					}
					if (c == 0) {
						for (int i = 0; i < table.size(); i++) {
							String columnValue = table.get(i).split(",")[getColIndex(tableName, column)];
							temp.add(oper(op, column, value, columnValue));
						}
						c++;
					} else {
						for (int i = 0; i < table.size(); i++) {
							String columnValue = table.get(i).split(",")[getColIndex(tableName, column)];
							int j = oper(op, column, value, columnValue);
							switch (strarrOperators[c - 1]) {
							case "AND":
								int and = temp.get(i) & j;
								temp.set(i, and);
								break;
							case "OR":
								int or = temp.get(i) | j;
								temp.set(i, or);
								break;
							case "XOR":
								int xor = temp.get(i) ^ j;
								temp.set(i, xor);
								break;
							default:
								throw new DBAppException("invalid operator");
							}
						}
						c++;
					}
				}
			}
		}
		catch(IOException | ParseException e) {}
		for (int i = 0; i < temp.size() && f == false; i++) {
			if (temp.get(i) == 1) {
				res.add(table.get(i));
			}

		}
		return res.iterator();
	}


	public static ArrayList<String> selectWithIndex(String column, String table, Object value, String op)
			throws IOException, ParseException, DBAppException {

		String fileName = table + column;
		ArrayList<String> sparse = readFile(path + fileName + ".csv");
		String type = getType(table, column);
		int page = 0;
		int col = getColIndex(table, column);
		ArrayList<String> res = new ArrayList<>();
		ArrayList<String> temp = new ArrayList<>();
		for (int i = 0; i < sparse.size(); i++) {
			if (compareValues(type, sparse.get(i).split(",")[0], value + "") <= 0) {
				page = Integer.parseInt(sparse.get(i).split(",")[1]);
			} else
				break;
		}
		if (page == 0) {

		}
		switch (op) {
		case "=": {
			temp = readFile(path + table + page + ".csv");
			for (int i = 0; i < temp.size(); i++) {
				if (temp.get(i).split(",")[col].equals(value + "")) {
					res.add(temp.get(i));
					break;
				}
			}
		}
		;
		break;
		case "<": {
			boolean f = false;
			for (int i = 1; i <= page && f == false; i++) {
				temp = readFile(path + table + i + ".csv");
				for (int j = 0; j < temp.size(); j++) {
					if (compareValues(type, temp.get(j).split(",")[col], value + "") < 0)
						res.add(temp.get(j));
					else {
						f = true;
						break;
					}
				}
			}
		}
		;
		break;
		case "<=": {
			boolean f = false;
			for (int i = 1; i <= page && f == false; i++) {
				temp = readFile(path + table + i + ".csv");
				for (int j = 0; j < temp.size(); j++) {
					if (compareValues(type, temp.get(j).split(",")[col], value + "") <= 0)
						res.add(temp.get(j));
					else {
						f = true;
						break;
					}
				}
			}
		}
		;
		break;
		case ">": {
			boolean f = false;
			for (int i = page; i <= numOfPages(table) && f == false; i++) {
				temp = readFile(path + table + i + ".csv");
				for (int j = 0; j < temp.size(); j++) {
					if (compareValues(type, temp.get(j).split(",")[col], value + "") > 0)
						res.add(temp.get(j));
				}
			}
		}
		;
		break;
		case ">=": {
			boolean f = false;
			for (int i = page; i <= numOfPages(table) && f == false; i++) {
				temp = readFile(path + table + i + ".csv");
				for (int j = 0; j < temp.size(); j++) {
					if (compareValues(type, temp.get(j).split(",")[col], value + "") >= 0)
						res.add(temp.get(j));
				}
			}
		}
		;
		break;
		case "!=": {
			for (int i = 1; i <= numOfPages(table); i++) {
				temp = readFile(path + table + i + ".csv");
				for (int j = 0; j < temp.size(); j++) {
					if (!temp.get(i).split(",")[col].equals(value + ""))
						res.add(temp.get(j));

				}
			}
		}
		;
		break;
		default:
			throw new DBAppException("invalid operator");
		}
		return res;
	}

	public static ArrayList<String> select1(String column, String table, Object value, String op)
			throws IOException, ParseException, DBAppException {

		String fileName = table + column;
		ArrayList<String> sparse = readFile(path + fileName + ".csv");
		String type = getType(table, column);
		int page = 0;
		ArrayList<String> res = new ArrayList<>();
		ArrayList<String> temp = new ArrayList<>();
		for (int i = 0; i < sparse.size(); i++) {
			if (compareValues(type, sparse.get(i).split(",")[0], value + "") < 0) {
				page = Integer.parseInt(sparse.get(i).split(",")[1]);
			} else
				break;
		}
		if (page == 0) {

		}
		switch (op) {
		case "=": {
			boolean f = false;
			for (int j = page; j <= pageNum(table,column) && f == false; j++) {
				temp = readFile(path + fileName + j + ".csv");
				for (int i = 0; i < temp.size(); i++) {
					if (temp.get(i).split(",")[0].equals(value + "")) {
						res.add(temp.get(i));
						f = true;

					}
					if (compareValues(type, temp.get(i).split(",")[0], value + "") > 0) {
						f = true;
						break;
					}
				}
			}
		}
		;
		break;
		case "<": {
			for (int i = 1; i <= page; i++) {
				temp = readFile(path + fileName + i + ".csv");
				for (int j = 0; j < temp.size(); j++) {
					if (compareValues(type, temp.get(j).split(",")[0], value + "") < 0)
						res.add(temp.get(j));

				}
			}
		}
		;
		break;
		case "<=": {
			boolean f = false;
			for (int i = 1; i <= pageNum(table,column) && f == false; i++) {
				temp = readFile(path + fileName + i + ".csv");
				for (int j = 0; j < temp.size(); j++) {
					if (compareValues(type, temp.get(j).split(",")[0], value + "") <= 0)
						res.add(temp.get(j));
					else {
						f = true;
						break;
					}
				}
			}
		}
		;
		break;
		case ">": {
			boolean f = false;
			for (int i = page; i <= pageNum(table,column) && f == false; i++) {
				temp = readFile(path + fileName + i + ".csv");
				for (int j = 0; j < temp.size(); j++) {
					if (compareValues(type, temp.get(j).split(",")[0], value + "") > 0)
						res.add(temp.get(j));

				}
			}
		}
		;
		break;
		case ">=": {
			boolean f = false;
			for (int i = page; i <= pageNum(table,column) && f == false; i++) {
				temp = readFile(path + fileName + i + ".csv");
				for (int j = 0; j < temp.size(); j++) {
					if (compareValues(type, temp.get(j).split(",")[0], value + "") >= 0)
						res.add(temp.get(j));

				}
			}
		}
		;
		break;
		case "!=": {
			for (int i = 1; i <= pageNum(table,column); i++) {
				temp = readFile(path + fileName + i + ".csv");
				for (int j = 0; j < temp.size(); j++) {
					if (!temp.get(i).split(",")[0].equals(value + ""))
						res.add(temp.get(j));

				}
			}
		}
		;
		break;
		default:
			throw new DBAppException("invalid operator");
		}
		ArrayList<String> rows = new ArrayList<>();
		for (int i = 0; i < res.size(); i++) {
			String ref = res.get(i).split(",")[1];
			String p = ref.split("_")[0];
			String row = ref.split("_")[1];
			ArrayList<String> t = readFile(path + table + p + ".csv");
			for (int j = 0; j < t.size(); j++) {
				if (getClusterVal(t.get(j), table).equals(row)) {
					rows.add(t.get(j));
				}
			}
		}
		return rows;
	}

	public static ArrayList<String> seqSearch(String column, String table, Object value, String op)
			throws IOException, ParseException, DBAppException {
		ArrayList<String> t = read(table);
		ArrayList<String> res = new ArrayList<>();
		int col = getColIndex(table, column);
		String type = getType(table, column);
		switch (op) {
		case "=":
			for (int i = 0; i < t.size(); i++) {
				if (t.get(i).split(",")[col].equals(value + "")) {
					res.add(t.get(i));
				}
			}
			;
			break;
		case "<": {
			for (int i = 0; i < t.size(); i++) {
				if (compareValues(type, t.get(i).split(",")[col], value + "") < 0) {
					res.add(t.get(i));
				}
			}
		}
		;
		break;
		case "<=": {
			for (int i = 0; i < t.size(); i++) {
				if (compareValues(type, t.get(i).split(",")[col], value + "") < 0) {
					res.add(t.get(i));
				}
			}
		}
		;
		break;

		case ">": {
			for (int i = 0; i < t.size(); i++) {
				if (compareValues(type, t.get(i).split(",")[col], value + "") > 0) {
					res.add(t.get(i));
				}
			}
		}
		;
		break;

		case ">=": {
			for (int i = 0; i < t.size(); i++) {
				if (compareValues(type, t.get(i).split(",")[col], value + "") >= 0) {
					res.add(t.get(i));
				}
			}
		}
		;
		break;

		case "!=": {
			for (int i = 0; i < t.size(); i++) {
				if (!t.get(i).split(",")[col].equals(value + "")) {
					res.add(t.get(i));
				}
			}
		}
		;
		break;
		default:
			throw new DBAppException("invalid operator");
		}
		return res;
	}

	public static ArrayList<String> andOp(ArrayList<String> temp, ArrayList<String> res) {
		ArrayList<String> arr = new ArrayList<>();
		for (int j = 0; j < temp.size(); j++) {
			if (res.contains(temp.get(j))) {
				arr.add(temp.get(j));
			}
		}
		return arr;
	}

	public static ArrayList<String> readFile(String fileName) throws IOException {
		ArrayList<String> res = new ArrayList<>();
		FileReader fileReader = new FileReader(fileName);
		BufferedReader reader = new BufferedReader(fileReader);
		String strCurrentLine = "";
		while ((strCurrentLine = reader.readLine()) != null) {
			res.add(strCurrentLine);
		}
		reader.close();
		fileReader.close();
		return res;
	}

	public static int compareValues(String type, String indexValue, String value)
			throws IOException, ParseException, DBAppException {
		if (type.equals("java.lang.Integer")) {
			if (Integer.parseInt(indexValue) == Integer.parseInt(value)) {
				return 0;
			} else if (Integer.parseInt(indexValue) < Integer.parseInt(value)) {
				return -1;
			} else
				return 1;
		}
		if (type.equals("java.lang.Double")) {
			if (Double.parseDouble(indexValue) == Double.parseDouble(value)) {
				return 0;
			} else if (Double.parseDouble(indexValue) < Double.parseDouble(value)) {
				return -1;
			} else
				return 1;
		}
		if (type.equals("java.lang.String")) {
			if (indexValue.compareTo(value) == 0) {
				return 0;
			} else if (indexValue.compareTo(value) < 0) {
				return -1;
			} else
				return 1;
		}
		if (type.equals("java.lang.Date")) {
			Date d1 = new SimpleDateFormat("dd/MM/yyyy").parse(indexValue);
			Date d2 = new SimpleDateFormat("dd/MM/yyyy").parse(value);
			if (d1.compareTo(d2) == 0) {
				return 0;
			} else if (d1.compareTo(d2) < 0) {
				return -1;
			} else
				return 1;
		}
		throw new DBAppException("invalid types");
	}

	public static int oper(String operator, String column, Object Value, String tableValue)
			throws ParseException, DBAppException {
		if (operator.equals("=")) {
			if (Value.getClass().getName().equals("java.lang.Integer"))
				if (Integer.parseInt(tableValue) == (Integer) Value) {
					return 1;
				} else
					return 0;
			else if (Value.getClass().getName().equals("java.lang.Double"))
				if (Double.parseDouble(tableValue) == (Double) Value) {
					return 1;
				} else
					return 0;
			else if (Value.getClass().getName().equals("java.lang.String"))
				if ((tableValue).equals(Value)) {
					return 1;
				} else
					return 0;
			else if (Value.getClass().getName().equals("java.lang.Date")) {
				Date d1 = new SimpleDateFormat("dd/MM/yyyy").parse(Value + "");
				Date d2 = new SimpleDateFormat("dd/MM/yyyy").parse(tableValue);
				if ((d1).compareTo(d2) == 0) {
					return 1;
				} else
					return 0;
			}
		} else if (operator.equals("!=")) {
			if (Value.getClass().getName().equals("java.lang.Integer"))
				if (Integer.parseInt(tableValue) != (Integer) Value) {
					return 1;
				} else
					return 0;
			else if (Value.getClass().getName().equals("java.lang.Double"))
				if (Double.parseDouble(tableValue) != (Double) Value) {
					return 1;
				} else
					return 0;
			else if (Value.getClass().getName().equals("java.lang.String"))
				if (!(tableValue).equals(Value)) {
					return 1;
				} else
					return 0;
			else if (Value.getClass().getName().equals("java.lang.Date")) {
				Date d1 = new SimpleDateFormat("dd/MM/yyyy").parse(Value + "");
				Date d2 = new SimpleDateFormat("dd/MM/yyyy").parse(tableValue);
				if ((d1).compareTo(d2) != 0) {
					return 1;
				} else
					return 0;
			}
		} else if (operator.equals(">")) {
			if (Value.getClass().getName().equals("java.lang.Integer"))
				if (Integer.parseInt(tableValue) > (Integer) Value) {
					return 1;
				} else
					return 0;
			else if (Value.getClass().getName().equals("java.lang.Double"))
				if (Double.parseDouble(tableValue) > (Double) Value) {
					return 1;
				} else
					return 0;
			else if (Value.getClass().getName().equals("java.lang.String"))
				if ((tableValue).compareTo(Value + "") > 0) {
					return 1;
				} else
					return 0;
			else if (Value.getClass().getName().equals("java.lang.Date")) {
				Date d1 = new SimpleDateFormat("dd/MM/yyyy").parse(tableValue);
				Date d2 = new SimpleDateFormat("dd/MM/yyyy").parse(Value + "");
				if ((d1).compareTo(d2) > 0) {
					return 1;
				} else
					return 0;
			}
		} else if (operator.equals(">=")) {
			if (Value.getClass().getName().equals("java.lang.Integer")) {
				if (Integer.parseInt(tableValue) >= (Integer) Value) {
					return 1;
				} 
				else return 0;
			}else if (Value.getClass().getName().equals("java.lang.Double")) {
					if (Double.parseDouble(tableValue) >= (Double) Value) {
						return 1;
					} 
					else return 0;
				}else if (Value.getClass().getName().equals("java.lang.String")) {
						if ((tableValue).compareTo(Value + "") >= 0) {
							return 1;
						} 
						else return 0;	
				}else if (Value.getClass().getName().equals("java.lang.Date")) {
							Date d1 = new SimpleDateFormat("dd/MM/yyyy").parse(tableValue);
							Date d2 = new SimpleDateFormat("dd/MM/yyyy").parse(Value + "");
							if ((d1).compareTo(d2) >= 0) {
								return 1;
							}
							else return 0;
						}
		} else if (operator.equals("<")) {
			if (Value.getClass().getName().equals("java.lang.Integer")) {
				if (Integer.parseInt(tableValue) < (Integer) Value) {
					return 1;
				}
				else return 0;	
			}else if (Value.getClass().getName().equals("java.lang.Double")) {
					if (Double.parseDouble(tableValue) < (Double) Value) {
						return 1;
					} 
					else return 0;	
				}else if (Value.getClass().getName().equals("java.lang.String")) {
						if ((tableValue).compareTo(Value + "") < 0) {
							return 1;
						} 
						else return 0;	
					}else if (Value.getClass().getName().equals("java.lang.Date")) {
							Date d1 = new SimpleDateFormat("dd/MM/yyyy").parse(tableValue);
							Date d2 = new SimpleDateFormat("dd/MM/yyyy").parse(Value + "");
							if ((d1).compareTo(d2) < 0) {
								return 1;
							} else
								return 0;
						}
		} else if (operator.equals("<=")) {
			if (Value.getClass().getName().equals("java.lang.Integer"))
				if (Integer.parseInt(tableValue) <= (Integer) Value) {
					return 1;
				} else
					return 0;
			else if (Value.getClass().getName().equals("java.lang.Double"))
				if (Double.parseDouble(tableValue) <= (Double) Value) {
					return 1;
				} else
					return 0;
			else if (Value.getClass().getName().equals("java.lang.String"))
				if ((tableValue).compareTo(Value + "") <= 0) {
					return 1;
				} else
					return 0;
			else if (Value.getClass().getName().equals("java.lang.Date")) {
				Date d1 = new SimpleDateFormat("dd/MM/yyyy").parse(tableValue);
				Date d2 = new SimpleDateFormat("dd/MM/yyyy").parse(Value + "");
				if ((d1).compareTo(d2) <= 0) {
					return 1;
				} else
					return 0;
			}
		}
		throw new DBAppException("invalid operator");
	}
	public static int pageNum(String table,String column) throws IOException {
		ArrayList<String> csvRead = readPage(path + "Files.csv");
		int count = 0;
		for (int i = 0; i < csvRead.size(); i++) {
			String[] s = csvRead.get(i).split(",");
			if (s[0].compareTo(table+"_"+column)==0&& s[1].equals("DenseIndex"))
				count++;
		}
		return count;
	}
	

	public static void main(String[] args) throws IOException, DBAppException, ParseException {
		String strTableName = "Student";
		DBApp dbApp = new DBApp();
		Hashtable htblColNameType = new Hashtable();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		Hashtable htblColNameMin = new Hashtable();
		htblColNameMin.put("id", "1");
		htblColNameMin.put("name", "A");
		htblColNameMin.put("gpa", "0.0");
		Hashtable htblColNameMax = new Hashtable();
		htblColNameMax.put("id", "5674568");
		htblColNameMax.put("name", "zzzzzzzzzzzzzzzzzzzzzzzzzz");
		htblColNameMax.put("gpa", "9999999999999.99999999999");
		dbApp.createTable(strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax);
		dbApp.createIndex(strTableName, "id");
		dbApp.createIndex(strTableName, "gpa");
		Hashtable htblColNameValue = new Hashtable();
		htblColNameValue.put("id", new Integer(2343432));
		htblColNameValue.put("name", new String("Ahmed Noor"));
		htblColNameValue.put("gpa", new Double(0.95));
		dbApp.insertIntoTable(strTableName, htblColNameValue);
		htblColNameValue.clear();
		htblColNameValue.put("id", new Integer(453455));
		htblColNameValue.put("name", new String("Ahmed Noor"));
		htblColNameValue.put("gpa", new Double(0.95));
		dbApp.insertIntoTable(strTableName, htblColNameValue);
		htblColNameValue.clear();
		htblColNameValue.put("id", new Integer(5674567));
		htblColNameValue.put("name", new String("Dalia Noor"));
		htblColNameValue.put("gpa", new Double(1.5));
		dbApp.insertIntoTable(strTableName, htblColNameValue);
		htblColNameValue.clear();
		htblColNameValue.put("id", new Integer(23498));
		htblColNameValue.put("name", new String("John Noor"));
		htblColNameValue.put("gpa", new Double(1.5));
		dbApp.insertIntoTable(strTableName, htblColNameValue);
		htblColNameValue.clear();
		htblColNameValue.put("id", new Integer(78452));
		htblColNameValue.put("name", new String("Zaky Noor"));
		htblColNameValue.put("gpa", new Double(0.88));
	//	dbApp.insertIntoTable(strTableName, htblColNameValue);
//		htblColNameValue.clear();
//		htblColNameValue.put("name", new String("Zaky Noor"));
//	*/	dbApp.deleteFromTable(strTableName, htblColNameValue);
		 SQLTerm[] arrSQLTerms;
		 arrSQLTerms = new SQLTerm[2];
		 arrSQLTerms[0] = new SQLTerm();
		 arrSQLTerms[0]._strTableName = "Student";
		 arrSQLTerms[0]._strColumnName = "gpa";
		 arrSQLTerms[0]._strOperator = "=";
		 arrSQLTerms[0]._objValue = new Double(1.5);
		 arrSQLTerms[1] = new SQLTerm();
		 arrSQLTerms[1]._strTableName = "Student";
		 arrSQLTerms[1]._strColumnName = "id";
		 arrSQLTerms[1]._strOperator = "=";
		 arrSQLTerms[1]._objValue = new Integer(23498);
		 String[] strarrOperators = new String[1];
		 strarrOperators[0] = "OR";
		 Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
		 while (resultSet.hasNext()) {
		 System.out.println(resultSet.next());
		 }
	}
}
