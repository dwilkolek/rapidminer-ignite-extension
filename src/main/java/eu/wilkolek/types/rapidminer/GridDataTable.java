package eu.wilkolek.types.rapidminer;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.ignite.IgniteCache;

import com.rapidminer.datatable.AbstractDataTable;
import com.rapidminer.datatable.DataTable;
import com.rapidminer.datatable.DataTableListener;
import com.rapidminer.datatable.DataTableRow;
import com.rapidminer.gui.plotter.charts.AbstractChartPanel.Selection;
import com.rapidminer.tools.Tools;

public class GridDataTable extends AbstractDataTable implements Serializable {
	private IgniteCache<Integer,DataTableRow> cache;

	private String[] columns;
	
    private double[] weights;
    
	private boolean[] specialColumns;
	
	private Map<Integer, Map<Integer,String>> index2StringMap = new HashMap<Integer,Map<Integer,String>>();
	private Map<Integer, Map<String,Integer>> string2IndexMap = new HashMap<Integer,Map<String,Integer>>();

	private int[] currentIndices;

    public GridDataTable(IgniteCache cache, String name, String[] columns) {
        this(cache, name, columns, null);
    }
    
    Integer maxKey = 0;
    
	public GridDataTable(IgniteCache cache, String name, String[] columns, double[] weights) {
	    super(name);
		this.columns = columns;
        this.weights = weights;
		this.specialColumns = new boolean[columns.length];
		for (int i = 0; i < this.specialColumns.length; i++)
			this.specialColumns[i] = false;
		this.currentIndices = new int[columns.length];
		for (int i = 0; i < currentIndices.length; i++) {
			currentIndices[i] = 0;
		}
		this.cache = cache;
	}
	
	private GridDataTable(GridDataTable GridDataTable) {
		super(GridDataTable.getName());
		
		this.columns = null;
		if (GridDataTable.columns != null) {
			this.columns = new String[GridDataTable.columns.length];
			for (int i = 0; i < GridDataTable.columns.length; i++) {
				this.columns[i] = GridDataTable.columns[i];
			}
		}
		
		this.weights = null;
		if (GridDataTable.weights != null) {
			this.weights = new double[GridDataTable.weights.length];
			for (int i = 0; i < GridDataTable.weights.length; i++) {
				this.weights[i] = GridDataTable.weights[i];
			}
		}
		
		this.specialColumns = null;
		if (GridDataTable.specialColumns != null) {
			this.specialColumns = new boolean[GridDataTable.specialColumns.length];
			for (int i = 0; i < GridDataTable.specialColumns.length; i++) {
				this.specialColumns[i] = GridDataTable.specialColumns[i];
			}
		}
		
		this.currentIndices = new int[GridDataTable.currentIndices.length];
		for (int i = 0; i < this.currentIndices.length; i++)
			this.currentIndices[i] = GridDataTable.currentIndices[i];
		
		this.index2StringMap = new HashMap<Integer,Map<Integer,String>>();
		for (Map.Entry<Integer, Map<Integer,String>> entry : GridDataTable.index2StringMap.entrySet()) {
			Integer key = entry.getKey();
			Map<Integer, String> indexMap = entry.getValue();
			Map<Integer, String> newIndexMap = new HashMap<Integer, String>();
			for (Map.Entry<Integer, String> innerEntry : indexMap.entrySet()) {
				newIndexMap.put(innerEntry.getKey(), innerEntry.getValue());
			}
			this.index2StringMap.put(key, newIndexMap);
		}
		
		this.string2IndexMap = new HashMap<Integer,Map<String,Integer>>();
		for (Map.Entry<Integer, Map<String,Integer>> entry : GridDataTable.string2IndexMap.entrySet()) {
			Integer key = entry.getKey();
			Map<String, Integer> indexMap = entry.getValue();
			Map<String, Integer> newIndexMap = new HashMap<String, Integer>();
			for (Map.Entry<String, Integer> innerEntry : indexMap.entrySet()) {
				newIndexMap.put(innerEntry.getKey(), innerEntry.getValue());
			}
			this.string2IndexMap.put(key, newIndexMap);
		}
	}
	
	public int getNumberOfSpecialColumns() {
		int counter = 0;
		for (boolean b : specialColumns)
			if (b) counter++;
		return counter;
	}
	
	public boolean isSpecial(int index) {
		return specialColumns[index];
	}
	
	public void setSpecial(int index, boolean special) {
		this.specialColumns[index] = special;
	}
	
	public boolean isNominal(int column) {
		return (index2StringMap.get(column) != null);
	}
	
	public boolean isDate(int index) {
		return false;
	}
	
	public boolean isTime(int index) {
		return false;
	}
	
	public boolean isDateTime(int index) {
		return false;
	}
	
	public boolean isNumerical(int index) {
		return !isNominal(index);
	}
	
	public String mapIndex(int column, int index) {
		Map<Integer,String> columnIndexMap = index2StringMap.get(column);
		return columnIndexMap.get(index);
	}
	
	public int mapString(int column, String value) {
		Map<String,Integer> columnValueMap = string2IndexMap.get(column);
		if (columnValueMap == null) {
			columnValueMap = new HashMap<String,Integer>();
			columnValueMap.put(value, currentIndices[column]);
			string2IndexMap.put(column, columnValueMap);
			Map<Integer,String> columnIndexMap = new HashMap<Integer,String>();
			columnIndexMap.put(currentIndices[column], value);
			index2StringMap.put(column, columnIndexMap);
			int returnValue = currentIndices[column];
			currentIndices[column]++;
			return returnValue;
		} else {
			Integer result = columnValueMap.get(value);
			if (result != null) {
				return result.intValue();
			} else {
				int newIndex = currentIndices[column];
				columnValueMap.put(value, newIndex);
				Map<Integer,String> columnIndexMap = index2StringMap.get(column);
				columnIndexMap.put(newIndex, value);
				currentIndices[column]++;
				return newIndex;
			}
		}
	}
	
	public int getNumberOfValues(int column) {
		return index2StringMap.get(column).size();
	}
	
	public void cleanMappingTables() {		
		Map<Integer, Set<String>> allValues = new HashMap<Integer, Set<String>>();
		for (Map.Entry<Integer, Map<String,Integer>> entry : this.string2IndexMap.entrySet()) {
			Integer key = entry.getKey();
			Set<String> columnValues = new HashSet<String>();
			for (String current : entry.getValue().keySet()) {
				columnValues.add(current);
			}
			allValues.put(key, columnValues);
		}
		
		for (DataTableRow row : this) {
			for (int i = 0; i < getNumberOfColumns(); i++) {
				if (isNominal(i)) {
					String currentValue = getValueAsString(row, i);
					allValues.get(i).remove(currentValue);
				}
			}
		}
		
		for (int i = 0; i < getNumberOfColumns(); i++) {
			Set<String> toDelete = allValues.get(i);
			if (toDelete != null) {
				Map<String, Integer> string2Index = this.string2IndexMap.get(i);
				Map<Integer, String> index2String = this.index2StringMap.get(i);
				for (String current : toDelete) {
					int oldIndex = string2Index.get(current);
					index2String.remove(oldIndex);
					string2Index.remove(current);
				}	
			}
		}
	}
	
    public boolean isSupportingColumnWeights() {
        return weights != null;
    }
    
    public double getColumnWeight(int column) {
        if (weights == null)
            return Double.NaN;
        else
            return weights[column];
    }
    
	public String getColumnName(int i) {
		return columns[i];
	}

	public int getColumnIndex(String name) {
		for (int i = 0; i < columns.length; i++) {
			if (columns[i].equals(name))
				return i;
		}
		return -1;
	}

	public int getNumberOfColumns() {
		return columns.length;
	}

	@Override
	public String[] getColumnNames() {
		return columns;
	}
    
	public synchronized void add(DataTableRow row) {
		synchronized (cache) {
			cache.put(maxKey, row);
			maxKey++;
			fireEvent();
		}
	}

	public synchronized void remove(DataTableRow row) {
		synchronized (cache) {
			cache.remove(Integer.parseInt(row.getId()));
			fireEvent();
		}
	}
	
    public DataTableRow getRow(int index) {
        return cache.get(index);
    }
    
	public synchronized Iterator<DataTableRow> iterator() {
		Iterator<DataTableRow> i = null;
		synchronized (cache) {
			i = new GridIterator(cache.iterator());
		}
		return i;
	}

	public int getNumberOfRows() {
		int result = 0;
		synchronized (cache) {
			result = cache.size();
		}
		return result;
	}

	public void clear() {
		cache.clear();
		fireEvent();
	}
    
    public synchronized DataTable sample(int newSize) {
    	if (getNumberOfRows() <= newSize) {
    		return this;
    	} else {
    		GridDataTable result = new GridDataTable(this);
    		
    		// must be a usual random since otherwise plotting would change the rest of 
    		// the process during a breakpoint result viewing
    		Random random = new Random();
        
    		List<Integer> indices = new ArrayList<Integer>(getNumberOfRows());
    		for (int i = 0; i < getNumberOfRows(); i++)
    			indices.add(i);
    		
    		while (result.getNumberOfRows() < newSize) {
    			int index = random.nextInt(indices.size());
    			result.add(cache.get(indices.remove(index)));
    		}
    		
    		return result;
        }
    }
    
    /** Dumps the complete table into a string (complete data!). */
    @Override
	public String toString() {
        StringBuffer result = new StringBuffer();
        for (DataTableRow row : this) {
            for (int i = 0; i < getNumberOfColumns(); i++) {
                if (i != 0)
                    result.append(", ");
                result.append(row.getValue(i));
            }
            result.append(Tools.getLineSeparator());
        }
        return result.toString();
    }

}
