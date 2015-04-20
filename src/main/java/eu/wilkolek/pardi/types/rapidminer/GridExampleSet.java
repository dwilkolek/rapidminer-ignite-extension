/**
 * 
 */
package eu.wilkolek.pardi.types.rapidminer;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.ignite.IgniteCache;

import com.rapidminer.example.Attributes;
import com.rapidminer.example.Example;
import com.rapidminer.example.set.AbstractExampleSet;
import com.rapidminer.example.table.ExampleTable;
import com.rapidminer.example.Attribute;;
/**
 * @author Damian
 *
 */
public class GridExampleSet extends AbstractExampleSet {

	IgniteCache<Integer, Example> cache;
	ArrayList<Attribute> attributes = new ArrayList<>();
	/**
	 * 
	 */
	public GridExampleSet() {
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see com.rapidminer.example.ExampleSet#getAttributes()
	 */
	@Override
	public Attributes getAttributes() {
		
		return null;
	}

	/* (non-Javadoc)
	 * @see com.rapidminer.example.ExampleSet#size()
	 */
	@Override
	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}

	/* (non-Javadoc)
	 * @see com.rapidminer.example.ExampleSet#getExampleTable()
	 */
	@Override
	public ExampleTable getExampleTable() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.rapidminer.example.ExampleSet#getExample(int)
	 */
	@Override
	public Example getExample(int index) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<Example> iterator() {
		// TODO Auto-generated method stub
		return null;
	}

}
