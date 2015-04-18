package eu.wilkolek.types.rapidminer;

import java.util.Iterator;

import javax.cache.Cache.Entry;

import com.rapidminer.datatable.DataTableRow;

public class GridIterator implements Iterator<DataTableRow> {
	Iterator<Entry<Integer, DataTableRow>> iterator;
	
	public GridIterator(Iterator<Entry<Integer, DataTableRow>> it) {
		iterator =it;
	}
	
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return iterator.hasNext();
	}

	@Override
	public DataTableRow next() {
		// TODO Auto-generated method stub
		return iterator.next().getValue();
	}

	@Override
	public void remove() {
		iterator.remove();

	}

}
