package eu.wilkolek.rm.util;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.GridCache;

import com.rapidminer.example.ExampleSet;
import com.rapidminer.operator.IOObject;

public class CacheTools {

	public static void uploadCache(GridCache<Integer, IOObject> cache,
			ExampleSet set, Integer i) {
		try {
			Sequences seq = new Sequences();
			cache.put(i, set);
		} catch (IgniteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (IgniteCheckedException e) {
			e.printStackTrace();
		}
	}

	public static IOObject downloadCache(IgniteCache<Integer, IOObject> result, Integer i) {

		try {
			return result.get(i);
		} catch (IgniteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
				
		return null;
	}

	public CacheTools() {
		// TODO Auto-generated constructor stub
	}

}
