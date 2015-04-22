package eu.wilkolek.pardi.util.ignite;

import java.io.File;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;

import com.rapidminer.operator.IOObject;

import eu.wilkolek.pardi.util.Helper;

public class IgniteJobManagerHelper {
	public static IgniteCache<String, IOObject> DATACache;
	public static IgniteCache<String, IOObject> RESULTCache;
	public static Ignite ignite;
	public static String DATA = "cache";
	public static String RESULT = "result";

	private static File cfgFile = new File(
			"D:/gitlab/rapidminer-node/ignite-fabric/config/default-config.xml");

	public static File getCfgFile() {
		return cfgFile;
	}

	public static void setCfgFile(File cfgFile) {
		IgniteJobManagerHelper.cfgFile = cfgFile;
	}

	public static ClassLoader loader;
	public static IgniteCache<Object, Object> igniteCacheResult;
	public static IgniteCache<Object, Object> igniteCacheData;

	public static void prepareIgniteJobManagerForRemotes(Ignite i) {
		if (i != null) {
			IgniteJobManagerHelper.ignite = i;
			IgniteJobManagerHelper.DATACache = i.jcache(DATA);
			IgniteJobManagerHelper.RESULTCache = i.jcache(RESULT);
		}
	}

	public static String storeData(Object key, IOObject obj) {
		String k;
		if (key == null) {
			k = IgniteJobManagerHelper.generateDataKey("d", "it");
		} else {
			k = (String) key;
		}
		IgniteJobManagerHelper.DATACache.put(k, obj);
		Helper.out("Stored in cache CACHE");
		return k;
	}

	public static String generateResultKey(String a, String z) {
		String key = proposeKey(a, z);

		while (RESULTCache.containsKey(key)) {
			key = proposeKey(a, z);
		}
		return key;
	}

	public static String generateDataKey(String a, String z) {
		String key = proposeKey(a, z);

		while (DATACache.containsKey(key)) {
			key = proposeKey(a, z);
		}
		return key;
	}

	private static String proposeKey(String a, String z) {
		Long time = System.nanoTime();
		Long rnd = Math.round(Math.random() * 1000);

		return a + time.toString() + rnd.toString() + z;
	}

	public void setIgnite(Ignite ignite) {
		IgniteJobManagerHelper.ignite = ignite;
		IgniteJobManagerHelper.igniteCacheResult = ignite.jcache(RESULT);
		IgniteJobManagerHelper.igniteCacheData = ignite.jcache(DATA);
	}

	public static String storeResult(Object key, IOObject obj) {
		String k;
		if (key == null) {
			k = IgniteJobManagerHelper.generateDataKey("d", "it");
		} else {
			k = (String) key;
		}
		IgniteJobManagerHelper.RESULTCache.put(k, obj);
		Helper.out("Stored in cache RESULT");
		return k;
	}
}
