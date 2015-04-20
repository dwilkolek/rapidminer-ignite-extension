package eu.wilkolek.pardi.util.ignite;

import java.io.File;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;

import com.rapidminer.operator.IOObject;

public class IgniteJobManagerHelper {
	public static IgniteCache<String,IOObject> DATACache;
	public static IgniteCache<String,IOObject> RESULTCache;
	public static Ignite ignite;
	public static String DATA = "cache";
	public static String RESULT = "result";
	
	private static File cfgFile = new File(
			"C:/dev/mgr/node/ignite-fabric/config/default-config.xml");

	public static File getCfgFile() {
		return cfgFile;
	}

	public static void setCfgFile(File cfgFile) {
		IgniteJobManagerHelper.cfgFile = cfgFile;
	}
	public static ClassLoader loader;
}
