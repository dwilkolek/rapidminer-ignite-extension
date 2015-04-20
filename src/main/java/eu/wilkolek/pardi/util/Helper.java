package eu.wilkolek.pardi.util;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Date;

import eu.wilkolek.pardi.types.rapidminer.AbstractJobManager;

public class Helper {
	
	public static AbstractJobManager masterOperator = null;

	public static Boolean lastPerSubprocess = false;
	public static Boolean lastPerNode = false;
	public static boolean persistentData = false;
	public static boolean flushCache = false;
	public static Boolean preloaded = false;
	
	public static void saveToFile(String string, String string2) {
		// DEBUG method
		if (Config.DEBUG) {
			Date d = new Date();
			File f = new File("C:/dev/procesy/" + string + "_" + d.getTime()
					+ ".xml");
			Helper.out("File saved at: " + f.getAbsolutePath());
			FileOutputStream fos;
			try {
				fos = new FileOutputStream(f);
				fos.write(string2.getBytes());
				fos.flush();
				fos.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}
	public static void out(String x) {
		if (Config.DEBUG) {
			System.out.println(x);
		}

	}
	
}
