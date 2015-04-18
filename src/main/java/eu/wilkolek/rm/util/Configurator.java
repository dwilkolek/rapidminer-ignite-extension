package eu.wilkolek.rm.util;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;

import com.rapidminer.operator.IOObject;
import com.rapidminer.operator.IOObjectCollection;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.PortPairExtender;
import com.rapidminer.operator.ports.PortPairExtender.PortPair;

import eu.wilkolek.types.gridgain.GridRmTask;
import eu.wilkolek.types.rapidminer.AbstractTaskManager;

public class Configurator {
	
	public static AbstractTaskManager masterOperator = null;
	
	public static Boolean lastPerSubprocess = false;
	public static Boolean lastPerNode = false;
	public static boolean persistentData = false;
	public static boolean flushCache = false;
	public static Boolean preloaded = false;
	public static Boolean delayedExecution = false;
	
	public static Boolean DEBUG = true;
	public static Integer tc = 2;
	public static String PROLOG = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?> \n <process version=\"5.3.015\"> \n   <context> \n     <input/> \n     <output/> \n     <macros/> \n   </context> \n   <operator activated=\"true\" class=\"process\" compatibility=\"5.3.015\" expanded=\"true\" name=\"Process\"> \n     <parameter key=\"logverbosity\" value=\"init\"/> \n     <parameter key=\"random_seed\" value=\"2001\"/> \n     <parameter key=\"send_mail\" value=\"never\"/> \n     <parameter key=\"notification_email\" value=\"\"/> \n     <parameter key=\"process_duration_for_mail\" value=\"30\"/> \n     <parameter key=\"encoding\" value=\"SYSTEM\"/> \n     <process expanded=\"true\"> \n ";
	public static String EPILOG = "<portSpacing port=\"source_input 1\" spacing=\"0\"/> \n       <portSpacing port=\"sink_result 1\" spacing=\"0\"/> \n     </process> \n   </operator> \n </process> \n ";
	public static Ignite ignite;
	
	public static ArrayList<GridRmTask> delayedTasks = new ArrayList<GridRmTask>();
//	private static ArrayList<String> jarList = new ArrayList<String>() {
//		{
//			add("C:\\dev\\gridgain-fabric\\gridgain-core-6.5.7.jar");
//			add("C:\\dev\\gridgain-fabric\\gridgain-spring\\gridgain-spring-6.5.7.jar");
//			add("C:\\dev\\gridgain-fabric\\gridgain-spring\\spring-aop-4.1.0.RELEASE.jar");
//			add("C:\\dev\\gridgain-fabric\\gridgain-spring\\spring-beans-4.1.0.RELEASE.jar");
//			add("C:\\dev\\gridgain-fabric\\gridgain-spring\\spring-context-4.1.0.RELEASE.jar");
//			add("C:\\dev\\gridgain-fabric\\gridgain-spring\\spring-core-4.1.0.RELEASE.jar");
//			add("C:\\dev\\gridgain-fabric\\gridgain-spring\\spring-expression-4.1.0.RELEASE.jar");
//			add("C:\\dev\\gridgain-fabric\\gridgain-spring\\spring-tx-4.1.0.RELEASE.jar");
//		}
//	};
	
	private static String jarString;
	
	public static ClassLoader loader;
	private static File cfgFile = new File(
			"C:/dev/mgr/node/ignite-fabric/config/default-config.xml");

	public static ArrayList<HashSet<String>> keySetsToRemove = new ArrayList<HashSet<String>>();;


//	public static ArrayList<String> getJarList() {
//		return jarList;
//	}
//
//	public static void setJarList(ArrayList<String> jarList) {
//		Configurator.jarList = jarList;
//	}

//	public static String getJarString() {
//		if (Configurator.jarString == null) {
//			Configurator.jarString = "";
//			for (String jar : Configurator.jarList) {
//				jarString += ";" + jar;
//			}
//		}
//		return jarString;
//	}

	public static void setJarString(String jarString) {
		Configurator.jarString = jarString;
	}

	public static File getCfgFile() {
		return cfgFile;
	}

	public static void setCfgFile(File cfgFile) {
		Configurator.cfgFile = cfgFile;
	}
	public static void out(String x) {
		if (Configurator.DEBUG) {
			System.out.println(x);
		}
		
	}
	
	public static void zapisDoPliku(String string, String string2) {
		// DEBUG method
		Date d = new Date();
		File f = new File("C:/dev/procesy/" + string + "_" + d.getTime()
				+ ".xml");
		Configurator.out("File saved at: "+f.getAbsolutePath());
		FileOutputStream fos;
		try {
			fos = new FileOutputStream(f);
			fos.write(string2.getBytes());
			fos.flush();
			fos.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		;

	}

	public static void addJobList(ArrayList<GridRmTask> tasks) {
		Configurator.delayedTasks.addAll(tasks);		
	}
	
	public static HashMap<Integer, HashMap<Integer, IOObject>> processResponse(
			List<Future<String>> resultKeys, IgniteCache<String, IOObject> result) {

		// ArrayList<ExampleSet> sets = new ArrayList<ExampleSet>();
		Iterator<Future<String>> resultKeysIterator = resultKeys.iterator();
		HashMap<Integer, HashMap<Integer, IOObject>> outputSets = new HashMap<Integer, HashMap<Integer, IOObject>>();

		while (resultKeysIterator.hasNext()) {
			try {
				String code = resultKeysIterator.next().get();
				String[] cacheKeys = code.split(";");
				for (String cacheKey : cacheKeys) {
					// 0 - TaskID, 1- iteration, 2 - nodeId, 3 - outputPortNumber
					String[] values = cacheKey.split("_");
					Integer nodeId = Integer.parseInt(values[2]);
					Integer outputPortNumber = Integer.parseInt(values[3]);

					if (!outputSets.containsKey(outputPortNumber)) {
						outputSets.put(outputPortNumber,
								new HashMap<Integer, IOObject>());
					}
					outputSets.get(outputPortNumber).put(nodeId,
							result.get(cacheKey));

				}

			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return outputSets;
	}

	public static void toOutput(
			HashMap<Integer, HashMap<Integer, IOObject>> outputSet,
			PortPairExtender outputExtender) {
		Iterator<PortPair> iterator = outputExtender.getManagedPairs()
				.iterator();
		Configurator.out("managed pairs size: "
				+ outputExtender.getManagedPairs().size());
		int portNo = 0;
		while (iterator.hasNext()) {
			portNo++;

			OutputPort outputPort = iterator.next().getOutputPort();

			if (outputPort.isConnected() && outputSet.get(portNo) != null) {
				ArrayList<IOObject> iOObjectList = new ArrayList<IOObject>(
						outputSet.get(portNo).values());
				IOObjectCollection<IOObject> ioc = new IOObjectCollection<IOObject>(
						iOObjectList);
				outputPort.deliver(ioc);
			}

		}
		
	}

}
