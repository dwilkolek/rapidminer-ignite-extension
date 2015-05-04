package eu.wilkolek.pardi.ignite;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;

import com.rapidminer.RapidMiner;
import com.rapidminer.operator.IOObject;
import com.rapidminer.operator.IOObjectCollection;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.CollectingPortPairExtender;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.InputPorts;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.PortPairExtender;
import com.rapidminer.operator.ports.PortPairExtender.PortPair;

import eu.wilkolek.pardi.types.AbstractJobManagerHelper;
import eu.wilkolek.pardi.types.RemoteJob;
import eu.wilkolek.pardi.util.Helper;
import eu.wilkolek.pardi.util.PluginClassLoader;

public class IgniteJobManagerHelper extends AbstractJobManagerHelper {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1454955049129358662L;
	public IgniteCache<String, IOObject> DATACache;
	public IgniteCache<String, IOObject> RESULTCache;

	private Boolean workingRemotly = false;

	public Ignite ignite;
	public String DATA = "cache";
	public String RESULT = "result";

	private File cfgFile;

	public File getCfgFile() {
		return cfgFile;
	}

	public void setCfgFile(File cfgFile) {
		this.cfgFile = cfgFile;
	}

	public ClassLoader loader;
	public IgniteCache<Object, Object> igniteCacheResult;
	public IgniteCache<Object, Object> igniteCacheData;

	public IgniteJobManagerHelper() {
		super();
		String defaultCfg = "D:/gitlab/rapidminer-node/ignite/config/default-config.xml";
		String cfgLocation = System.getProperty("ignite.configuration.file");
		if (cfgLocation != null) {
			if (!cfgLocation.isEmpty()) {

				defaultCfg = cfgLocation;
			}
		}
		Helper.out("Ignite configuration file: " + defaultCfg);;
		cfgFile = new File(defaultCfg);
	}

	public void prepareIgniteJobManagerForRemotes(Ignite i) {
		if (i != null) {
			this.ignite = i;
			this.DATACache = i.cache(DATA);
			this.RESULTCache = i.cache(RESULT);
		}
	}

	@Override
	public String storeData(Object key, IOObject obj) {
		String k;
		if (key == null) {
			k = this.generateDataKey("d", "it");
		} else {
			k = (String) key;
		}
		this.DATACache.put(k, obj);
		// Helper.out("Stored in cache CACHE");
		return k;
	}

	public String generateResultKey(String a, String z) {
		String key = proposeKey(a, z);

		while (RESULTCache.containsKey(key)) {
			key = proposeKey(a, z);
		}
		return key;
	}

	private String generateDataKey(String a, String z) {
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
		this.ignite = ignite;
		igniteCacheResult = ignite.cache(RESULT);
		igniteCacheData = ignite.cache(DATA);
	}

	@Override
	public String storeResult(Object key, IOObject obj) {
		String k;
		if (key == null) {
			k = this.generateDataKey("d", "it");
		} else {
			k = (String) key;
		}
		this.RESULTCache.put(k, obj);
		// Helper.out("Stored in cache RESULT");
		return k;
	}

	@Override
	public void asureInstanceIsReady() {

		if (this.ignite == null) {
			if (this.loader == null) {

				prepareClassLoader();
			}
			// Helper.out("Ignite == null? --> " + (ignite == null));
			try {

				this.ignite = Ignition.start(this.getCfgFile()
						.getAbsolutePath());

			} catch (Exception e) {
				this.ignite = Ignition.ignite();
			}

			Helper.out("Ignite start");

			this.DATACache = this.ignite.cache(this.DATA);
			this.RESULTCache = this.ignite.cache(this.RESULT);
		}

	}

	private void prepareClassLoader() {
		if (Thread.currentThread().getContextClassLoader() instanceof PluginClassLoader) {
			this.loader = Thread.currentThread().getContextClassLoader();
		} else {
			IgniteConfiguration cfg = new IgniteConfiguration();
			this.loader = cfg.getClass().getClassLoader();
			PluginClassLoader wcl = new PluginClassLoader(
					RapidMiner.class.getClassLoader(), cfg.getClass()
							.getClassLoader());

			try {
				this.loader = wcl;
				this.loader.loadClass(IgniteConfiguration.class
						.getCanonicalName());
				Thread.currentThread().setContextClassLoader(this.loader);
				Thread.currentThread()
						.getContextClassLoader()
						.loadClass(IgniteConfiguration.class.getCanonicalName());
			} catch (ClassNotFoundException e2) {
				Helper.out("loader don't load");
			}
		}
	}

	@Override
	public ExecutorService getExecutorService() {
//		ArrayList<ClusterNode> clusterNodes = new ArrayList<ClusterNode>();
//		UUID master = UUID.fromString(uuid);
//		for (ClusterNode cn : this.ignite.cluster().forRemotes().nodes()) {
//			// Helper.out("if ( "+ uuid + "==" + cn.id());
//			if (!cn.id().toString().equals(uuid)) {
//				clusterNodes.add(cn);
//			}
//			// else{
//			// Helper.out("Master node execluded");
//			// }
//		}
//		return ignite.executorService(this.ignite.cluster().forRemotes()
//				.forNodes(clusterNodes));
		IgniteCluster cluster = ignite.cluster();

		ClusterGroup workerGroup = cluster.forAttribute("ROLE", "worker");

		return ignite.executorService(workerGroup);
	}

	public int nodeCout() {
		return this.ignite.cluster().forRemotes().nodes().size();
	}

	@Override
	public void prepareForRemoteJob(Object instance) {
		//this.uuid = masterNodeId;
		// Helper.out("remote Job has uuid : "+masterNodeId);
		// Ignite itest = Ignition.ignite();
		// Helper.out("itest : " + (itest == null));
		workingRemotly = true;
		ignite = (Ignite) instance;
		DATACache = ignite.cache(DATA);
		RESULTCache = ignite.cache(RESULT);
		//uuid = masterNodeId;
	}

	@Override
	public IOObject retriveData(String cacheKey) {
		return DATACache.get(cacheKey);
	}

	@Override
	public int nodeCount() {
		return ignite.cluster().nodes().size();
	}

	@Override
	public void removeAllDataByKeys(HashSet<Object> keysToRemoveCache) {
		Iterator<Object> iterator = keysToRemoveCache.iterator();
		HashSet<String> keyList = new HashSet<String>();
		while (iterator.hasNext()) {
			keyList.add((String) iterator.next());
		}
		DATACache.removeAll(keyList);
	}

	@Override
	public void removeAllResults() {
		RESULTCache.removeAll();

	}

	@Override
	public IOObject retriveResult(Object key) {
		return RESULTCache.get((String) key);
	}

	@Override
	public IOObject retriveData(Object key) {
		return DATACache.get((String) key);
	}

	//
	// @Override
	public HashMap<Integer, HashMap<Integer, IOObject>> processResponse(
			List<Future<Object>> resultKeys) throws OperatorException {

		Iterator<Future<Object>> resultKeysIterator = resultKeys.iterator();
		HashMap<Integer, HashMap<Integer, IOObject>> outputSets = new HashMap<Integer, HashMap<Integer, IOObject>>();

		while (resultKeysIterator.hasNext()) {
			try {
				String code = (String) resultKeysIterator.next().get();
				if (!code.isEmpty()) {
					String[] cacheKeys = code.split(";");
					for (String cacheKey : cacheKeys) {
						// 0 - JobID, 1- iteration, 2 - nodeId, 3 -
						// outputPortNumber
						String[] values = cacheKey.split("_");
						Integer nodeId = Integer.parseInt(values[2]);
						Integer outputPortNumber = Integer.parseInt(values[3]);

						if (!outputSets.containsKey(outputPortNumber)) {
							outputSets.put(outputPortNumber,
									new HashMap<Integer, IOObject>());
						}
						outputSets.get(outputPortNumber).put(nodeId,
								retriveResult(cacheKey));
					}
				}

			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new OperatorException(
						"IgniteJobManagerHelper.processResponse()[InterruptedException]",
						e);
			} catch (ExecutionException e) {
				e.printStackTrace();
				throw new OperatorException(
						"IgniteJobManagerHelper.processResponse()[ExecutionException]",
						e);
			}
		}
		return outputSets;
	}

	//
	// @Override
	// public ArrayList<HashMap<Integer, IOObject>> processResponseToArray(
	// List<Future<Object>> resultKeys) throws OperatorException {
	// Iterator<Future<Object>> resultKeysIterator = resultKeys.iterator();
	// ArrayList<HashMap<Integer, IOObject>> outputSets = new
	// ArrayList<HashMap<Integer, IOObject>>();
	// Integer i = 0;
	// while (resultKeysIterator.hasNext()) {
	// try {
	// String code = (String) resultKeysIterator.next().get();
	// if (!code.isEmpty()) {
	// String[] cacheKeys = code.split(";");
	// for (String cacheKey : cacheKeys) {
	// // 0 - JobID, 1- iteration , 2 - outputPortNumber
	// String[] values = cacheKey.split("_");
	// Integer outputPortNumber = Integer.parseInt(values[2]);
	//
	// if (outputSets.get(i) == null) {
	// outputSets.add(new HashMap<Integer, IOObject>());
	// }
	// outputSets.get(i).put(outputPortNumber,
	// retriveResult(cacheKey));
	// }
	// }
	// i++;
	// } catch (InterruptedException e) {
	// e.printStackTrace();
	// throw new OperatorException(
	// "IgniteJobManagerHelper.processResponse()[InterruptedException]",
	// e);
	// } catch (ExecutionException e) {
	// e.printStackTrace();
	// throw new OperatorException(
	// "IgniteJobManagerHelper.processResponse()[ExecutionException]",
	// e);
	// }
	// }
	// return outputSets;
	// }
	//
	// @Override
	// public void toOutput(ArrayList<String> outputSet, InputPorts
	// outputExtender) {
	// Iterator<InputPort> iterator = outputExtender.getAllPorts().iterator();
	//
	// int portNo = 0;
	// while (iterator.hasNext()) {
	// portNo++;
	//
	// InputPort inputPort = iterator.next();
	//
	// if (inputPort.isConnected()) {
	// inputPort.receive(retriveData(outputSet.get(portNo)));
	// }
	//
	// }
	// }
	//
	// @Override
	// public void toOutput(HashMap<Integer, IOObject> outputSet,
	// CollectingPortPairExtender outputExtender) {
	// Iterator<PortPair> iterator = outputExtender.getManagedPairs()
	// .iterator();
	// Helper.out("managed pairs size: "
	// + outputExtender.getManagedPairs().size());
	// int portNo = 0;
	// while (iterator.hasNext()) {
	// portNo++;
	//
	// OutputPort outputPort = iterator.next().getOutputPort();
	//
	// if (outputPort.isConnected() && outputSet.get(portNo) != null) {
	// outputPort.deliver(outputSet.get(portNo));
	// }
	//
	// }
	// }
	//
	// @Override
	// public List<Future<String>> invokeAll() throws InterruptedException {
	// return getExecutorService().invokeAll(jobList);
	// }
	//
	// @Override
	public void toOutput(
			HashMap<Integer, HashMap<Integer, IOObject>> outputSet,
			PortPairExtender outputExtender) {
		Iterator<PortPair> iterator = outputExtender.getManagedPairs()
				.iterator();
		// Helper.out("managed pairs size: "
		// + outputExtender.getManagedPairs().size());
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

	//
	// @Override
	// public ArrayList<String> prepareDataForNoneSubprocess(
	// ArrayList<IOObject> data) {
	// ArrayList<String> keySet = new ArrayList<String>();
	// for (IOObject io : data) {
	// String key = storeData(null, io);
	// keySet.add(key);
	// }
	// return keySet;
	// }
	//
	// @Override
	// public void toOutputArray(ArrayList<HashMap<Integer, IOObject>>
	// outputSet,
	// PortPairExtender outputExtender) {
	// Iterator<PortPair> iterator = outputExtender.getManagedPairs()
	// .iterator();
	// Helper.out("managed pairs size: "
	// + outputExtender.getManagedPairs().size());
	// int portNo = 0;
	// while (iterator.hasNext()) {
	// portNo++;
	//
	// OutputPort outputPort = iterator.next().getOutputPort();
	//
	// if (outputPort.isConnected() && outputSet.get(portNo) != null) {
	// ArrayList<IOObject> iOObjectList = new ArrayList<IOObject>(
	// outputSet.get(portNo).values());
	// IOObjectCollection<IOObject> ioc = new IOObjectCollection<IOObject>(
	// iOObjectList);
	// outputPort.deliver(ioc);
	// }
	//
	// }
	//
	// }

	@Override
	public void removeAllData() {
		DATACache.removeAll();
	}

	@Override
	public RemoteJob createJob(String xml, HashMap<Integer, String> dataKeys,
			HashMap<String, String> macros) {
		// Helper.out("UUID : " + uuid);

		return new RemoteJob(xml, dataKeys, macros,
				IgniteJobManagerHelper.class.getName());
	}

	// @Override
	// public void prepareForRemoteJob() {
	// // TODO Auto-generated method stub
	//
	// }
	//
	// @Override
	// public void prepareForRemoteJob(Object instance) {
	// TODO Auto-generated method stub

	// }

	@Override
	public HashMap<Integer, ArrayList<IOObject>> resultKeysToOutput(
			List<Future<String>> resultKeys) throws InterruptedException,
			ExecutionException {
		HashMap<Integer, ArrayList<IOObject>> toOutput = new HashMap<Integer, ArrayList<IOObject>>();
		if (resultKeys != null) {
			// Helper.out("resultKeys size: " + resultKeys.size());
			if (resultKeys.size() > 0) {
				for (Future<String> nodeResult : resultKeys) {
					String keys = nodeResult.get();
					if (keys != null) {
						String[] values = keys.split(";");
						Integer portNo = 0;
						// Helper.out("Job result = [" + keys + "]");
						for (String value : values) {
							// Helper.out("retrive: [" + ((String) value)
							// + "]");
							IOObject io = retriveResult(value);

							if (toOutput.get(portNo) == null) {
								toOutput.put(portNo, new ArrayList<IOObject>());
							}
							if (io != null) {
								toOutput.get(portNo).add(io);
								portNo++;
							} else {
								// Helper.out("IO IS NULL!");
							}
						}
					}
				}
			}
		}
		return toOutput;
	}

}
