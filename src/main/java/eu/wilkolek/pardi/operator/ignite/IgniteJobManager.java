package eu.wilkolek.pardi.operator.ignite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;

import com.rapidminer.RapidMiner;
import com.rapidminer.operator.IOObject;
import com.rapidminer.operator.IOObjectCollection;
import com.rapidminer.operator.OperatorChain;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.PortPairExtender;
import com.rapidminer.operator.ports.PortPairExtender.PortPair;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;

import eu.wilkolek.pardi.types.rapidminer.AbstractJobManager;
import eu.wilkolek.pardi.util.Config;
import eu.wilkolek.pardi.util.GridClassLoader;
import eu.wilkolek.pardi.util.Helper;
import eu.wilkolek.pardi.util.ignite.IgniteJobManagerHelper;

public class IgniteJobManager extends AbstractJobManager<IgniteRemoteJob> {
	
	
	/** This constructor allows subclasses to change the subprocess' name. */
	protected IgniteJobManager(OperatorDescription description,
			String subProcessName) {
		super(description, subProcessName);
		inputExtender.start();
		outputExtender.start();
		getTransformer().addRule(inputExtender.makePassThroughRule());
	}

	public IgniteJobManager(OperatorDescription description) {
		this(description, "IgniteTaskManager");
	}

	@Override
	public void doWork() throws OperatorException {
		Helper.out("version: ["+Config.version+"]");
		inputExtender.passDataThrough();
		Helper.flushCache = getParameterAsBoolean(FLUSH_CACHE);
		Helper.masterOperator = this;
		try {
			asureInstanceIsReady();
			Helper.out("super.doWork()");
			super.doWork();
			outputExtender.passDataThrough();
		} catch (OperatorException e) {
			IgniteJobManagerHelper.ignite.close();
			IgniteJobManagerHelper.ignite = null;
			Helper.out("Ignite stop");
			throw new OperatorException("Something gone wrong");
		} finally {
			if (IgniteJobManagerHelper.ignite != null) {
				IgniteCache<String, IOObject> cache =IgniteJobManagerHelper.ignite
						.jcache("cache");
				if (Helper.flushCache) {
					removeAllData();
				}
			}
		}
	}

	private void removeAllData() {
		asureInstanceIsReady();
		IgniteJobManagerHelper.DATACache.removeAll();
	}

	public void asureInstanceIsReady() {
		if (IgniteJobManagerHelper.ignite == null) {
			if (IgniteJobManagerHelper.loader==null){
					prepareClassLoader();
			}	
			IgniteJobManagerHelper.ignite = Ignition.start(IgniteJobManagerHelper.getCfgFile()
					.getAbsolutePath());
			Helper.out("Ignite start");
			((GridClassLoader) Thread.currentThread()
					.getContextClassLoader()).setIgnite(IgniteJobManagerHelper.ignite);
			
			
			IgniteJobManagerHelper.DATACache = IgniteJobManagerHelper.ignite.jcache(IgniteJobManagerHelper.DATA);
			IgniteJobManagerHelper.RESULTCache = IgniteJobManagerHelper.ignite.jcache(IgniteJobManagerHelper.RESULT);
		}
	}

	private void prepareClassLoader() {
		IgniteConfiguration cfg = new IgniteConfiguration();
		IgniteJobManagerHelper.loader = cfg.getClass().getClassLoader();
		GridClassLoader wcl = new GridClassLoader(
				RapidMiner.class.getClassLoader(), cfg.getClass()
						.getClassLoader());

		try {
			IgniteJobManagerHelper.loader = wcl;
			IgniteJobManagerHelper.loader.loadClass(IgniteConfiguration.class
					.getCanonicalName());
			Thread.currentThread().setContextClassLoader(IgniteJobManagerHelper.loader);
			Thread.currentThread().getContextClassLoader()
					.loadClass(IgniteConfiguration.class.getCanonicalName());
		} catch (ClassNotFoundException e2) {
			Helper.out("loader don't load");
		}
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> types = super.getParameterTypes();
		types.add(new ParameterTypeBoolean(FLUSH_CACHE,
				"Flush data from cache", Boolean.FALSE));
		return types;
	}

	@Override
	public ExecutorService getExecutorService() {
		asureInstanceIsReady();
		return IgniteJobManagerHelper.ignite.executorService(IgniteJobManagerHelper.ignite.cluster().forRemotes());
	}

	@Override
	public int nodeCount() {
		asureInstanceIsReady();
		return IgniteJobManagerHelper.ignite.cluster().forRemotes().nodes().size();
	}

	@Override
	public void removeAllDataByKeys(HashSet<Object> keysToRemoveCache) {
		asureInstanceIsReady();
		Iterator<Object> iterator = keysToRemoveCache.iterator();
		HashSet<String> keyList = new HashSet<String>();
		while (iterator.hasNext()){
			keyList.add((String)iterator.next());
		}
		IgniteJobManagerHelper.DATACache.removeAll(keyList);
	}

	@Override
	public void removeAllResults() {
		asureInstanceIsReady();
		IgniteJobManagerHelper.RESULTCache.removeAll();
	}

	@Override
	public void storeData(Object key, IOObject obj) {
		asureInstanceIsReady();
		String k = (String)key;
		IgniteJobManagerHelper.DATACache.put(k, obj);
	}

	@Override
	public void storeResult(Object key, IOObject obj) {
		asureInstanceIsReady();
		String k = (String)key;
		IgniteJobManagerHelper.ignite.jcache(IgniteJobManagerHelper.RESULT).put(k, obj);
	}

	@Override
	public IOObject retriveData(Object key) {
		asureInstanceIsReady();
		String k = (String)key;
		
		return IgniteJobManagerHelper.DATACache.get(k);
	}

	@Override
	public IOObject retriveResult(Object key) {
		asureInstanceIsReady();
		String k = (String)key;
		return IgniteJobManagerHelper.RESULTCache.get(k);
	}
	
	@Override
	public HashMap<Integer, HashMap<Integer, IOObject>> processResponse(
			List<Future<String>> resultKeys) {

		Iterator<Future<String>> resultKeysIterator = resultKeys.iterator();
		HashMap<Integer, HashMap<Integer, IOObject>> outputSets = new HashMap<Integer, HashMap<Integer, IOObject>>();

		while (resultKeysIterator.hasNext()) {
			try {
				String code = resultKeysIterator.next().get();
				if (!code.isEmpty()){
					String[] cacheKeys = code.split(";");
					for (String cacheKey : cacheKeys) {
						// 0 - JobID, 1- iteration, 2 - nodeId, 3 - outputPortNumber
						String[] values = cacheKey.split("_");
						Integer nodeId = Integer.parseInt(values[2]);
						Integer outputPortNumber = Integer.parseInt(values[3]);
	
						if (!outputSets.containsKey(outputPortNumber)) {
							outputSets.put(outputPortNumber,
									new HashMap<Integer, IOObject>());
						}
						outputSets.get(outputPortNumber).put(nodeId,
								Helper.masterOperator.retriveResult(cacheKey));
					}
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
	
	@Override
	public void toOutput(
			HashMap<Integer, HashMap<Integer, IOObject>> outputSet,
			PortPairExtender outputExtender) {
		Iterator<PortPair> iterator = outputExtender.getManagedPairs()
				.iterator();
		Helper.out("managed pairs size: "
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

	@Override
	public Callable<IgniteRemoteJob> createJob(HashMap<String, Object> params) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Class<IgniteRemoteJob> jobReturnType() {
		return IgniteRemoteJob.class;
	}
		
	
}
