package eu.wilkolek.pardi.operator.ignite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
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
import com.rapidminer.operator.ports.CollectingPortPairExtender;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.InputPorts;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.PortPairExtender;
import com.rapidminer.operator.ports.PortPairExtender.PortPair;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;

import eu.wilkolek.pardi.types.rapidminer.AbstractJobManager;
import eu.wilkolek.pardi.types.rapidminer.RemoteJob;
import eu.wilkolek.pardi.util.BeanHandler;
import eu.wilkolek.pardi.util.Config;
import eu.wilkolek.pardi.util.PluginClassLoader;
import eu.wilkolek.pardi.util.Helper;
import eu.wilkolek.pardi.util.ignite.IgniteJobManagerHelper;

public class IgniteJobManager extends AbstractJobManager<IgniteRemoteJob> {

	HashSet<Integer> generatedKeys = new HashSet<Integer>();
	Integer currentKeyToReturn = 1;
	IgniteJobManagerHelper helper = new IgniteJobManagerHelper();
	Boolean needInit = true;
	String INIT_REMOTE_NODES = "Initiate remote nodes";

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
		helper = (IgniteJobManagerHelper) BeanHandler.getInstance().getBeans(
				"ignite");
		if (helper == null) {
			helper = new IgniteJobManagerHelper();
			helper.asureInstanceIsReady();
			BeanHandler.getInstance().addBeans("ignite", helper);
		}
		BeanHandler.getInstance().setCurrentBean("ignite");

		UUID uuid = helper.ignite.cluster().localNode().id();
		helper.uuid = uuid.toString();

		if (getParameterAsBoolean(INIT_REMOTE_NODES) && needInit) {

			RemoteJob job = new RemoteJob(xmlInitRemote,
					new HashMap<Integer, String>(),
					new HashMap<String, String>(),
					IgniteJobManagerHelper.class.getName(), uuid.toString());
			ArrayList<RemoteJob> jobList = new ArrayList<RemoteJob>();
			for (int jn = 0; jn < helper.nodeCount(); jn++) {
				jobList.add(job);
			}
			Helper.out("Init Remote Jobs : " + jobList.size());
			try {
				helper.getExecutorService().invokeAll(jobList);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			needInit = false;
		}

		Helper.out("version: [" + Config.version + "]");
		inputExtender.passDataThrough();
		Helper.flushCache = getParameterAsBoolean(FLUSH_CACHE);
		// Helper.masterOperator = this;
		try {
			Helper.out("super.doWork()");
			super.doWork();
			outputExtender.passDataThrough();
		} catch (OperatorException e) {
			helper.ignite.close();
			helper.ignite = null;
			BeanHandler.getInstance().removeBean("ignite");
			helper = null;
			Helper.out("Ignite stop");
			throw new OperatorException("Something gone wrong", e);
		} finally {
			if (helper != null) {
				if (helper.ignite != null) {
					helper.removeAllResults();
				}
			}
		}
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> types = super.getParameterTypes();
		types.add(new ParameterTypeBoolean(INIT_REMOTE_NODES,
				"Init remote nodes", Boolean.FALSE));
		return types;
	}

	// @Override
	// public ExecutorService getExecutorService() {
	// asureInstanceIsReady();
	// return helper.getExecutorService();
	// }
	//
	// @Override
	// public int nodeCount() {
	// asureInstanceIsReady();
	// return helper.nodeCout();
	// }
	//
	// @Override
	// public void removeAllDataByKeys(HashSet<Object> keysToRemoveCache) {
	// asureInstanceIsReady();
	//
	// helper.removeAllDataByKeys(keysToRemoveCache);
	// }
	//
	// @Override
	// public void removeAllResults() {
	// asureInstanceIsReady();
	// helper.removeAllData();
	// }
	//
	// @Override
	// public String storeData(Object key, IOObject obj) {
	// asureInstanceIsReady();
	// return helper.storeData(key, obj);
	// }
	// @Override
	// public String storeResult(Object key, IOObject obj) {
	// asureInstanceIsReady();
	// return helper.storeResult(key, obj);
	// }
	//
	//
	//
	// @Override
	// public IOObject retriveData(Object key) {
	// asureInstanceIsReady();
	// String k = (String)key;
	//
	// return helper.retriveData(key);
	// }
	//
	// @Override
	// public IOObject retriveResult(Object key) {
	// asureInstanceIsReady();
	// String k = (String)key;
	// return helper.retriveData(key);
	// }
	// // @Override
	// // public ArrayList<HashMap<Integer, IOObject>> processResponseToArray(
	// // List<Future<Object>> resultKeys) throws OperatorException {
	// //
	// // return helper.processResponseToArray(resultKeys);
	// // }
	// // @Override
	// // public HashMap<Integer, HashMap<Integer, IOObject>> processResponse(
	// // List<Future<Object>> resultKeys) throws OperatorException {
	// // return helper.processResponse(resultKeys);
	// // }
	// @Override
	// public void toOutput(
	// ArrayList<String> cacheKeys,
	// InputPorts outputExtender) {
	// helper.toOutput(cacheKeys, outputExtender);
	//
	// }
	// @Override
	// public void toOutput(
	// HashMap<Integer, HashMap<Integer, IOObject>> outputSet,
	// PortPairExtender outputExtender) {
	// helper.toOutput(outputSet, outputExtender);
	//
	// }
	// @Override
	// public void toOutputArray(
	// ArrayList<HashMap<Integer, IOObject>> outputSet,
	// PortPairExtender outputExtender) {
	// helper.toOutputArray(outputSet, outputExtender);
	//
	// }
	// @Override
	// public void toOutput(
	// HashMap<Integer, IOObject> outputSet,
	// CollectingPortPairExtender outExtender) {
	// helper.toOutput(outputSet, outExtender);
	//
	// }
	// // @Override
	// // public List<Future<String>> invokeAll() throws InterruptedException {
	// // return
	// IgniteJobManagerHelper.ignite.executorService(IgniteJobManagerHelper.ignite.cluster().forRemotes()).invokeAll(jobList);
	// // }
	//
	// public ArrayList<String> prepareDataForNoneSubprocess(
	// ArrayList<IOObject> data) {
	// return helper.prepareDataForNoneSubprocess(data);
	// }
	//
	// @Override
	// public List<Future<String>> invokeAll() throws InterruptedException {
	// return helper.invokeAll();
	// }
	//
	//
	//

	private String xmlInitRemote = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>"
			+ "<process version=\"5.3.015\">"
			+ "<context>"
			+ "<input/>"
			+ "<output/>"
			+ "<macros/>"
			+ " </context>"
			+ "<operator activated=\"true\" class=\"process\" compatibility=\"5.3.015\" expanded=\"true\" name=\"Process\">"
			+ " <process expanded=\"true\">"
			+ "  <operator activated=\"true\" class=\"pardi_extension:Ignite\" compatibility=\"1.0.000\" expanded=\"true\" height=\"60\" name=\"Ignite\" width=\"90\" x=\"246\" y=\"75\">"
			+ "   <process expanded=\"true\">"
			+ "    <portSpacing port=\"source_iin 1\" spacing=\"0\"/>"
			+ "   <portSpacing port=\"sink_iou 1\" spacing=\"0\"/>"
			+ "</process>"
			+ "</operator>"
			+ "<portSpacing port=\"source_input 1\" spacing=\"0\"/>"
			+ "<portSpacing port=\"sink_result 1\" spacing=\"0\"/>"
			+ "</process>" + " </operator>" + "</process>";
}
