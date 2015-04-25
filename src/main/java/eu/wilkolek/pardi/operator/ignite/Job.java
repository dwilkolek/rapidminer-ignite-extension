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
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.resources.IgniteInstanceResource;

import com.rapidminer.operator.IOObject;
import com.rapidminer.operator.IOObjectCollection;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorChain;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.PortPairExtender;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.ParameterTypeString;

import eu.wilkolek.pardi.types.rapidminer.IOString;
import eu.wilkolek.pardi.util.BeanHandler;
import eu.wilkolek.pardi.util.Config;
import eu.wilkolek.pardi.util.PluginClassLoader;
import eu.wilkolek.pardi.util.Helper;
import eu.wilkolek.pardi.util.XMLTools;
import eu.wilkolek.pardi.util.ignite.IgniteJobManagerHelper;

public class Job extends OperatorChain {

	public static final String PER_NODE = "Per node";
	public static final String PER_SUBPROCESS = "Per subprocess";
	public static final String PERSISTENT_DATA = "Persistent data in cache";
	public static final String FLUSH_CACHE = "Flush cache";
	public static final String PASSED_VARIABLE = "Variable";
	public static final String MACRO_NAME = "Macro";

	XMLTools xmlTools = new XMLTools();

	ArrayList<ArrayList<ArrayList<String>>> keySetForAll;
	ArrayList<ArrayList<String>> keySetForElement;
	ArrayList<String> keySetForNone;
	HashSet<Object> keysToRemoveCache = new HashSet<Object>();
	IgniteJobManagerHelper helper = (IgniteJobManagerHelper)BeanHandler.getInstance().getBeans("ignite");
	
	private int id = -1;

	Boolean perNode;
	Boolean perSubprocess;
	Boolean flushCache;
	Boolean persistentData;
	String passedVariable = "";
	String macroName = "";
	Integer subprocessCount = 0;
	int iteration = 0;

	protected PortPairExtender inputExtender = new PortPairExtender("gin",
			getInputPorts(), getSubprocess(0).getInnerSources());

	protected PortPairExtender outputExtender = new PortPairExtender("gou",
			getSubprocess(0).getInnerSinks(), getOutputPorts());

	/** Creates an empty operator chain. */
	public Job(OperatorDescription description) {
		this(description, "Nested Chain");
		this.id = (int) Math.round(Math.random() * 200);
	}

	/** This constructor allows subclasses to change the subprocess' name. */
	protected Job(OperatorDescription description, String subProcessName) {
		super(description, subProcessName);
		inputExtender.start();
		outputExtender.start();
		this.id = (int) Math.round(Math.random() * 1000);
		getTransformer().addRule(inputExtender.makePassThroughRule());
	}

	@Override
	public void doWork() throws OperatorException{
		helper = (IgniteJobManagerHelper)BeanHandler.getInstance().getBeans("ignite");
		iteration++;
		Helper.out("Job.doWork()");
		helper.asureInstanceIsReady();

		perNode = getParameterAsBoolean(PER_NODE);
		perSubprocess = getParameterAsBoolean(PER_SUBPROCESS);
		flushCache = getParameterAsBoolean(FLUSH_CACHE);
		persistentData = getParameterAsBoolean(PERSISTENT_DATA);
		

		subprocessCount = subprocessCount();
		clearAllInnerSinks();
		inputExtender.passDataThrough();

		String xml = this.getXML(false);

		IOString procesXML = new IOString();
		procesXML.setSource(xmlTools.processXML(this, xml, this.getProcess()
				.getRootOperator().getXML(false), inputExtender
				.getManagedPairs().size() - 1));

		
		HashMap<Integer, HashMap<Integer, IOObject>> outputSet = compute(
				procesXML, true);

		helper.toOutput(outputSet, outputExtender);

			helper.removeAllResults();
			if (!persistentData) {
				helper.removeAllDataByKeys(keysToRemoveCache);
			}
		
	}

	private ArrayList<ArrayList<String>> prepareDataForNode(
			ArrayList<IOObject> data) {
		ArrayList<ArrayList<String>> keySet = new ArrayList<ArrayList<String>>();
		Integer keyInput = 0;

		for (IOObject io : data) {
			Integer keyElement = 0;
			IOObjectCollection<IOObject> ion = (IOObjectCollection<IOObject>) io;
			ArrayList<String> elementList = new ArrayList<String>();
			for (IOObject ione : ion.getObjects()) {
				String key = id + "_" + iteration + "_" + keyInput + "_"
						+ keyElement;
				helper.storeData(key, ione);
				elementList.add(key);
				keysToRemoveCache.add(key);
				keyElement += 1;
			}
			keySet.add(elementList);
			keyInput += 100;
		}

		return keySet;
	}

	private ArrayList<ArrayList<ArrayList<String>>> prepareDataForAll(
			ArrayList<IOObject> data) {
		ArrayList<ArrayList<ArrayList<String>>> keySet = new ArrayList<ArrayList<ArrayList<String>>>();
		Integer keyInput = 0;

		for (IOObject io : data) {
			Integer keyNode = 0;
			IOObjectCollection<IOObject> ion = (IOObjectCollection<IOObject>) io;
			ArrayList<ArrayList<String>> nodeList = new ArrayList<ArrayList<String>>();
			for (IOObject ione : ion.getObjects()) {
				Integer keySubprocess = 0;
				IOObjectCollection<IOObject> ios = (IOObjectCollection<IOObject>) ione;
				ArrayList<String> subprocessList = new ArrayList<String>();
				for (IOObject iose : ios.getObjects()) {
					String key = id + "_" + iteration + "_" + keyInput + "_"
							+ keyNode + "_" + keySubprocess;
					helper.storeData(key, iose);
					keysToRemoveCache.add(key);
					subprocessList.add(key);
					keySubprocess++;
				}
				nodeList.add(subprocessList);
				keyNode += 10;
			}
			keySet.add(nodeList);
			keyInput += 100;
		}
		Helper.out("inputs :" + keySet.size());
		Helper.out("nodes :" + keySet.get(0).size());
		Helper.out("subprocesses :" + keySet.get(0).get(0).size());
		return keySet;
	}

	private ArrayList<String> prepareDataForNoneSubprocess(
			ArrayList<IOObject> data) {
		ArrayList<String> keySet = new ArrayList<String>();
		Integer key = 0;
		for (IOObject io : data) {
			String k = (id + "_" + iteration + "_" + key).toString();
			helper.storeData(k, io);
			keysToRemoveCache.add(k);
			keySet.add(k);
			key++;
		}
		return keySet;
	}

	private HashMap<Integer, HashMap<Integer, IOObject>> compute(
			final IOString procesXML, Boolean loadingType) throws OperatorException{
		IOString process = procesXML;
		HashMap<Integer, HashMap<Integer, IOObject>> returnValue = new HashMap<Integer, HashMap<Integer, IOObject>>();
		HashMap<String, String> macros = new HashMap<String, String>();
		Iterator<String> macroNamesIterator = this.getProcess().getMacroHandler().getDefinedMacroNames();
		while (macroNamesIterator.hasNext()){
			String key = macroNamesIterator.next();
			String macro = this.getProcess().getMacroHandler().getMacro(key);
			macros.put(key, macro);
		}
		
		Helper.out("compute()");
		
		

		ArrayList<IOObject> inputData = new ArrayList<>();
		for (int i = 0; i < inputExtender.getManagedPairs().size() - 1; i++) {
			IOObject set = (IOObject) inputExtender.getManagedPairs().get(i)
					.getOutputPort().getAnyDataOrNull();
			if (set != null) {
				inputData.add(set);
			}
		}

		boolean theSameSettings = false;
		if (Helper.lastPerNode == null
				|| Helper.lastPerSubprocess == null) {
			theSameSettings = false;
		} else {
			theSameSettings = ((Helper.lastPerNode == perNode) && (Helper.lastPerSubprocess == perSubprocess));
		}

		if (!persistentData || flushCache || Helper.flushCache
				|| !theSameSettings) {

			if (perSubprocess && perNode) {
				keySetForAll = prepareDataForAll(inputData);
			} else {
				if (perNode && !perSubprocess) {
					keySetForElement = prepareDataForNode(inputData);
				} else {
					keySetForNone = prepareDataForNoneSubprocess(inputData);
				}
			}
		}


		
		ArrayList<Callable<Object>> jobs = new ArrayList<Callable<Object>>();
		
		

		// 0 - JobID, 1- iteration, 2 - nodeId, 3 - outputPortNumber
		if (perNode && !perSubprocess) {
			Integer nodesRequired = keySetForElement.get(0).size();
			for (int i = 0; i < nodesRequired; i++) {
				ArrayList<String> keySetForJob = new ArrayList<String>();
				for (int we = 0; we < keySetForElement.size(); we++) {
					keySetForJob.add(keySetForElement.get(we).get(i));
				}
				jobs.add(new IgniteRemoteJob(process.getSource(), i, keySetForJob,
						id, iteration, macros,xmlTools.getOpNameForJob(process.getSource())));
			}
		}

		if (perNode && perSubprocess) {
			HashMap<String, ArrayList<String>> keyMap = new HashMap<String, ArrayList<String>>();
			for (int we = 0; we < keySetForAll.size(); we++) {
				for (int no = 0; no < keySetForAll.get(we).size(); no++) {
					for (int su = 0; su < keySetForAll.get(we).get(no).size(); su++) {
						String k = no + "_" + su;
						if (!keyMap.containsKey(k)) {
							keyMap.put(k, new ArrayList<String>());
						}
						keyMap.get(k).add(keySetForAll.get(we).get(no).get(su));
					}
				}
			}
			int rm = 0;
			Helper.out("Keyset size: " + keyMap.keySet().size());
			for (String key : keyMap.keySet()) {
				Helper.out("key #" + key);
				String values[] = key.split("_");
				Integer j = Integer.parseInt(values[1]); // subprocess
				Integer i = Integer.parseInt(values[0]); // node
				jobs.add(new IgniteRemoteJob(xmlTools.selectSubproces(
						(j) % keySetForAll.get(0).get(0).size() + 1, process)
						.getSource(), rm, keyMap.get(key), id, iteration, macros, xmlTools.getOpNameForJob(process.getSource())));
				rm++;
			}

		}

		if (!perNode) {
			if (perSubprocess) {
				for (int p = 0; p < subprocessCount; p++) {
					jobs.add(new IgniteRemoteJob(xmlTools.selectSubproces(p + 1, process)
							.getSource(), p, keySetForNone, id, iteration, macros, xmlTools.getOpNameForJob(process.getSource())));
				}
			} else {
				for (int p = 0; p < helper.nodeCount(); p++) {
					jobs.add(new IgniteRemoteJob(process.getSource(), p,
							keySetForNone, id, iteration, macros,xmlTools.getOpNameForJob(process.getSource())));
				}
			}
		}

		List<Future<String>> resultKeys;
		// List<IOObject> result =new ArrayList<IOObject>();
		try {
			Helper.out("Jobs to compute: " + jobs.size());
				ExecutorService exec = helper
						.getExecutorService();
//				resultKeys = exec.invokeAll(jobs);
				returnValue = helper.processResponse(exec.invokeAll(jobs));
			
			Helper.out("return " + returnValue.size());
		
		}catch (InterruptedException e){
			e.printStackTrace();
			throw new OperatorException("Computing thrown Interupted exception",e);
		}catch (Exception e) {
			throw new OperatorException("Computing thrown Exception",e);
		}
		
		Helper.out("COMPUTE: " + returnValue.size() + " exampleSets");
		return returnValue;
	}

	private Integer subprocessCount() {
		Iterator<Operator> it = getSubprocess(0).getOperators().iterator();
		while (it.hasNext()) {
			Operator op = it.next();
			if (JobSubprocess.class.getSimpleName().equals(
					op.getOperatorClassName())) {
				OperatorChain chain = (OperatorChain) op;
				Helper.out("Subprocess size()"
						+ chain.getSubprocesses().size());
				return chain.getSubprocesses().size();
			}
		}

		return 0;
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> types = super.getParameterTypes();
		types.add(new ParameterTypeBoolean(
				PER_NODE,
				"Load different data per process. 1st element of inputs collection for 1 node, 2nd for 2nd node etc.",
				Boolean.FALSE));
		types.add(new ParameterTypeBoolean(PER_SUBPROCESS,
				"Loads the same data per SubprocessDeclarations subprecesses",
				Boolean.FALSE));
		types.add(new ParameterTypeBoolean(FLUSH_CACHE,
				"Flush data from cache", Boolean.FALSE));
		types.add(new ParameterTypeBoolean(PERSISTENT_DATA,
				"After 1st run save original data in cache.", Boolean.TRUE));
		types.add(new ParameterTypeString(PASSED_VARIABLE, "Passed variable",
				""));
		types.add(new ParameterTypeString(MACRO_NAME, "Local macro name", ""));
		return types;
	}
}