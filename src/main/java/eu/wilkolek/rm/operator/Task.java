package eu.wilkolek.rm.operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;

import com.rapidminer.RapidMiner;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.ExampleSetFactory;
import com.rapidminer.operator.IOContainer;
import com.rapidminer.operator.IOObject;
import com.rapidminer.operator.IOObjectCollection;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorChain;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.meta.AbstractIteratingOperatorChain;
import com.rapidminer.operator.meta.IteratingOperatorChain;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.PortPairExtender;
import com.rapidminer.operator.ports.PortPairExtender.PortPair;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.ParameterTypeString;

import eu.wilkolek.rm.util.CacheTools;
import eu.wilkolek.rm.util.Configurator;
import eu.wilkolek.rm.util.GridClassLoader;
import eu.wilkolek.rm.util.ProcessXML;
import eu.wilkolek.types.gridgain.GridRmTask;
import eu.wilkolek.types.rapidminer.IOObjectStub;
import eu.wilkolek.types.rapidminer.IOString;

public class Task extends OperatorChain {

	public static final String PER_NODE = "Per node";
	public static final String PER_SUBPROCESS = "Per subprocess";
	public static final String PERSISTENT_DATA = "Persistent data in cache";
	public static final String FLUSH_CACHE = "Flush cache";
	public static final String PASSED_VARIABLE = "Variable";
	public static final String MACRO_NAME = "Macro";

	ProcessXML xmlTools = new ProcessXML();

	IgniteCache<String, IOObject> cache;
	IgniteCache<String, IOObject> result;

	ArrayList<ArrayList<ArrayList<String>>> keySetForAll;
	ArrayList<ArrayList<String>> keySetForElement;
	ArrayList<String> keySetForNone;
	HashSet<String> keysToRemoveCache = new HashSet<String>();

	@IgniteInstanceResource
	private Ignite grid;
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
	public Task(OperatorDescription description) {
		this(description, "Nested Chain");
		this.id = (int) Math.round(Math.random() * 200);
	}

	/** This constructor allows subclasses to change the subprocess' name. */
	protected Task(OperatorDescription description, String subProcessName) {
		super(description, subProcessName);
		inputExtender.start();
		outputExtender.start();
		this.id = (int) Math.round(Math.random() * 1000);
		getTransformer().addRule(inputExtender.makePassThroughRule());
	}

	@Override
	public void doWork() throws OperatorException {
		iteration++;
		Configurator.out("Task.doWork()");
		// this.getRoot().getProcess().getMacroHandler().
		// if (grid == null){
		// Configurator.out("anotation don't work");
		if (Configurator.ignite != null) {
			grid = Configurator.ignite;
		} else {
			grid = ((GridClassLoader) Thread.currentThread()
					.getContextClassLoader()).getIgnite();
		}
		if (grid == null) {
			throw new OperatorException(
					"Ignite not started. Use Ignite operator.");
		}

		// }
		perNode = getParameterAsBoolean(PER_NODE);
		perSubprocess = getParameterAsBoolean(PER_SUBPROCESS);
		flushCache = getParameterAsBoolean(FLUSH_CACHE);
		persistentData = getParameterAsBoolean(PERSISTENT_DATA);
		passedVariable = getParameterAsString(PASSED_VARIABLE);
		macroName = getParameterAsString(MACRO_NAME);

		Configurator.out(macroName + "--" + passedVariable);

		subprocessCount = subprocessCount();
		clearAllInnerSinks();
		inputExtender.passDataThrough();

		String xml = this.getXML(false);

		IOString procesXML = new IOString();
		procesXML.setSource(xmlTools.processXML(this, xml, this.getProcess()
				.getRootOperator().getXML(false), inputExtender
				.getManagedPairs().size() - 1, macroName, passedVariable));

		HashMap<Integer, HashMap<Integer, IOObject>> outputSet = compute(
				procesXML, true);

		Configurator.toOutput(outputSet, outputExtender);

		if (!Configurator.delayedExecution) {
			result.removeAll();
			if (!persistentData) {
				cache.removeAll(keysToRemoveCache);
			}
		} else {
			if (!persistentData) {
				Configurator.keySetsToRemove.add(keysToRemoveCache);
			}
		}
	}

	private ArrayList<ArrayList<String>> prepareDataForNode(
			IgniteCache<String, IOObject> cache, ArrayList<IOObject> data) {
		ArrayList<ArrayList<String>> keySet = new ArrayList<ArrayList<String>>();
		Integer keyInput = 0;

		for (IOObject io : data) {
			Integer keyElement = 0;
			IOObjectCollection<IOObject> ion = (IOObjectCollection<IOObject>) io;
			ArrayList<String> elementList = new ArrayList<String>();
			for (IOObject ione : ion.getObjects()) {
				String key = id + "_" + iteration + "_" + keyInput + "_"
						+ keyElement;
				cache.put(key, ione);
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
			IgniteCache<String, IOObject> cache, ArrayList<IOObject> data) {
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
					cache.put(key, iose);
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
		Configurator.out("inputs :" + keySet.size());
		Configurator.out("nodes :" + keySet.get(0).size());
		Configurator.out("subprocesses :" + keySet.get(0).get(0).size());
		return keySet;
	}

	private ArrayList<String> prepareDataForNoneSubprocess(
			IgniteCache<String, IOObject> cache, ArrayList<IOObject> data) {
		ArrayList<String> keySet = new ArrayList<String>();
		Integer key = 0;
		for (IOObject io : data) {
			String k = (id + "_" + iteration + "_" + key).toString();
			cache.put(k, io);
			keysToRemoveCache.add(k);
			keySet.add(k);
			key++;
		}
		return keySet;
	}

	private HashMap<Integer, HashMap<Integer, IOObject>> compute(
			final IOString procesXML, Boolean loadingType) {
		IOString process = procesXML;
		HashMap<Integer, HashMap<Integer, IOObject>> returnValue = new HashMap<Integer, HashMap<Integer, IOObject>>();
		HashMap<String, String> macros = new HashMap<String, String>();
		Iterator<String> macroNamesIterator = this.getProcess().getMacroHandler().getDefinedMacroNames();
		while (macroNamesIterator.hasNext()){
			String key = macroNamesIterator.next();
			String macro = this.getProcess().getMacroHandler().getMacro(key);
			macros.put(key, macro);
		}
		
		Configurator.out("comput()");
		// try (Ignite grid = Ignition.start(Configurator.getCfgFile()
		// .getAbsolutePath())) {

		cache = grid.jcache("cache");
		result = grid.jcache("result");

		Integer dataForNumberOfNodes = 0;
		Integer numberOfData = inputExtender.getManagedPairs().size() - 1;
		ArrayList<IOObject> inputData = new ArrayList<>();
		for (int i = 0; i < inputExtender.getManagedPairs().size() - 1; i++) {
			IOObject set = (IOObject) inputExtender.getManagedPairs().get(i)
					.getOutputPort().getAnyDataOrNull();
			if (set != null) {
				inputData.add(set);
			}
		}

		boolean theSameSettings = false;
		if (Configurator.lastPerNode == null
				|| Configurator.lastPerSubprocess == null) {
			theSameSettings = false;
		} else {
			theSameSettings = ((Configurator.lastPerNode == perNode) && (Configurator.lastPerSubprocess == perSubprocess));
		}

		if (!persistentData || flushCache || Configurator.flushCache
				|| !theSameSettings) {

			if (perSubprocess && perNode) {
				keySetForAll = prepareDataForAll(cache, inputData);
			} else {
				if (perNode && !perSubprocess) {
					keySetForElement = prepareDataForNode(cache, inputData);
				} else {
					keySetForNone = prepareDataForNoneSubprocess(cache,
							inputData);
				}
			}
		}

		ArrayList<ExampleSet> wyniki = new ArrayList<ExampleSet>();
		Iterator<ClusterNode> nod = grid.cluster().nodes().iterator();

		while (nod.hasNext()) {
			ClusterNode cn = nod.next();
			Configurator.out("id: " + cn.id() + " local:" + cn.isLocal());
		}

		ClusterGroup projection = grid.cluster().forRemotes();
		ArrayList<GridRmTask> tasks = new ArrayList<GridRmTask>();
		ArrayList<String> jobsOutputs = new ArrayList<String>();
		
		GridRmTask task = new GridRmTask(process.getSource(), 0,
				new ArrayList<String>(), id, iteration, macros); // stub//
		// 0 - TaskID, 1- iteration, 2 - nodeId, 3 - outputPortNumber
		if (perNode && !perSubprocess) {
			Integer nodesRequired = keySetForElement.get(0).size();
			for (int i = 0; i < nodesRequired; i++) {
				ArrayList<String> keySetForTask = new ArrayList<String>();
				for (int we = 0; we < keySetForElement.size(); we++) {
					keySetForTask.add(keySetForElement.get(we).get(i));
				}

				tasks.add(new GridRmTask(process.getSource(), i, keySetForTask,
						id, iteration, macros));
				jobsOutputs.add(id+"_"+iteration+"_"+i+"_");
			}
		}

		if (perNode && perSubprocess) {
			HashMap<String, ArrayList<String>> keyMap = new HashMap();
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
			Configurator.out("Keyset size: " + keyMap.keySet().size());
			for (String key : keyMap.keySet()) {
				Configurator.out("key #" + key);
				String values[] = key.split("_");
				Integer j = Integer.parseInt(values[1]); // subprocess
				Integer i = Integer.parseInt(values[0]); // node
				tasks.add(new GridRmTask(selectSubproces(
						(j) % keySetForAll.get(0).get(0).size() + 1, process)
						.getSource(), rm, keyMap.get(key), id, iteration, macros));
				jobsOutputs.add(id+"_"+iteration+"_"+rm+"_");
				rm++;
			}

		}

		if (!perNode) {
			if (perSubprocess) {
				for (int p = 0; p < subprocessCount; p++) {
					tasks.add(new GridRmTask(selectSubproces(p + 1, process)
							.getSource(), p, keySetForNone, id, iteration, macros));
					jobsOutputs.add(id+"_"+iteration+"_"+p+"_");
				}
			} else {
				for (int p = 0; p < grid.cluster().nodes().size(); p++) {
					tasks.add(new GridRmTask(process.getSource(), p,
							keySetForNone, id, iteration, macros));
					jobsOutputs.add(id+"_"+iteration+"_"+p+"_");
				}
			}
		}

		List<Future<String>> resultKeys;
		// List<IOObject> result =new ArrayList<IOObject>();
		try {
			Configurator.out("Tasks to compute: " + tasks.size());
			if (Configurator.delayedExecution) {
				Configurator.addJobList(tasks);
				for (int po = 1; po < outputExtender.getManagedPairs().size(); po++) {
					Integer numberOfResult = 1;
					for (String pre : jobsOutputs) {
						//ExampleSet o = ExampleSetFactory.
						if (returnValue.get(po) == null){
							returnValue.put(po, new HashMap<Integer, IOObject>());
						}
						returnValue.get(po).put(numberOfResult, new IOObjectStub(pre+po));
						numberOfResult++;
					}
				}
			} else {
				ExecutorService exec = Configurator.masterOperator
						.getExecutorService();
				resultKeys = exec.invokeAll(tasks);
				returnValue = Configurator.processResponse(resultKeys, result);
			}
			Configurator.out("return " + returnValue.size());
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// } catch (IgniteException e1) {
		// e1.printStackTrace();
		// }

		Configurator.out("COMPUTE: " + returnValue.size() + " exampleSets");
		return returnValue;
	}

	private Integer subprocessCount() {
		Iterator<Operator> it = getSubprocess(0).getOperators().iterator();
		while (it.hasNext()) {
			Operator op = it.next();
			if (SubprocessDeclaration.class.getSimpleName().equals(
					op.getOperatorClassName())) {
				OperatorChain chain = (OperatorChain) op;
				Configurator.out("Subprocess size()"
						+ chain.getSubprocesses().size());
				return chain.getSubprocesses().size();
			}
		}

		return 0;
	}

	

	private void printExampleSet(ExampleSet resultSet) {
		int i = 0;
		for (Example example : resultSet) {
			Iterator<Attribute> allAtts = example.getAttributes()
					.allAttributes();

			while (allAtts.hasNext()) {
				Attribute a = allAtts.next();
				if (i <= 3)
					System.out.print(a.getName() + "  ");

				i++;
			}
		}
		Configurator.out("");
		for (Example example : resultSet) {
			Iterator<Attribute> allAtts = example.getAttributes()
					.allAttributes();

			while (allAtts.hasNext()) {
				Attribute a = allAtts.next();

				if (a.isNumerical()) {
					double value = example.getValue(a);
					System.out.print(value + " ");
				} else {
					String value = example.getValueAsString(a);
					System.out.print(value + " ");

				}
			}
			Configurator.out("");
		}
	}

	private IOString selectSubproces(int nodeId, final IOString xml) {
		Configurator.out("select subprocess");
		IOString processingText = xml;
		String patternString = "class=\"ignite_extension:SubprocessDeclaration\"";
		String selected = "";
		// Pattern pattern = Pattern.compile(patternString);

		boolean nextLineIsTheOneToProcess = false;

		String[] lines = processingText.getSource().split(
				System.getProperty("line.separator"));
		for (String line : lines) {

			// Matcher matcher = pattern.matcher(line);

			if (line.contains(patternString)) {
				// Configurator.out("FOUND LINE WITH SubprocessDeclaration\n"+line);
				line.replace("ignite_extension:SubprocessDeclaration",
						"ignite_extension:SubprocessDeclarationInvocation");
				line += System.getProperty("line.separator")
						+ "<parameter key=\"select_which\" value=\"" + nodeId
						+ "\" />" + System.getProperty("line.separator");
				// Configurator.out("\n\nAFTER MODS: \n" +line+"\n\n\n");
			}

			selected += line + "\n";
		}

		return new IOString(selected);
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