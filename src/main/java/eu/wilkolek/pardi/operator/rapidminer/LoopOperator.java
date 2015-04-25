package eu.wilkolek.pardi.operator.rapidminer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.rapidminer.MacroHandler;
import com.rapidminer.Process;
import com.rapidminer.io.process.XMLExporter;
import com.rapidminer.operator.ExecutionUnit;
import com.rapidminer.operator.IOContainer;
import com.rapidminer.operator.IOObject;
import com.rapidminer.operator.IOObjectCollection;
import com.rapidminer.operator.OperatorChain;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ProcessRootOperator;
import com.rapidminer.operator.ValueDouble;
import com.rapidminer.operator.meta.IteratingOperatorChain;
import com.rapidminer.operator.ports.CollectingPortPairExtender;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.PortPairExtender;
import com.rapidminer.operator.ports.PortPairExtender.PortPair;
import com.rapidminer.operator.ports.metadata.SubprocessTransformRule;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.parameter.conditions.BooleanParameterCondition;

import eu.wilkolek.pardi.types.rapidminer.IOString;
import eu.wilkolek.pardi.types.rapidminer.RemoteJob;
import eu.wilkolek.pardi.util.AbstractJobManagerHelper;
import eu.wilkolek.pardi.util.BeanHandler;
import eu.wilkolek.pardi.util.Config;
import eu.wilkolek.pardi.util.Helper;
import eu.wilkolek.pardi.util.XMLTools;

public class LoopOperator extends OperatorChain {

	/** The parameter name for &quot;Number of iterations&quot; */
	public static final String PARAMETER_ITERATIONS = "iterations";

	public static final String PARAMETER_LIMIT_TIME = "limit_time";
	/** The parameter name for &quot;Timeout in minutes (-1: no timeout)&quot; */
	public static final String PARAMETER_TIMEOUT = "timeout";
	public static final String PARAMETER_SET_MACRO = "set_iteration_macro";
	public static final String PARAMETER_MACRO_NAME = "macro_name";
	public static final String PARAMETER_MACRO_START_VALUE = "macro_start_value";

	protected PortPairExtender inputExtender = new PortPairExtender("input",
			getInputPorts(), getSubprocess(0).getInnerSources());

	protected PortPairExtender outputExtender = new PortPairExtender("result",
			getSubprocess(0).getInnerSinks(), getOutputPorts());

	private int currentIteration = 0;
	private XMLTools xmlTools = new XMLTools();

	AbstractJobManagerHelper helper = BeanHandler.getInstance()
			.getCurrentBean();

	/** Creates an empty operator chain. */
	public LoopOperator(OperatorDescription description) {
		this(description, "LoopOperator");
	}
	
	/** This constructor allows subclasses to change the subprocess' name. */
	protected LoopOperator(OperatorDescription description,
			String subProcessName) {
		super(description, subProcessName);
		inputExtender.start();
		outputExtender.start();
		getTransformer().addRule(inputExtender.makePassThroughRule());
	}

	@Override
	public void doWork() throws OperatorException {
		try {
			helper = BeanHandler.getInstance().getCurrentBean();
			if (helper == null){
				Helper.out("null helper");
			}
			clearAllInnerSinks();
			inputExtender.passDataThrough();

			XMLExporter ex = new XMLExporter();

			String xml = this.getXML(false);

			IOString procesXML = new IOString();
			xml = (xmlTools.processXML(this, xml, "Dupa", "Loop"));
			Helper.saveToFile("tools", xml);

			String iterationMacroName = null;
			int macroIterationOffset = 0;
			boolean setIterationMacro = getParameterAsBoolean(PARAMETER_SET_MACRO);
			if (setIterationMacro) {
				iterationMacroName = getParameterAsString(PARAMETER_MACRO_NAME);
				macroIterationOffset = getParameterAsInt(PARAMETER_MACRO_START_VALUE);
			}
			this.currentIteration = 0;

			ArrayList<RemoteJob> jobList = new ArrayList<RemoteJob>();
			HashMap<Integer, String> dataKeys = new HashMap<Integer, String>();

			ArrayList<IOObject> inputData = new ArrayList<>();
			for (int i = 0; i < inputExtender.getManagedPairs().size() - 1; i++) {
				IOObject set = (IOObject) inputExtender.getManagedPairs()
						.get(i).getOutputPort().getAnyDataOrNull();
				if (set != null) {
					inputData.add(set);
				} else {

				}
			}
			Integer a = 1;
			for (IOObject io : inputData) {

				String key = helper.storeData(null, io);
				dataKeys.put(a, key);
				a++;
			}
			Helper.out("Keys size" + dataKeys.size());
			// Iterator<PortPair> inputPortIterator =
			// inputPortPairExtender.getManagedPairs().iterator();
			Integer portIndex = 0;
			// while (inputPortIterator.hasNext()){
			// PortPair portPair = inputPortIterator.next();
			// if (portPair.getInputPort().isConnected()){
			// IOObject io = portPair.getInputPort().getAnyDataOrNull();
			// if (io!=null){
			// String key = Helper.masterOperator.storeData(null, io);
			// dataKeys.put(key, value)
			// }
			// }
			// }
			ArrayList<RemoteJob> jobs = new ArrayList<RemoteJob>();
			while (getIteration() < getParameterAsInt(PARAMETER_ITERATIONS)) {
				if (setIterationMacro) {
					String iterationString = Integer.toString(currentIteration
							+ macroIterationOffset);
					getProcess().getMacroHandler().addMacro(iterationMacroName,
							iterationString);
				}
				getLogger()
						.fine("Starting iteration " + (currentIteration + 1));

				MacroHandler macroHandler = getRoot().getProcess()
						.getMacroHandler();
				Iterator<String> macrosIterator = macroHandler
						.getDefinedMacroNames();
				HashMap<String, String> macros = new HashMap<String, String>();
				while (macrosIterator.hasNext()) {
					String macroKey = macrosIterator.next();
					macros.put(macroKey, macroHandler.getMacro(macroKey));
				}
				RemoteJob rj = helper.createJob(xml, dataKeys, macros);

				Helper.saveToFile("PROCESSLOOP_", xml);
				jobs.add(rj);
				// Helper.masterOperator.addJob(rj);
				inApplyLoop();
				getLogger().fine(
						"Completed job creation " + (currentIteration + 1));
				currentIteration++;
			}
			// currentIteration=0;
			HashMap<Integer, ArrayList<IOObject>> toOutput = new HashMap<Integer, ArrayList<IOObject>>();
			try {

				Helper.out("jobs.size()=" + jobs.size());

				List<Future<String>> resultKeys;
				ExecutorService exec = helper.getExecutorService();

				resultKeys = exec.invokeAll(jobs);

				if (resultKeys != null) {
					Helper.out("resultKeys size: " + resultKeys.size());
					if (resultKeys.size() > 0) {
						Integer node = 0;
						for (Future<String> nodeResult : resultKeys) {
							String keys = nodeResult.get();
							if (keys != null) {
								String[] values = keys.split(";");
								Integer portNo = 0;
								Helper.out("Job result = [" + keys + "]");
								for (String value : values) {
									Helper.out("retrive: [" + ((String) value)
											+ "]");
									IOObject io = helper.retriveResult(value);

									if (toOutput.get(portNo) == null) {
										toOutput.put(portNo,
												new ArrayList<IOObject>());
									}
									if (io != null) {
										toOutput.get(portNo).add(io);
										portNo++;
									} else {
										Helper.out("IO IS NULL!");
									}
								}
							}
						}
					}
				}
				// try {
				//
				// } catch (Exception e) {
				// for (int i : toOutput.keySet()) {
				// IOObjectCollection<IOObject> ioc = new
				// IOObjectCollection<IOObject>(
				// toOutput.get(i));
				// outputExtender.getManagedPairs().get(i + 1).getOutputPort()
				// .deliver(ioc);
				// }
				// Helper.out("second option");
				// }

				// ArrayList<HashMap<Integer, IOObject>> result =
				// Helper.masterOperator.processResponseToArray(resultKeys);
				// Helper.masterOperator.toOutputArray(result,
				// outputExtender);//
				// resultKeys.size()
				// List<IOObject> result =new ArrayList<IOObject>();
				// try {
				// Helper.out("Jobs to compute: " + jobs.size());
				// ExecutorService exec = Helper.masterOperator
				// .getExecutorService();
				// // resultKeys = exec.invokeAll(jobs);
				// returnValue =
				// Helper.masterOperator.processResponse(exec.invokeAll(jobs));
				//
				// Helper.out("return " + returnValue.size());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			Helper.out("toOutput:" + toOutput.size());
			for (int i : toOutput.keySet()) {

				IOObjectCollection<IOObject> ioc = new IOObjectCollection<IOObject>(
						toOutput.get(i));
				OutputPort o = outputExtender.getManagedPairs().get(i)
						.getOutputPort();
				Helper.out("Output Name: " + o.getName());
				o.deliver(ioc);
			}
			Helper.out("first option");

			Helper.out("Loop doWork(); finished");
		} catch (Exception exc) {
			throw new OperatorException("Something new", exc);
		}
	}

	protected int getIteration() {
		return currentIteration;
	}

	boolean shouldStop(IOContainer unused) throws OperatorException {
		int timeOut = getParameterAsInt(PARAMETER_TIMEOUT);
		long stoptime = Long.MAX_VALUE;
		if (getParameterAsBoolean(PARAMETER_LIMIT_TIME)) {
			stoptime = System.currentTimeMillis() + 60L * 1000 * timeOut;
			if ((stoptime >= 0) && (System.currentTimeMillis() > stoptime)) {
				getLogger().info("Timeout reached");
				return true;
			}
		}
		return getIteration() >= getParameterAsInt(PARAMETER_ITERATIONS);
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> types = super.getParameterTypes();
		ParameterType type = new ParameterTypeInt(PARAMETER_ITERATIONS,
				"Number of iterations", 0, Integer.MAX_VALUE, 1);
		type.setExpert(false);
		types.add(type);
		type = new ParameterTypeBoolean(
				PARAMETER_LIMIT_TIME,
				"If checked, the loop will be aborted at last after a specified time.",
				false);
		types.add(type);
		type = new ParameterTypeInt(PARAMETER_TIMEOUT, "Timeout in minutes", 1,
				Integer.MAX_VALUE, 1);
		type.registerDependencyCondition(new BooleanParameterCondition(this,
				PARAMETER_LIMIT_TIME, true, true));
		type.setExpert(true);
		types.add(type);
		type = new ParameterTypeBoolean(
				PARAMETER_SET_MACRO,
				"Selects if in each iteration a macro with the current iteration number is set.",
				false, true);
		types.add(type);
		type = new ParameterTypeString(PARAMETER_MACRO_NAME,
				"The name of the iteration macro.", "iteration", true);
		type.registerDependencyCondition(new BooleanParameterCondition(this,
				PARAMETER_SET_MACRO, true, true));
		types.add(type);
		type = new ParameterTypeInt(
				PARAMETER_MACRO_START_VALUE,
				"The number which is set for the macro in the first iteration.",
				Integer.MIN_VALUE, Integer.MAX_VALUE, 1, true);
		type.registerDependencyCondition(new BooleanParameterCondition(this,
				PARAMETER_SET_MACRO, true, true));
		types.add(type);

		return types;
	}

}
