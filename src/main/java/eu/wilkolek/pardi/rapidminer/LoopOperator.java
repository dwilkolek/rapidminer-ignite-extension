package eu.wilkolek.pardi.rapidminer;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.rapidminer.MacroHandler;
import com.rapidminer.operator.IOContainer;
import com.rapidminer.operator.IOObject;
import com.rapidminer.operator.IOObjectCollection;
import com.rapidminer.operator.OperatorChain;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.PortPairExtender;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.parameter.conditions.BooleanParameterCondition;

import eu.wilkolek.pardi.types.AbstractJobManagerHelper;
import eu.wilkolek.pardi.types.RemoteJob;
import eu.wilkolek.pardi.util.BeanHandler;
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
		Long start = System.currentTimeMillis();
		try {
			helper = BeanHandler.getInstance().getCurrentBean();
			if (helper == null) {
				throw new OperatorException("No helper instance!");
			}
			clearAllInnerSinks();
			inputExtender.passDataThrough();

			String xml = this.getXML(false);

			xml = (xmlTools.processXML(this, xml, "Dupa", "Loop"));
//			Helper.saveToFile("tools", xml);

			String iterationMacroName = null;
			int macroIterationOffset = 0;
			boolean setIterationMacro = getParameterAsBoolean(PARAMETER_SET_MACRO);
			if (setIterationMacro) {
				iterationMacroName = getParameterAsString(PARAMETER_MACRO_NAME);
				macroIterationOffset = getParameterAsInt(PARAMETER_MACRO_START_VALUE);
			}
			this.currentIteration = 0;

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

				HashMap<String, String> macros = helper
						.exportMacro(macroHandler);

				RemoteJob rj = helper.createJob(xml, dataKeys, macros);

				jobs.add(rj);

				inApplyLoop();
				getLogger().fine(
						"Completed job creation " + (currentIteration + 1));
				currentIteration++;
			}
			
			HashMap<Integer, ArrayList<IOObject>> toOutput = new HashMap<Integer, ArrayList<IOObject>>();

			List<Future<String>> resultKeys;
			ExecutorService exec = helper.getExecutorService();
			
			resultKeys = exec.invokeAll(jobs);
			exec.shutdown();
			exec.awaitTermination(5, TimeUnit.HOURS);
			toOutput = helper.resultKeysToOutput(resultKeys);
			//Helper.out("Jobs executed successfully: "+resultKeys.size()+"/"+jobs.size());
			for (int i : toOutput.keySet()) {

				IOObjectCollection<IOObject> ioc = new IOObjectCollection<IOObject>(
						toOutput.get(i));
				OutputPort o = outputExtender.getManagedPairs().get(i)
						.getOutputPort();

				o.deliver(ioc);
			}
			Long end = System.currentTimeMillis();
			Long diff = end - start;
			
			Helper.out("Loop("+resultKeys.size()+"/"+jobs.size()+") in: " + diff.toString()+" s");

		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new OperatorException("Interupted Exception", e);
		} catch (ExecutionException e) {
			e.printStackTrace();
			
			throw new OperatorException("Execution Exception", e);
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
