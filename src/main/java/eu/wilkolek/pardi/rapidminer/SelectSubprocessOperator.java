package eu.wilkolek.pardi.rapidminer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.rapidminer.MacroHandler;
import com.rapidminer.operator.ExecutionUnit;
import com.rapidminer.operator.IOObject;
import com.rapidminer.operator.IOObjectCollection;
import com.rapidminer.operator.OperatorChain;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.UserError;
import com.rapidminer.operator.meta.OperatorSelector;
import com.rapidminer.operator.ports.InputPorts;
import com.rapidminer.operator.ports.MultiInputPortPairExtender;
import com.rapidminer.operator.ports.MultiOutputPortPairExtender;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.OutputPorts;
import com.rapidminer.operator.ports.metadata.MDTransformationRule;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeInt;

import eu.wilkolek.pardi.ignite.IgniteJobManagerHelper;
import eu.wilkolek.pardi.types.AbstractJobManagerHelper;
import eu.wilkolek.pardi.types.RemoteJob;
import eu.wilkolek.pardi.util.BeanHandler;
import eu.wilkolek.pardi.util.Helper;
import eu.wilkolek.pardi.util.XMLTools;

public class SelectSubprocessOperator extends OperatorChain {

	/**
	 * The parameter name for &quot;Indicates if the operator which inner
	 * operator should be used&quot;.
	 */
	public static final String PARAMETER_SELECT_WHICH = "select_which";

	private final MultiOutputPortPairExtender inputExtender = new MultiOutputPortPairExtender(
			"input", getInputPorts(), new OutputPorts[] {
					getSubprocess(0).getInnerSources(),
					getSubprocess(1).getInnerSources() });
	private final MultiInputPortPairExtender outputExtender = new MultiInputPortPairExtender(
			"result", getOutputPorts(), new InputPorts[] {
					getSubprocess(0).getInnerSinks(),
					getSubprocess(1).getInnerSinks() });

	AbstractJobManagerHelper helper = BeanHandler.getInstance()
			.getCurrentBean();

	public SelectSubprocessOperator(OperatorDescription description) {
		super(description, "Selection 1", "Selection 2");
		inputExtender.start();
		outputExtender.start();
		getTransformer().addRule(inputExtender.makePassThroughRule());
		
		getTransformer().addRule(outputExtender.makePassThroughRule());
	}

	@Override
	public boolean areSubprocessesExtendable() {
		return true;
	}

	@Override
	protected ExecutionUnit createSubprocess(int index) {
		return new ExecutionUnit(this, "Selection");
	}

	@Override
	public ExecutionUnit addSubprocess(int index) {
		ExecutionUnit newProcess = super.addSubprocess(index);
		inputExtender.addMultiPorts(newProcess.getInnerSources(), index);
		outputExtender.addMultiPorts(newProcess.getInnerSinks(), index);
		normalizeSubprocessNames();
		return newProcess;
	}

	@Override
	public ExecutionUnit removeSubprocess(int index) {
		ExecutionUnit oldProcess = super.removeSubprocess(index);
		inputExtender.removeMultiPorts(index);
		outputExtender.removeMultiPorts(index);
		normalizeSubprocessNames();
		return oldProcess;
	}

	private void normalizeSubprocessNames() {
		for (int i = 0; i < getNumberOfSubprocesses(); i++) {
			getSubprocess(i).setName("Selection " + (i + 1));
		}
	}

	@Override
	public void doWork() throws OperatorException {
		try {
			helper = BeanHandler.getInstance().getCurrentBean();
			inputExtender.passDataThrough();
			ArrayList<RemoteJob> jobList = new ArrayList<RemoteJob>();
			HashMap<Integer, String> dataKeys = new HashMap<Integer, String>();
			ArrayList<IOObject> inputData = new ArrayList<>();
			for (int i = 0; i < getInputPorts().getNumberOfPorts() - 1; i++) {
				IOObject set = (IOObject) getInputPorts().getPortByIndex(i)
						.getAnyDataOrNull();
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
			XMLTools xmlTools = new XMLTools();
			
			MacroHandler macroHandler = getRoot().getProcess()
					.getMacroHandler();
			HashMap<String, String> macros = helper.exportMacro(macroHandler);
			
			for (int i = 0; i < getSubprocesses().size(); i++) {

				String xml = xmlTools.processXML(this, this.getXML(false), "",
						"Select Subprocess",i);
				RemoteJob job = helper.createJob(xml, dataKeys, macros);

				jobList.add(job);
			}

			HashMap<Integer, ArrayList<IOObject>> toOutput = new HashMap<Integer, ArrayList<IOObject>>();

			Helper.out("Jobs created: "+jobList.size());
			List<Future<String>> resultKeys;
			ExecutorService exec = helper.getExecutorService();

			resultKeys = exec.invokeAll(jobList);

			toOutput = helper.resultKeysToOutput(resultKeys);


			for (int i : toOutput.keySet()) {

				IOObjectCollection<IOObject> ioc = new IOObjectCollection<IOObject>(
						toOutput.get(i));

				OutputPort o = getOutputPorts().getPortByIndex(i);
				o.deliver(ioc);
			}

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

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> types = super.getParameterTypes();
		// ParameterType type = new ParameterTypeInt(PARAMETER_SELECT_WHICH,
		// "Indicates which inner operator should be currently employed by this operator on the input objects.",
		// 1, Integer.MAX_VALUE, 1);
		// type.setExpert(false);
		// types.add(type);
		return types;
	}

}
