package eu.wilkolek.pardi.operator.rapidminer;

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

import eu.wilkolek.pardi.types.rapidminer.RemoteJob;
import eu.wilkolek.pardi.util.AbstractJobManagerHelper;
import eu.wilkolek.pardi.util.BeanHandler;
import eu.wilkolek.pardi.util.Helper;
import eu.wilkolek.pardi.util.XMLTools;
import eu.wilkolek.pardi.util.ignite.IgniteJobManagerHelper;

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
		// getTransformer().addRule(new MDTransformationRule() {
		// @Override
		// public void transformMD() {
		// int operatorIndex = -1;
		// try {
		// operatorIndex = getParameterAsInt(PARAMETER_SELECT_WHICH) - 1;
		// for (int i = 0; i < getNumberOfSubprocesses(); i++) {
		// if (i != operatorIndex) { // skip selected and transform
		// // last, so it overrides
		// // everything
		// getSubprocess(i).transformMetaData();
		// }
		// }
		// if ((operatorIndex >= 0)
		// && (operatorIndex < getNumberOfSubprocesses())) {
		// getSubprocess(operatorIndex).transformMetaData();
		// }
		// } catch (Exception e) {
		//
		// }
		//
		// }
		// });
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
			Iterator<String> macrosIterator = macroHandler
					.getDefinedMacroNames();
			HashMap<String, String> macros = new HashMap<String, String>();
			while (macrosIterator.hasNext()) {
				String macroKey = macrosIterator.next();
				macros.put(macroKey, macroHandler.getMacro(macroKey));
			}
			
			for (int i = 0; i < getSubprocesses().size(); i++) {
//				com.rapidminer.Process procForSubprocess = new com.rapidminer.Process(
//						new String(xml));
//				Helper.saveToFile("subprocess_pe_"+i, procForSubprocess
//						.getRootOperator().getXML(false));
//				for (int subNo = 0; subNo < procForSubprocess.getRootOperator()
//						.getSubprocesses().size(); subNo++) {
//					if (i != subNo) {
//						Helper.out("removing subprocess "+subNo+" for "+i+" job");
//						procForSubprocess.getRootOperator().removeSubprocess(
//								subNo);
//					}
//				}
//				Helper.saveToFile("subprocess_"+i, procForSubprocess
//						.getRootOperator().getXML(false));
				String xml = xmlTools.processXML(this, this.getXML(false), "Dupa",
						"Select Subprocess",i);
				RemoteJob job = helper.createJob(xml, dataKeys, macros);

				jobList.add(job);
			}

			HashMap<Integer, ArrayList<IOObject>> toOutput = new HashMap<Integer, ArrayList<IOObject>>();

			Helper.out("jobs.size()=" + jobList.size());

			List<Future<String>> resultKeys;
			ExecutorService exec = helper.getExecutorService();

			resultKeys = exec.invokeAll(jobList);

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

			Helper.out("toOutput:" + toOutput.size());
			for (int i : toOutput.keySet()) {

				IOObjectCollection<IOObject> ioc = new IOObjectCollection<IOObject>(
						toOutput.get(i));

				OutputPort o = getOutputPorts().getPortByIndex(i);
				Helper.out("Output Name: " + o.getName());
				o.deliver(ioc);
			}
			Helper.out("first option");

			Helper.out("Loop doWork(); finished");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
