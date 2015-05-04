package eu.wilkolek.pardi.ignite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;

import eu.wilkolek.pardi.types.AbstractJobManager;
import eu.wilkolek.pardi.types.RemoteJob;
import eu.wilkolek.pardi.util.BeanHandler;
import eu.wilkolek.pardi.util.Config;
import eu.wilkolek.pardi.util.Helper;

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

		if (getParameterAsBoolean(INIT_REMOTE_NODES) && needInit) {

			RemoteJob job = new RemoteJob(xmlInitRemote,
					new HashMap<Integer, String>(),
					new HashMap<String, String>(),
					IgniteJobManagerHelper.class.getName());
			ArrayList<RemoteJob> jobList = new ArrayList<RemoteJob>();
			for (int jn = 0; jn < helper.nodeCount(); jn++) {
				jobList.add(job);
			}
//			Helper.out("Init Remote Jobs : " + jobList.size());
			try {
				helper.getExecutorService().invokeAll(jobList);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			needInit = false;
		}

//		Helper.out("version: [" + Config.version + "]");
		inputExtender.passDataThrough();
		Helper.flushCache = getParameterAsBoolean(FLUSH_CACHE);
		// Helper.masterOperator = this;
		try {
//			Helper.out("super.doWork()");
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
