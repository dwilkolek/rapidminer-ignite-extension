package eu.wilkolek.pardi.operator.java;

import java.util.List;

import com.rapidminer.operator.IOObject;
import com.rapidminer.operator.OperatorChain;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.PortPairExtender;
import com.rapidminer.operator.ports.metadata.SubprocessTransformRule;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.ParameterTypeString;


/*
 * the same data for each node
 * each data 
 * or each 
 */








import eu.wilkolek.pardi.util.Config;
import eu.wilkolek.pardi.util.Helper;

public class JobEvaluator extends OperatorChain {
	
	protected PortPairExtender inputExtender = new PortPairExtender("gin",
			getInputPorts(), getSubprocess(0).getInnerSources());
	protected PortPairExtender outputExtender = new PortPairExtender("gou",
			getSubprocess(0).getInnerSinks(), getOutputPorts());

	public static final String PER_NODE= "Per node";
	public static final String PER_SUBPROCESS= "Per subprocess";
	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> types = super.getParameterTypes();
		types.add(new ParameterTypeBoolean(PER_NODE,
				"Load the same data for each node?", Boolean.FALSE));
		types.add(new ParameterTypeBoolean(PER_SUBPROCESS,
				"Load the same data for each node?", Boolean.FALSE));
		return types;
	}
	
		
	public static final String NODE_ID = "node id";
	
	
	/** Creates an empty operator chain. */
	public JobEvaluator(OperatorDescription description) {
		this(description, "Nested Chain");
	}

	/** This constructor allows subclasses to change the subprocess' name. */
	protected JobEvaluator(OperatorDescription description, String subProcessName) {
		super(description, subProcessName);
		inputExtender.start();
		outputExtender.start();
		getTransformer().addRule(inputExtender.makePassThroughRule());
		getTransformer().addRule(new SubprocessTransformRule(getSubprocess(0)));
		getTransformer().addRule(outputExtender.makePassThroughRule());
	}

	@Override
	public void doWork() throws OperatorException {
		

		Helper.out("delivered "+inputExtender.getManagedPairs().size()+"managed inputs");
//		for (int key = 0; key < inputExtender.getManagedPairs().size()-1; key++) { 
//			IOObject ioobj = inputExtender.getManagedPairs().get(key).getOutputPort().getData(IOObject.class);
//		}
		inputExtender.passDataThrough();
		super.doWork();
		outputExtender.passDataThrough();
	}

}