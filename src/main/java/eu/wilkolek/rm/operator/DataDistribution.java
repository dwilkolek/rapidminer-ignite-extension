package eu.wilkolek.rm.operator;

import org.apache.ignite.Ignite;
import org.apache.ignite.resources.IgniteInstanceResource;

import com.rapidminer.operator.OperatorChain;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.PortPairExtender;

public class DataDistribution extends OperatorChain {

	
	//many subprocess input - out ioc output;
	
	
	public DataDistribution(OperatorDescription description) {
		super(description, "DataDistribution");
		outputExtender.start();
	}
	
	
	public DataDistribution(OperatorDescription description,
			String[] subprocessNames) {
		super(description, subprocessNames);
		outputExtender.start();
	}

	
	protected PortPairExtender outputExtender = new PortPairExtender("dou",
			getSubprocess(0).getInnerSinks(), getOutputPorts());
	
	
	@Override
	public void doWork() throws OperatorException {	
		
		clearAllInnerSinks();
		super.doWork();
		outputExtender.passDataThrough();
	}

	
}
