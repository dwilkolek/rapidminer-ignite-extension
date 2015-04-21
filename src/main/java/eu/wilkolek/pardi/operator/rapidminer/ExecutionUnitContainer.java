package eu.wilkolek.pardi.operator.rapidminer;

import com.rapidminer.operator.OperatorChain;
import com.rapidminer.operator.OperatorDescription;

public class ExecutionUnitContainer extends OperatorChain {

	public ExecutionUnitContainer(OperatorDescription description,
			String[] subprocessNames) {
		super(description, subprocessNames);
		// TODO Auto-generated constructor stub
	}
	
	public ExecutionUnitContainer(OperatorDescription description) {
		super(description,"op");
		// TODO Auto-generated constructor stub
	}
	
}
