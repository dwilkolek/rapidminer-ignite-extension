package eu.wilkolek.types.rapidminer;

import java.util.concurrent.ExecutorService;

import com.rapidminer.operator.OperatorChain;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.ports.PortPairExtender;

public abstract class AbstractTaskManager extends OperatorChain {

	public static final String FLUSH_CACHE = "Flush cache";
	public static final String DELAYED_EXECUTION = "Delayed execution";

	protected PortPairExtender inputExtender = new PortPairExtender("iin",
			getInputPorts(), getSubprocess(0).getInnerSources());

	protected PortPairExtender outputExtender = new PortPairExtender("iou",
			getSubprocess(0).getInnerSinks(), getOutputPorts());

	
	protected AbstractTaskManager(OperatorDescription description,
			String subProcessName) {
		super(description, subProcessName);
	}

	public AbstractTaskManager(OperatorDescription description) {
		this(description, "Ignite holder");
	}
	
	abstract public ExecutorService getExecutorService();
	
	
}
