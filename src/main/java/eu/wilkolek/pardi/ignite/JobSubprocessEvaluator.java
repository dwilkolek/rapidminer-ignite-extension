package eu.wilkolek.pardi.ignite;

import java.util.List;

import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeInt;

public class JobSubprocessEvaluator extends JobSubprocess {

    public JobSubprocessEvaluator(OperatorDescription description) {
		super(description);
		// TODO Auto-generated constructor stub
	}

	@Override
    public List<ParameterType> getParameterTypes() {
        List<ParameterType> types = super.getParameterTypes();
        ParameterType type = new ParameterTypeInt(PARAMETER_SELECT_WHICH, "Indicates which inner operator should be currently employed by this operator on the input objects.", 1, Integer.MAX_VALUE, 1);
        type.setExpert(false);
        types.add(type);
        return types;
    }
	
}
