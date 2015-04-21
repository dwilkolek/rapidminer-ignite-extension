package eu.wilkolek.pardi.operator.rapidminer;

import java.util.List;

import com.rapidminer.operator.ExecutionUnit;
import com.rapidminer.operator.OperatorChain;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.UserError;
import com.rapidminer.operator.meta.OperatorSelector;
import com.rapidminer.operator.ports.InputPorts;
import com.rapidminer.operator.ports.MultiInputPortPairExtender;
import com.rapidminer.operator.ports.MultiOutputPortPairExtender;
import com.rapidminer.operator.ports.OutputPorts;
import com.rapidminer.operator.ports.metadata.MDTransformationRule;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeInt;

public class SelectSubprocessOperator extends OperatorChain {

    /** The parameter name for &quot;Indicates if the operator which inner operator should be used&quot;. */
    public static final String PARAMETER_SELECT_WHICH = "select_which";

    private final MultiOutputPortPairExtender inputExtender = new MultiOutputPortPairExtender("input", getInputPorts(), new OutputPorts[] { getSubprocess(0).getInnerSources(), getSubprocess(1).getInnerSources() });
    private final MultiInputPortPairExtender outputExtender = new MultiInputPortPairExtender("output", getOutputPorts(), new InputPorts[] { getSubprocess(0).getInnerSinks(), getSubprocess(1).getInnerSinks() });

    public SelectSubprocessOperator(OperatorDescription description) {
        super(description, "Selection 1", "Selection 2");
        inputExtender.start();
        outputExtender.start();
        getTransformer().addRule(inputExtender.makePassThroughRule());
        getTransformer().addRule(new MDTransformationRule() {
            @Override
            public void transformMD() {
                int operatorIndex = -1;
                try {
                    operatorIndex = getParameterAsInt(PARAMETER_SELECT_WHICH) - 1;
                    for (int i = 0; i < getNumberOfSubprocesses(); i++) {
                        if (i != operatorIndex) { // skip selected and transform last, so it overrides everything
                            getSubprocess(i).transformMetaData();
                        }
                    }
                    if ((operatorIndex >= 0) && (operatorIndex < getNumberOfSubprocesses())) {
                        getSubprocess(operatorIndex).transformMetaData();
                    }
                } catch (Exception e) {

                }

            }
        });
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
            getSubprocess(i).setName("Selection " + (i+1));
        }
    }


    @Override
    public void doWork() throws OperatorException {
        int operatorIndex = getParameterAsInt(PARAMETER_SELECT_WHICH);
        if ((operatorIndex < 1) || (operatorIndex > getNumberOfSubprocesses())) {
            throw new UserError(this, 207, new Object[] { operatorIndex, PARAMETER_SELECT_WHICH, "must be between 1 and the number of inner operators."} );
        }

        inputExtender.passDataThrough();
        getSubprocess(operatorIndex - 1).execute();
        outputExtender.passDataThrough(operatorIndex - 1);
    }

    @Override
    public List<ParameterType> getParameterTypes() {
        List<ParameterType> types = super.getParameterTypes();
        ParameterType type = new ParameterTypeInt(PARAMETER_SELECT_WHICH, "Indicates which inner operator should be currently employed by this operator on the input objects.", 1, Integer.MAX_VALUE, 1);
        type.setExpert(false);
      //  types.add(type);
        return types;
    }
	
}
