package eu.wilkolek.types.rapidminer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import javax.swing.Icon;

import com.rapidminer.operator.Annotations;
import com.rapidminer.operator.IOObject;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.ResultObject;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.ProcessingStep;
import com.rapidminer.tools.LoggingHandler;

public class IOObjectStub implements ResultObject {

	public String key;
	
	public IOObjectStub(String key){
		this.key = key;
	}
	// 0 - TaskID, 1- iteration, 2 - nodeId, 3 - outputPortNumber
	public String getCacheKey(){
		return  key;
	}
	
	@Override
	public void setSource(String sourceName) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getSource() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void appendOperatorToHistory(Operator operator, OutputPort port) {
		// TODO Auto-generated method stub

	}

	@Override
	public List<ProcessingStep> getProcessingHistory() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IOObject copy() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void write(OutputStream out) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public LoggingHandler getLog() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setLoggingHandler(LoggingHandler loggingHandler) {
		// TODO Auto-generated method stub

	}

	@Override
	public Annotations getAnnotations() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public String toResultString() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public Icon getResultIcon() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public List getActions() {
		// TODO Auto-generated method stub
		return null;
	}

}