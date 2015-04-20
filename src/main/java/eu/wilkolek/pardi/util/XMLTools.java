package eu.wilkolek.pardi.util;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.expression.spel.ast.OpNE;

import com.rapidminer.Process;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.PortException;
import com.rapidminer.tools.XMLException;

import eu.wilkolek.pardi.operator.java.Job;
import eu.wilkolek.pardi.types.rapidminer.IOString;

public class XMLTools {
	
	public static String PROLOG = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?> \n <process version=\"5.3.015\"> \n   <context> \n     <input/> \n     <output/> \n     <macros/> \n   </context> \n   <operator activated=\"true\" class=\"process\" compatibility=\"5.3.015\" expanded=\"true\" name=\"Process\"> \n     <parameter key=\"logverbosity\" value=\"init\"/> \n     <parameter key=\"random_seed\" value=\"2001\"/> \n     <parameter key=\"send_mail\" value=\"never\"/> \n     <parameter key=\"notification_email\" value=\"\"/> \n     <parameter key=\"process_duration_for_mail\" value=\"30\"/> \n     <parameter key=\"encoding\" value=\"SYSTEM\"/> \n     <process expanded=\"true\"> \n ";
	public static String EPILOG = "<portSpacing port=\"source_input 1\" spacing=\"0\"/> \n       <portSpacing port=\"sink_result 1\" spacing=\"0\"/> \n     </process> \n   </operator> \n </process> \n ";

	
	public String processXML(
			Job job, 
			final String xmlOriginal,
			final String xmlSink, 
			final Integer quantity) {
		Helper.saveToFile("before", xmlOriginal);
		String xml;
		StringBuilder builder = new StringBuilder();
		StringWriter writer = new StringWriter();
		xml = job.cloneOperator(Config.JOBEvaluator, true).getXML(false);
		String opName = Config.JOBEvaluator;
		//For jobSubprocess
		xml = xml.replace(	"class=\""+Config.extensionName+":"+Config.JOBSubprocess+"\"",
							"class=\""+Config.extensionName+":"+Config.JOBSubprocessEvaluator+"\"");
//		xml = xml.replace(	"\""+Config.JOBSubprocess+"\"", 
//							"\""+Config.JOBSubprocessEvaluator+"\"");
		//For job
		xml = xml.replace(	"class=\""+Config.extensionName+":"+Config.JOB+"\"",
							"class=\""+Config.extensionName+":"+Config.JOBEvaluator+"\"");
//		xml = xml.replace(	"\""+Config.JOB+"\"", 
//							"\""+Config.JOBEvaluator+"\"");
		
		String xml2Append = "";
		Helper.saveToFile("before1", xml);
		Boolean startAppending = false;
		Scanner scanner = new Scanner(xml);
		String[] lines = xml.split(System.getProperty("line.separator"));
		Integer lineNumber = 0;
		while (scanner.hasNext()) {

			String line = scanner.nextLine();

			if (line.contains("class=\""+Config.extensionName+":"+Config.JOBEvaluator+"\"")) {
				startAppending = true;
				String tmpOpName= getOpNameFromLine(line);
				opName=(tmpOpName!=null ? tmpOpName : Config.JOB);
				// xml2Append += line;
			}
			if (startAppending) {

				if (lines.length - 2 == lineNumber) {

					startAppending = false;

				}

				xml2Append += line + "\n";

			}
			lineNumber++;
		}
		
		builder.append(PROLOG);
		builder.append(xml2Append);
		builder.append(addOutput(xmlSink,opName));
		builder.append(EPILOG);
		
		try {
			Helper.saveToFile("beforeProcB", builder.toString());
			Process proc = new Process(builder.toString());
			Helper.saveToFile("beforeProcA", builder.toString());
			Helper.out("Subprocess innersources ports : "
					+ proc.getRootOperator().getSubprocess(0).getInnerSources()
							.getNumberOfPorts());
			for (Operator o : proc.getAllOperators()){
				Helper.out(o.getName()+"--" + o.getClass().getSimpleName());
			}
			for (int port = 1; port < proc.getOperator(opName)
					.getInputPorts().getNumberOfPorts() + 1; port++) {
				try {

					proc.getRootOperator().getSubprocess(0).getInnerSources()
							.createPort("input " + port);
					Helper.out("creating port " + "input " + port);
				} catch (PortException e) {
					Helper.out("creating port " + "input " + port
							+ " ! WAS EXISTING !");
				}
			}

			Helper.out("WIll connect "
					+ proc.getOperator(opName).getInputPorts()
							.getNumberOfPorts());

			for (int port = 0; port < proc.getOperator(opName)
					.getInputPorts().getNumberOfPorts() - 1; port++) {
				Helper.out("Connecting port #" + port);

				if (!proc.getRootOperator().getSubprocess(0).getInnerSources()
						.getPortByIndex(port).isConnected()
						&& !proc.getOperator(opName).getInputPorts()
								.getPortByIndex(port).isConnected()) {
					proc.getRootOperator()
							.getSubprocess(0)
							.getInnerSources()
							.getPortByIndex(port)
							.connectTo(
									proc.getOperator(opName)
											.getInputPorts()
											.getPortByIndex(port));

					Helper.out("Stan (TASK): "
							+ proc.getOperator(opName).getInputPorts()
									.getNumberOfConnectedPorts()
							+ "/"
							+ (proc.getOperator(opName).getInputPorts()
									.getNumberOfPorts()-1));
				}
			}
			/*
			 * Date cur = new Date();
			 * 
			 * File fileProc = new File("C:/dev/proces/proc_" + cur.getTime()
			 * 
			 * + ".xml"); fileProc.createNewFile(); proc.save(fileProc);
			 */
			Helper.out("returning xml from PROCESS");
			
				Helper.saveToFile("test", proc.getRootOperator()
						.getXML(false));
			
			return proc.getRootOperator().getXML(false);
		} catch (IOException | XMLException e) {
			e.printStackTrace();
		}

			Helper.saveToFile("test", builder.toString());

		Helper.out("returning xml from TASK");
		return builder.toString();
	}


	private String getOpNameFromLine(String line) {
		String pattern = "name=\""+"([0-9A-Za-z\\ \\(\\)]+)"+"\"";
		Pattern opPattern = Pattern.compile(pattern);
		Matcher matcher = opPattern.matcher(line);
		if (matcher.find()){
			String name = matcher.group(0);
			Helper.out("Found name: "+name);
			name = name.replace("\"","").replace("name=","");
			Helper.out("Returning after replacement: "+name);
			return name;
		}
		return null;
	}


	private String addInput(Integer quantity, String opName) {
		// <connect from_port="input 2" to_op="TaskEvaluator" to_port="gin 2"/>
		final String in = "<connect from_port=\"input {id}\" to_op=\""+opName+"\" to_port=\"gin {id}\"/>";
		String inputSink = "";
		for (int i = 1; i <= quantity; i++) {
			inputSink += in.replace("{id}", i + "");
		}
		return inputSink;
	}

	private String addOutput(final String xmlOryginal, String opName) {
		final String ou = "<connect from_op=\""+opName+"\" from_port=\"gou {sinkID}\" to_port=\"result {sinkID}\"/>\n"
				+ "<portSpacing port=\"sink_result {sinkID}\" spacing=\"0\"/>";
		String sinkXML = "";

		String xml = xmlOryginal;

		Pattern SINKS = Pattern.compile("=\"sink_gou ([0-9]+)\"",
				Pattern.MULTILINE);
		Matcher matcher = SINKS.matcher(xml);
		Integer sinkCount = 0;

		while (matcher.find()) {
			sinkCount++;
		}
		// Configurator.out("SINKS COUNT : " + sinkCount);

		for (int i = 1; i < sinkCount; i++) {
			Integer id = i;
			sinkXML += ou.replace("{sinkID}", id.toString());
		}
		// Configurator.out("SINKS RESULT : \n" + sinkXML);

		return sinkXML;
	}
	
	public IOString selectSubproces(int nodeId, final IOString xml) {
		Helper.out("select subprocess ");
		Helper.saveToFile("selectSubprocess"+nodeId, xml.getSource());
		IOString processingText = xml;
		String patternString = "class=\""+Config.extensionName+":"+Config.JOBSubprocessEvaluator+"\"";
		String selected = "";
		// Pattern pattern = Pattern.compile(patternString);
		Helper.out("select subprocess :"+patternString);
		boolean nextLineIsTheOneToProcess = false;	

		String[] lines = processingText.getSource().split(
				System.getProperty("line.separator"));
		for (String line : lines) {

			// Matcher matcher = pattern.matcher(line);

			if (line.contains(patternString)) {
				Helper.out(line);
				// Configurator.out("FOUND LINE WITH SubprocessDeclaration\n"+line);
				line += System.getProperty("line.separator")
						+ "<parameter key=\"select_which\" value=\"" + nodeId
						+ "\" />" + System.getProperty("line.separator");
				// Configurator.out("\n\nAFTER MODS: \n" +line+"\n\n\n");
			}

			selected += line + "\n";
		}
		Helper.saveToFile("selectSubprocessReturn"+nodeId, xml.getSource());
		return new IOString(selected);
	}
	
	public String getOpNameForJob(String xml){
		String nameP = "name=\"([A-Za-z0-9 _-]+)\"";
		String classP = "class=\""+Config.extensionName+":"+Config.JOBEvaluator+"\"";
		Pattern namePattern = Pattern.compile(nameP);
		Pattern classPattern = Pattern.compile(classP);
		Matcher matcher = classPattern.matcher(xml);
		Helper.out("getOpNameForJob with classP:"+classP);
		String[] lines = xml.split(System.getProperty("line.separator"));
		for (String line : lines){
			if (line.contains(classP)){
				Helper.out("Found line: "+line);
				return getOpNameFromLine(line);
			}
		}
		return null;
	}
}
