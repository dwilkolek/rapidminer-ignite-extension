package eu.wilkolek.rm.util;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.rapidminer.Process;
import com.rapidminer.operator.ports.PortException;
import com.rapidminer.tools.XMLException;

import eu.wilkolek.rm.operator.Task;

public class ProcessXML {
	public String processXML(
			Task task, 
			final String xmlOriginal,
			final String xmlSink, 
			final Integer quantity, 
			final String macroName, 
			final String variable) {

		String xml;
		StringBuilder builder = new StringBuilder();
		StringWriter writer = new StringWriter();
		xml = task.cloneOperator("TaskEvaluator", true).getXML(false);
		
		
		if (!macroName.isEmpty() && !variable.isEmpty()){
			Configurator.zapisDoPliku("Przed", xml);
			xml = xml.replace("%{"+macroName+"}", variable);
			Configurator.zapisDoPliku("Po", xml);
		}
		
		// writer.flush();
		// xml = writer.toString();
		xml = xml.replace("ignite_extension:Task",
				"ignite_extension:TaskEvaluator");
		xml = xml.replace("\"Task\"", "\"TaskEvaluator\"");
		String xml2Append = "";

		Boolean startAppending = false;
		Scanner scanner = new Scanner(xml);
		String[] lines = xml.split(System.getProperty("line.separator"));
		Integer lineNumber = 0;
		while (scanner.hasNext()) {

			String line = scanner.nextLine();

			if (line.contains("class=\"ignite_extension:TaskEvaluator\"")) {
				startAppending = true;
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
		// builder.append(xml2Append);
		// builder.append(addOutput(xmlSink));
		builder.append(Configurator.PROLOG);
		// builder.append("\n<<<BODY START>>>\n\n");
		builder.append(xml2Append);
		builder.append(addOutput(xmlSink));
		// builder.append("\n<<<BODY END>>>\n\n");
		builder.append(Configurator.EPILOG);
		// builder.append(Configurator.EPILOG);

		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }

		// String[] lines = xml.split(System.getProperty("line.separator"));

		// builder.append(Configurator.PROLOG);
		// Scanner scanner = new Scanner(xml);
		// Integer ln = 0;
		//
		// while (scanner.hasNextLine()) {
		// String line = scanner.nextLine();
		// ln++;
		// if (ln > 7 && ln < lines.length) {
		// builder.append(line + "\n");
		// }
		// }
		// // if (quantity > 0) {
		// // Configurator.out("Adding " + quantity + " inputs");
		// // builder.append(addInput(quantity));
		// // }
		// // builder.append(addOutput(xmlSink));
		// builder.append(Configurator.EPILOG);
		// scanner.close();

		try {
			Process proc = new Process(builder.toString());

			for (String opnames : proc.getAllOperatorNames()) {
				Configurator.out("opName: " + opnames);
			}
			Configurator.out("Subprocess innersources porst : "
					+ proc.getRootOperator().getSubprocess(0).getInnerSources()
							.getNumberOfPorts());
			//TODO what if there are many TaskEvaluators? Nullpointer on proc.getOperator("TaskEvaluator")
			for (int port = 1; port < proc.getOperator("TaskEvaluator")
					.getInputPorts().getNumberOfPorts() + 1; port++) {
				try {

					proc.getRootOperator().getSubprocess(0).getInnerSources()
							.createPort("input " + port);
					Configurator.out("creating port " + "input " + port);
				} catch (PortException e) {
					Configurator.out("creating port " + "input " + port
							+ " ! WAS EXISTING !");
				}
			}

			Configurator.out("WIll connect "
					+ proc.getOperator("TaskEvaluator").getInputPorts()
							.getNumberOfPorts());

			for (int port = 0; port < proc.getOperator("TaskEvaluator")
					.getInputPorts().getNumberOfPorts() - 1; port++) {
				Configurator.out("Connecting port #" + port);

				if (!proc.getRootOperator().getSubprocess(0).getInnerSources()
						.getPortByIndex(port).isConnected()
						&& !proc.getOperator("TaskEvaluator").getInputPorts()
								.getPortByIndex(port).isConnected()) {
					proc.getRootOperator()
							.getSubprocess(0)
							.getInnerSources()
							.getPortByIndex(port)
							.connectTo(
									proc.getOperator("TaskEvaluator")
											.getInputPorts()
											.getPortByIndex(port));

					Configurator.out("Stan (TASK): "
							+ proc.getOperator("TaskEvaluator").getInputPorts()
									.getNumberOfConnectedPorts()
							+ "/"
							+ (proc.getOperator("TaskEvaluator").getInputPorts()
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
			Configurator.out("returning xml from PROCESS");
			if (Configurator.DEBUG) {
				Configurator.zapisDoPliku("test", proc.getRootOperator()
						.getXML(false));
			}
			return proc.getRootOperator().getXML(false);
		} catch (IOException | XMLException e) {
			e.printStackTrace();
		}

		if (Configurator.DEBUG) {
			Configurator.zapisDoPliku("test", builder.toString());
		}
		Configurator.out("returning xml from TASK");
		return builder.toString();
	}

	private String addInput(Integer quantity) {
		// <connect from_port="input 2" to_op="TaskEvaluator" to_port="gin 2"/>
		final String in = "<connect from_port=\"input {id}\" to_op=\"TaskEvaluator\" to_port=\"gin {id}\"/>";
		String inputSink = "";
		for (int i = 1; i <= quantity; i++) {
			inputSink += in.replace("{id}", i + "");
		}
		return inputSink;
	}

	private String addOutput(final String xmlOryginal) {
		final String ou = "<connect from_op=\"TaskEvaluator\" from_port=\"gou {sinkID}\" to_port=\"result {sinkID}\"/>\n"
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
}
