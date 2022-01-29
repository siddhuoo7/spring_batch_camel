package com.sidd.batch.camel;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

/**
 * @author siddarth.ajay
 */

@Component
public class FileSplitterRoute extends RouteBuilder {

	@Override
	public void configure() {
		log.info("Configuring camel routes...");

		String inputFilePath = System.getProperty("user.home") + "\\data\\input\\";// C:\Users\siddarth.ajay\data\input
		String outputFilePath = System.getProperty("user.home") + "\\data\\out\\";
		log.info("Copying data from {} to {}", inputFilePath, outputFilePath);

		from("direct:start")
//		.noAutoStartup()
		.loopDoWhile(body().isNotNull())
				.pollEnrich("file://C:/Users/siddarth.ajay/data/input/?noop=true&recursive=true")
//				.log("${body}")
				.choice()
				.when(body().isNotNull())
				.split()
				.tokenize("\n", 200, false)
				.to("file://" + outputFilePath + "?fileName=${headers.CamelFileName}_" + "${header.CamelSplitIndex}" + ".csv")
				.to("spring-batch:importUserJob")
//				.to("spring-batch:readCSVFilesJob")
				.routeId("fileSplitterRoute")
				.end();

		log.info("Configured camel routes...");
	}
}