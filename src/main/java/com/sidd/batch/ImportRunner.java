package com.sidd.batch;

import org.apache.camel.ProducerTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class ImportRunner implements ApplicationRunner {
	
	@Autowired
	private ProducerTemplate producerTemplate;

	@Override
	public void run(final ApplicationArguments args) throws Exception {
		System.out.println("Starting task to split files...");

		// 1. Start routes (or startAllRoutes())
		producerTemplate.getCamelContext().getRoute("fileSplitterRoute");

		// 2. Send message to start the direct route
		producerTemplate.sendBody("direct:start", "");

	}

}
