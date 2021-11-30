package com.japharr.starter_camel;

import io.vertx.camel.CamelBridge;
import io.vertx.camel.CamelBridgeOptions;
import io.vertx.camel.InboundMapping;
import io.vertx.camel.OutboundMapping;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.parsetools.RecordParser;
import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultMessage;

import java.io.File;

public class MainVerticle extends AbstractVerticle {

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    CamelContext camel = new DefaultCamelContext();

    Endpoint endpoint = camel.getEndpoint("file:/Users/japharr/Projects/vertx/");

    camel.addRoutes(new RouteBuilder() {
      @Override
      public void configure() throws Exception {
        from(endpoint)
          .process(new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception {
              String actFileName = exchange.getIn().getHeader(Exchange.FILE_NAME).toString();

              File file = exchange.getIn().getBody(File.class);

              System.out.println("FileName: " + file.getAbsolutePath());
              vertx.fileSystem().open(file.getAbsolutePath(), new OpenOptions(), result -> {
                if (result.succeeded()) {
                  AsyncFile asyncFile = result.result();
                  RecordParser recordParser = RecordParser.newDelimited("\n", bufferedLine -> {
                    System.out.println("bufferedLine = " + bufferedLine);
                  });

                  asyncFile.handler(recordParser)
                    .endHandler(v -> {
                      asyncFile.close();
                      System.out.println("Done");
                    });

                } else {
                  // Something went wrong!
                  System.out.println("An error occurred!");
                }
              });
            }
          })
          .to("file:/Users/japharr/Projects/maven/");
      }
    });

    vertx.eventBus().consumer("eventbus-address", message -> {
      System.out.println("ANNOUNCE >> " + message.body());
    });

    vertx.eventBus().consumer("errors", message -> {
      System.out.println("ERROR >> " + message.body());
    });

    camel.start();

    CamelBridge.create(vertx,
      new CamelBridgeOptions(camel)
        .addInboundMapping(InboundMapping.fromCamel("direct:stuff").toVertx("eventbus-address"))
        //.addOutboundMapping(OutboundMapping.fromVertx("eventbus-address").toCamel("stream:out"))
    ).start();

    vertx.createHttpServer().requestHandler(req -> {
      req.response()
        .putHeader("content-type", "text/plain")
        .end("Hello from Vert.x!");
    }).listen(8888, http -> {
      if (http.succeeded()) {
        startPromise.complete();
        System.out.println("HTTP server started on port 8888");
      } else {
        startPromise.fail(http.cause());
      }
    });
  }
}
