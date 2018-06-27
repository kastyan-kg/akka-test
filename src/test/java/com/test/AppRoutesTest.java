package com.test;

import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.http.impl.engine.rendering.BodyPartRenderer;
import akka.http.javadsl.model.*;
import akka.http.javadsl.testkit.JUnitRouteTest;
import akka.http.javadsl.testkit.TestRoute;
import akka.http.javadsl.testkit.TestRouteResult;
import com.test.actors.FileLoaderActor;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class AppRoutesTest extends JUnitRouteTest {
    private TestRoute testRoute;
    private ActorSystem actorSystem = ActorSystem.create("Test");

    @Before
    public void setUp(){
        testRoute = testRoute(new AppRoutes(actorSystem.actorOf(Props.create(FileLoaderActor.class))).routes());
        loadTestData();
    }

    @Test
    public void fileUploadTest(){
        loadTestData().assertStatusCode(StatusCodes.OK);
    }

    @Test
    public void fileStatisticsTest(){
        testRoute.run(HttpRequest.GET("/statistics/primes.csv/2"))
                .assertEntity("{\"entries\":2,\"percentageToTotal\":\"50.0%\"}");
    }

    @Test
    public void downloadFileTest(){
        testRoute.run(HttpRequest.GET("/download/primes.csv/2"))
                .assertEntity("5,5\n3,5\n");
    }

    @Test
    public void allStatisticsTest(){
        testRoute.run(HttpRequest.GET("/statistics/"))
                .assertEntity("{\"primes.csv\":{\"recordsRead\":4,\"statisticByFileName\":" +
                        "{\"2\":{\"entries\":2,\"percentageToTotal\":\"50.0%\"}," +
                        "\"7\":{\"entries\":1,\"percentageToTotal\":\"25.0%\"}," +
                        "\"29\":{\"entries\":1,\"percentageToTotal\":\"25.0%\"}}}}");
    }

    private TestRouteResult loadTestData(){
        Map<String, String> filenameMapping = new HashMap<>();
        filenameMapping.put("filename", "primes.csv");

        Multipart.FormData multipartForm =
                Multiparts.createStrictFormDataFromParts(
                        Multiparts.createFormDataBodyPartStrict("file",
                                HttpEntities.create(ContentTypes.TEXT_PLAIN_UTF8,
                                        "2,3,5\n2,5,5\n7,11,13,17,23\n29,31,37\n"), filenameMapping));


        return testRoute.run(HttpRequest.POST("/load")
                .withEntity(multipartForm.toEntity(BodyPartRenderer.randomBoundaryWithDefaults())))
                .assertStatusCode(StatusCodes.OK);
    }
}
