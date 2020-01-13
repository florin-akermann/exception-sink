import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestInbox;
import akka.actor.typed.javadsl.Adapter;
import akka.japi.Pair;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.TestKit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import static akka.stream.Materializer.createMaterializer;
import static org.slf4j.LoggerFactory.*;


class MapWithExceptionHandlerTest {

    private static final Logger log = getLogger(MapWithExceptionHandlerTest.class);
    private ActorSystem system;
    private List<String> input;
    private Materializer materializer;

    @BeforeEach
    void setup() {
        system = ActorSystem.create();
        materializer = Materializer.createMaterializer(system);
        input = Arrays.asList("one", "two", "3", "four");
    }

    @AfterEach
    void tearDown() {
        system.terminate();
    }

    private String pickyFunction(String s) {
        if (s.matches(".*\\d.*")) {
            throw new RuntimeException("the picky function does not like numbers in a string");
        } else {
            return s;
        }
    }

    @Test
    void testStreamWithoutExceptionHandlerMap() {
        CompletionStage<List<String>> resultPromise = Source.from(input)
                .map(s -> pickyFunction(s))
                .toMat(Sink.seq(), Keep.right())
                .run(materializer);
        Assertions.assertThrows(ExecutionException.class, () -> resultPromise.toCompletableFuture().get());
    }

    @Test
    void testStreamWithExceptionHandlerMap() throws ExecutionException, InterruptedException {
        CompletionStage<List<String>> resultPromise = Source.from(input)
                .via(MapWithExceptionHandler.mapWithHandler(s -> pickyFunction(s), exceptionHandler()))
                .toMat(Sink.seq(), Keep.right())
                .run(materializer);

        List<String> expected = Arrays.asList("one","two","four");
        List<String> actual = resultPromise.toCompletableFuture().get();

        Assertions.assertEquals(expected, actual);
    }

    private MapWithExceptionHandler.ExceptionHandler<String> exceptionHandler() {
        return (value, exception) -> {
            log.warn("exception {}:", exception, exception);
        };
    }

    @Test
    void testStreamWithExceptionSinkMap() throws ExecutionException, InterruptedException {

        TestInbox<String> testInbox = TestInbox.create();
        Sink<String, NotUsed> exceptionSink = Flow.<String>create().
                map(s->{testInbox.getRef().tell(s); return s;})
                .to(Sink.ignore());

        CompletionStage<List<String>> resultPromise = Source.from(input)
                .via(MapWithExceptionSink.map(t -> pickyFunction(t), exceptionSink))
                .toMat(Sink.seq(), Keep.right())
                .run(materializer);

        List<String> actual = resultPromise.toCompletableFuture().get();
        List<String> expected = Arrays.asList("one", "two", "four");
        Assertions.assertEquals(expected, actual);

        List<String> expectedFaultyElements = Arrays.asList("3");
        List<String> actualFaultyElements = testInbox.getAllReceived();
        Assertions.assertEquals(expectedFaultyElements, actualFaultyElements);


    }

}