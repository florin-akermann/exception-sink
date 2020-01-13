import akka.NotUsed;
import akka.japi.function.Function;
import akka.stream.*;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Sink;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogicWithLogging;


public class MapWithExceptionHandler<I, O> extends GraphStage<FlowShape<I, O>> {

    private final Function<I, O> function;
    private final ExceptionHandler<I> handler;

    private MapWithExceptionHandler(Function<I, O> function, ExceptionHandler<I> handler) {
        this.function = function;
        this.handler = handler;
    }

    public final Inlet<I> in = Inlet.<I>create("MapWithExceptionHandler.in");
    public final Outlet<O> out = Outlet.<O>create("MapWithExceptionHandler.out");

    private final FlowShape<I, O> shape = FlowShape.apply(in, out);

    @Override
    public GraphStageLogicWithLogging createLogic(Attributes inheritedAttributes) {
        return new GraphStageLogicWithLogging(shape) {
            {
                setHandler(
                        in,
                        new AbstractInHandler() {
                            @Override
                            public void onPush() {
                                I value = grab(in);
                                O result = null;
                                try {
                                    result = function.apply(value);
                                } catch (Throwable e) {
                                    handler.handle(value, e);
                                }
                                if (result != null) {
                                    push(out, result);
                                } else if (!hasBeenPulled(in)) {
                                    pull(in);
                                }
                            }
                        });
                setHandler(
                        out,
                        new AbstractOutHandler() {
                            @Override
                            public void onPull() {
                                if (!hasBeenPulled(in)) {
                                    pull(in);
                                }
                            }
                        });
            }
        };
    }

    @Override
    public FlowShape<I, O> shape() {
        return shape;
    }

    public static <I, O> MapWithExceptionHandler<I, O> mapWithHandler(Function<I, O> function, ExceptionHandler<I> handler) {
        return new MapWithExceptionHandler<>(function, handler);
    }


    public interface ExceptionHandler<I> {
        void handle(I value, Throwable exception);
    }
}