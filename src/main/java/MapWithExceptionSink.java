import akka.NotUsed;
import akka.japi.function.Function;
import akka.stream.*;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Sink;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogicWithLogging;

public class MapWithExceptionSink<I, O0, O1 extends I> extends GraphStage<FanOutShape2<I, O0, O1>> {

    private final Function<I, O0> f;

    public MapWithExceptionSink(Function<I, O0> f) {
        this.f = f;
    }

    public final Inlet<I> in = Inlet.<I>create("MapWithExceptionSink.in");
    public final Outlet<O0> out = Outlet.<O0>create("MapWithExceptionSink.out");
    public final Outlet<O1> outException = Outlet.<O1>create("MapWithExceptionSink.exceptionOut");

    private final FanOutShape2<I, O0, O1> shape = new FanOutShape2<>(in, out, outException);

    @Override
    @SuppressWarnings("unchecked")
    public GraphStageLogicWithLogging createLogic(Attributes inheritedAttributes) {
        return new GraphStageLogicWithLogging(shape) {

            {
                setHandler(in,
                        () -> {
                            I tempIn = grab(in);
                            O0 result = null;
                            try {
                                result = f.apply(tempIn);
                            } catch (Exception e) {
                                push(outException, (O1) tempIn);
                            }
                            if (result != null) {
                                push(out, result);
                            }
                        }
                );

                setHandler(out, () -> {
                    if (isAvailable(outException)) {
                        pull(in);
                    }
                });

                setHandler(outException, () -> {
                    if (isAvailable(out)) {
                        pull(in);
                    }
                });
            }

        };
    }

    @Override
    public FanOutShape2<I, O0, O1> shape() {
        return shape;
    }

    public static <I, O0, M> Flow<I, O0, NotUsed> map(Function<I, O0> fun, Sink<I, M> excSink) {
        return Flow.fromGraph(GraphDSL.create(builder -> {
            final FanOutShape2<I, O0, I> excSinkMap = builder.add(new MapWithExceptionSink<>(fun));
            builder.to(builder.add(excSink)).fromOutlet(excSinkMap.out1());
            return FlowShape.of(excSinkMap.in(), excSinkMap.out0());
        }));
    }
}

