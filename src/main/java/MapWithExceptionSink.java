import akka.NotUsed;
import akka.japi.function.Function;
import akka.stream.*;
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

    public final Inlet<I> in = Inlet.create("MapWithExceptionSink.in");
    public final Outlet<O0> out = Outlet.create("MapWithExceptionSink.out");
    public final Outlet<O1> outAlt = Outlet.create("MapWithExceptionSink.exceptionOut");

    private final FanOutShape2<I, O0, O1> shape = new FanOutShape2<>(in, out, outAlt);

    @Override
    @SuppressWarnings("unchecked")
    public GraphStageLogicWithLogging createLogic(Attributes inheritedAttributes) {
        return new GraphStageLogicWithLogging(shape) {
            {
                setHandler(
                        in,
                        new AbstractInHandler() {
                            @Override
                            public void onPush() {
                                I tempIn = grab(in);
                                O0 result = null;
                                try {
                                    result = f.apply(tempIn);
                                } catch (Exception e) {
                                    sendExceptionToExceptionSink((O1) tempIn, e);
                                }
                                if (result != null) {
                                    push(out, result);
                                }
                            }

                            private void sendExceptionToExceptionSink(O1 tempIn, Exception e) {
                                if (isAvailable(shape.out1())) {
                                    push(shape.out1(), tempIn);
                                    log().info("captured exception: {}, sent to exception sink", e);
                                } else {
                                    log().info("captured exception: {}, not sent to exception sink", e);
                                }
                            }
                        });

                setHandler(out, () -> pullIfNotPulled());

                setHandler(outAlt, () -> pullIfNotPulled());
            }

            private void pullIfNotPulled() {
                if (!hasBeenPulled(in)) pull(in);
            }
        };
    }

    @Override
    public FanOutShape2<I, O0, O1> shape() {
        return shape;
    }

    public static <I, O0, M> Graph<FlowShape<I, O0>, NotUsed> map(Function<I, O0> fun, Sink<I, M> excSink) {
        return GraphDSL.create(builder -> {
            try {
                final FanOutShape2<I, O0, I> excSinkMap = builder.add(new MapWithExceptionSink<>(fun));
                builder.to(builder.add(excSink)).fromOutlet(excSinkMap.out1());
                return FlowShape.of(excSinkMap.in(), excSinkMap.out0());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
    }

}

