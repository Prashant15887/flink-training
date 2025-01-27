/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.training.exercises.longrides;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
/*import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;*/
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
/*import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;*/
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
//import org.apache.flink.training.exercises.common.utils.MissingSolutionException;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.time.Duration;
import java.time.Instant;
//import java.util.OptionalLong;

/**
 * The "Long Ride Alerts" exercise.
 *
 * <p>The goal for this exercise is to emit the rideIds for taxi rides with a duration of more than
 * two hours. You should assume that TaxiRide events can be lost, but there are no duplicates.
 *
 * <p>You should eventually clear any state you create.
 */
public class LongRidesExercise {
    private final SourceFunction<TaxiRide> source;
    private final SinkFunction<Long> sink;
    private static final OutputTag<TaxiRide> lateRides = new OutputTag<TaxiRide>("lateRides") {};

    /** Creates a job using the source and sink provided. */
    public LongRidesExercise(SourceFunction<TaxiRide> source, SinkFunction<Long> sink) {
        this.source = source;
        this.sink = sink;
    }

    /**
     * Creates and executes the long rides pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(source);

        // the WatermarkStrategy specifies how to extract timestamps and generate watermarks
        WatermarkStrategy<TaxiRide> watermarkStrategy =
                WatermarkStrategy.<TaxiRide>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                        .withTimestampAssigner(
                                (ride, streamRecordTimestamp) -> ride.getEventTimeMillis());

        // create the pipeline
        SingleOutputStreamOperator longRides = rides.assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(ride -> ride.rideId)
                .process(new AlertFunction());

        longRides.addSink(sink);
        longRides.getSideOutput(lateRides).print();

        // execute the pipeline and return the result
        return env.execute("Long Taxi Rides");
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        LongRidesExercise job =
                new LongRidesExercise(new TaxiRideGenerator(), new PrintSinkFunction<>());

        job.execute();
    }

    @VisibleForTesting
    public static class AlertFunction extends KeyedProcessFunction<Long, TaxiRide, Long> {

        private transient ValueState<Instant> rideStartTime;
        private transient ValueState<Instant> rideEndTime;

        @Override
        public void open(Configuration config) throws Exception {
            ValueStateDescriptor<Instant> startTimeDesc = new ValueStateDescriptor<>("Start time of ride", Instant.class);
            ValueStateDescriptor<Instant> endTimeDesc = new ValueStateDescriptor<>("End time of ride", Instant.class);
            rideStartTime = getRuntimeContext().getState(startTimeDesc);
            rideEndTime = getRuntimeContext().getState(endTimeDesc);
        }

        @Override
        public void processElement(TaxiRide ride, Context context, Collector<Long> out)
                throws Exception {
            TimerService timerService = context.timerService();

            if (ride.eventTime.toEpochMilli() > timerService.currentWatermark()) {
                if (ride.isStart) {
                    if (rideEndTime.value() != null) {
                        if (Duration.between(ride.eventTime, rideEndTime.value()).compareTo(Duration.ofHours(2)) > 0) {
                            out.collect(context.getCurrentKey());
                        }
                        rideEndTime.clear();
                    }
                    else {
                        timerService.registerEventTimeTimer(ride.eventTime.plusSeconds(120 * 60).toEpochMilli());
                        rideStartTime.update(ride.eventTime);
                    }
                }
                else {
                    if (rideStartTime.value() != null) {
                        if (Duration.between(rideStartTime.value(), ride.eventTime).compareTo(Duration.ofHours(2)) > 0) {
                            out.collect(context.getCurrentKey());
                        }
                        timerService.deleteEventTimeTimer(rideStartTime.value().plusSeconds(120 * 60).toEpochMilli());
                        rideStartTime.clear();
                    }
                    else {
                        rideEndTime.update(ride.eventTime);
                    }
                }
            }
            else {
                context.output(lateRides, ride);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<Long> out)
                throws Exception {
            out.collect(context.getCurrentKey());
            rideStartTime.clear();
            rideEndTime.clear();
        }
    }
}
