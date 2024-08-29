import datetime
import time
from typing import Iterable

import pytz
from pyflink.common import Time, Types
from pyflink.common.time import Duration
from pyflink.common.watermark_strategy import (TimestampAssigner,
                                               WatermarkStrategy)
from pyflink.datastream import (ProcessWindowFunction,
                                StreamExecutionEnvironment, TimeCharacteristic)
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor
from pyflink.datastream.window import (SlidingEventTimeWindows, TimeWindow,
                                       TumblingEventTimeWindows)


def normalize_log(event):
    return event["username"], event["timestamp"]


def get_readable_time(time_ms):
    kathmandu_tz = pytz.timezone("Asia/Kathmandu")
    return (
        datetime.datetime.fromtimestamp(time_ms / 1000.0)
        .astimezone(kathmandu_tz)
        .strftime("%H:%M:%S")
    )


def print_event(event):
    username = event["username"]
    now_ms_readable = get_readable_time(time.time() * 1000)
    log_ms_readable = get_readable_time(event["timestamp"])
    print(f"received @{now_ms_readable} => {username}, {log_ms_readable}")


# Define the WatermarkStrategy
class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        return int(value["timestamp"])


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    # env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    # env.set_parallelism(1)

    # Define the JSON deserialization schema
    deserialization_schema = (
        JsonRowDeserializationSchema.builder()
        .type_info(
            Types.ROW_NAMED(["timestamp", "username"], [Types.LONG(), Types.STRING()])
        )
        .build()
    )

    # Define Kafka consumer
    kafka_consumer = FlinkKafkaConsumer(
        topics="logs",
        deserialization_schema=deserialization_schema,
        properties={"bootstrap.servers": "kafka:9092", "group.id": "flink_group_1"},
    )

    watermark_strategy = (
        WatermarkStrategy.for_monotonous_timestamps()
        .with_timestamp_assigner(MyTimestampAssigner())
        .with_idleness(Duration.of_seconds(30))
    )

    # Apply windowing and process function
    kafka_watermarked_stream = env.add_source(
        kafka_consumer
    ).assign_timestamps_and_watermarks(watermark_strategy)

    # Key by username
    keyed_stream = kafka_watermarked_stream.key_by(
        lambda x: x["username"], key_type=Types.STRING()
    )
    keyed_stream.map(lambda event: print_event(event))
    keyed_stream.process(AlertFunction()).print()

    # windowed_stream = (
    #     keyed_stream.window(
    #         SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(10))
    #     )
    #     # .window(TumblingEventTimeWindows.of(Time.seconds(30)))
    #     .process(
    #         CountProcessWindowFunction(),
    #         Types.TUPLE([Types.STRING(), Types.STRING(), Types.STRING(), Types.INT()]),
    #     )
    # )

    # Print the alert
    # windowed_stream.print()

    env.execute()


class AlertFunction(KeyedProcessFunction):
    def __init__(self):
        self.state = None

    def open(self, runtime_context):
        state_descriptor = ListStateDescriptor(
            "events",
            Types.ROW_NAMED(["timestamp", "username"], [Types.LONG(), Types.STRING()]),
        )
        self.state = runtime_context.get_list_state(state_descriptor)

    def process_element(self, value, ctx):
        self.state.add(value)
        ctx.timer_service().register_event_time_timer(
            value["timestamp"] + 60000
        )  # 60 seconds

    def on_timer(self, timestamp, ctx, out):
        events = list(self.state.get())
        window_start = timestamp - 60000
        recent_events = [e for e in events if e["timestamp"] > window_start]

        if len(recent_events) > 2:
            out.collect(
                (
                    ctx.get_current_key(),
                    get_readable_time(window_start),
                    get_readable_time(timestamp),
                    len(recent_events),
                )
            )

        self.state.clear()
        for event in recent_events:
            self.state.add(event)


class AlertFunction(KeyedProcessFunction):
    def __init__(self):
        self.state = None

    def open(self, runtime_context):
        state_descriptor = ListStateDescriptor(
            "events",
            Types.ROW_NAMED(["timestamp", "username"], [Types.LONG(), Types.STRING()]),
        )
        self.state = runtime_context.get_list_state(state_descriptor)

    def process_element(self, value, ctx):
        self.state.add(value)
        ctx.timer_service().register_event_time_timer(
            value["timestamp"] + 60000
        )  # 60 seconds

    def on_timer(self, timestamp, context):
        events = list(self.state.get())
        window_start = timestamp - 60000
        recent_events = [e for e in events if e["timestamp"] > window_start]

        if len(recent_events) > 2:
            context.output(
                (
                    context.get_current_key(),
                    get_readable_time(window_start),
                    get_readable_time(timestamp),
                    len(recent_events),
                )
            )

        self.state.clear()
        for event in recent_events:
            self.state.add(event)


if __name__ == "__main__":
    main()
