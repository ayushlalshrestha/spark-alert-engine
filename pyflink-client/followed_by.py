import logging as log

from pyflink.common.time import Duration
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import (TimestampAssigner,
                                               WatermarkStrategy)
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor


class AlertFunction(KeyedProcessFunction):
    def __init__(self):
        self.denied_events = None
        self.timer_state = None

    def open(self, runtime_context):
        self.denied_events = runtime_context.get_list_state(
            ListStateDescriptor("denied_events", Types.LONG())
        )
        self.timer_state = runtime_context.get_state(
            ValueStateDescriptor("timer_state", Types.LONG())
        )

    def process_element(self, value, ctx):
        current_time = ctx.timestamp()

        if value["action"] == "denied":
            self.denied_events.add(current_time)

            # Set a timer if not already set
            if self.timer_state.value() is None:
                new_timer = current_time + 60000  # 10 minutes in milliseconds
                self.timer_state.update(new_timer)
                ctx.timer_service().register_event_time_timer(new_timer)
        elif value["action"] == "accessed":
            denied_count = sum(1 for _ in self.denied_events.get())
            self.denied_events.clear()
            self.timer_state.clear()
            if denied_count >= 3:
                yield f"ALERT: User {value['username']} accessed after {denied_count} denials"

    def on_timer(self, timestamp, ctx):
        print("Time to clear all the state")
        if self.denied_events.get():
            self.denied_events.clear()
        if self.timer_state.value() is not None:
            self.timer_state.clear()
        return []


def normalize_log(event):
    return event["username"], event["timestamp"]


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
            Types.ROW_NAMED(
                ["timestamp", "username", "action"],
                [Types.LONG(), Types.STRING(), Types.STRING()],
            )
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
        # WatermarkStrategy.for_monotonous_timestamps()
        # .with_timestamp_assigner(MyTimestampAssigner())
        # .with_idleness(Duration.of_seconds(30))
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_minutes(1)
        ).with_timestamp_assigner(MyTimestampAssigner())
    )

    # Apply windowing and process function
    kafka_watermarked_stream = env.add_source(
        kafka_consumer
    ).assign_timestamps_and_watermarks(watermark_strategy)

    alerts = kafka_watermarked_stream.key_by(lambda x: x["username"]).process(
        AlertFunction()
    )
    alerts.print()
    env.execute("User Access Alert Job")


if __name__ == "__main__":
    main()
