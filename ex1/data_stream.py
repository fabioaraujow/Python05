#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.storage: list[str] = []
        self.rank = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    @abstractmethod
    def ingest(self, data: Any) -> None:
        ...

    def output(self) -> tuple[int, str]:
        data = self.storage.pop(0)
        rank = self.rank
        self.rank += 1
        return (rank, data)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            if not data:
                return False
            for current in data:
                if not isinstance(current, (int, float)):
                    return False
            return True
        else:
            return isinstance(data, (int, float))

    def ingest(self, data: int | float | list) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        if isinstance(data, (int, float)):
            self.storage.append(str(data))
        else:
            for current in data:
                self.storage.append(str(current))


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            if not data:
                return False
            for current in data:
                if not isinstance(current, str):
                    return False
            return True
        else:
            return isinstance(data, str)

    def ingest(self, data: str | list) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
        if isinstance(data, str):
            self.storage.append(data)
        else:
            for current in data:
                self.storage.append(current)


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            if not data:
                return False
            for current in data:
                if (not isinstance(current, dict)
                        or 'log_level' not in current
                        or 'log_message' not in current
                        or not isinstance(current['log_level'], str)
                        or not isinstance(current['log_message'], str)):
                    return False
            return True
        else:
            return (isinstance(data, dict)
                    and 'log_level' in data
                    and 'log_message' in data
                    and isinstance(data['log_level'], str)
                    and isinstance(data['log_message'], str))

    def ingest(self, data: dict[str, str] | list) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")
        if isinstance(data, dict):
            self.storage.append(f"{data['log_level']}: {data['log_message']}")
        else:
            for current in data:
                self.storage.append(
                        f"{current['log_level']}: {current['log_message']}")


class DataStream():
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        if not self.processors:
            print("No processor found, no data")
        if not stream:
            print("Empty List!")
        else:
            for current in stream:
                for proc in self.processors:
                    if proc.validate(current):
                        proc.ingest(current)
                        break
                else:
                    print(f"DataStream error - "
                          f"Can't process element in stream: {current}")

    def print_processors_stats(self) -> None:
        if not self.processors:
            print("== DataStream statistics ==")
            print("No processor found, no data")
        else:
            print("== DataStream statistics ==")
            for proc in self.processors:
                processed = proc.rank + len(proc.storage)
                name = type(proc).__name__
                pr_name = ""
                for char in name:
                    if char.isupper() and pr_name:
                        pr_name += " "
                    pr_name += char
                print(f"{pr_name}: total {processed} items processed, "
                      f"remaining {len(proc.storage)} on processor")


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")
    print()
    print("Initialize Data Stream...")
    ds = DataStream()
    sort_list = [
        'Hello World',
        [3.14, -1, 2.71],
        [{'log_level': 'WARNING',
         'log_message': 'Telnet access! Use ssh insted'},
         {'log_level': 'INFO', 'log_message': 'User wil is connected'}],
        42, ['Hi', 'five']]
    ds.print_processors_stats()
    print()
    print("Registering Numeric Processor")
    print()
    np = NumericProcessor()
    ds.register_processor(np)
    print(f"Send first batch of data on stream: {sort_list}")
    ds.process_stream(sort_list)
    ds.print_processors_stats()
    print()
    print("Registering other data processors")
    print("Send the same batch again")
    tp = TextProcessor()
    lp = LogProcessor()
    ds.register_processor(tp)
    ds.register_processor(lp)
    ds.process_stream(sort_list)
    ds.print_processors_stats()
    print()
    print("Consume some elements from the data processors: "
          "Numeric 3, Text 2, Log 1")
    np.output()
    np.output()
    np.output()
    tp.output()
    tp.output()
    lp.output()
    ds.print_processors_stats()
