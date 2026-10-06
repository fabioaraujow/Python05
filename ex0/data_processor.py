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

    def ingest(self, data: Any) -> None:
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

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
        if isinstance(data, str):
            self.storage.append(data)
        else:
            for current in data:
                self.storage.append(current)


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===")
    print("Testing Numeric Processor...")
    np = NumericProcessor()
    print(f" Trying to validate input '42': {np.validate(42)}")
    print(f" Trying to validate input 'Hello': {np.validate('Hello')}")
    print(" Test invalid ingestion of string 'foo' without prior validation:")
    try:
        np.ingest("foo")
    except ValueError as err:
        print(f" Got exception: {err}")
    nb_list = [1, 2, 3, 4, 5]
    print(f" Processing data: {nb_list}")
    if np.validate(nb_list):
        np.ingest(nb_list)
    for i in range(3):
        rank, data = np.output()
        print(f" Numeric value {rank}: {data}")
