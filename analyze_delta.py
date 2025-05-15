import os
import json
from pprint import pprint
from typing import Sequence, Dict, Any
from datetime import datetime, timedelta


def get_json_files(path: str):
    files = os.listdir(path)
    # print(files)

    return [file for file in files if "json" in file and "crc" not in file]


def parse_delta_file(path: str):
    data = []
    with open(path) as f:
        for line in f:
            data.append(json.loads(line))
    return data


def get_partitions_updated(
    operations: Sequence[Dict[Any, Any]], datetime: datetime.date
):
    partitions_updated = set()
    for operation in operations:
        for op, data in operation.items():
            if op in ("commitInfo", "metaData", "protocol"):
                continue

            if op == "add":
                timestamp_column = "modificationTime"
            elif op == "remove":
                timestamp_column = "deletionTimestamp"

            modification_time = datetime.utcfromtimestamp(data[timestamp_column] / 1000)

            for partition_column, partition_date in data["partitionValues"].items():
                partitions_updated.add(partition_date)

    return partitions_updated


def main():
    table_path = "tmp/delta-table/_delta_log"
    files = get_json_files(table_path)
    data = []
    for file in files:
        data += parse_delta_file(f"{table_path}/{file}")

    pprint(data)

    partitions_updated = get_partitions_updated(
        data, datetime.now() - timedelta(days=1)
    )
    print(partitions_updated)


if __name__ == "__main__":
    main()
