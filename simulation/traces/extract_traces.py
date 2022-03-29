#!/usr/bin/env python3

import sys
import argparse
import logging
import datetime
import calendar
import csv
import pandas as pd

pd.options.mode.chained_assignment = None  # default='warn'
import yaml

data_file_darshan = None
data_file_composite = None
month = 0


def parse_args():
    global data_file_darshan
    global data_file_composite
    global month

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--darshan", help="Path of the Darshan data file")
    parser.add_argument("-c", "--composite", help="Path of the Composite data file")
    parser.add_argument("-m", "--month", help="Specify a specific month to extract ([1,12])")
    parser.add_argument("-v", "--verbose", help="Display debug information", action="store_true")

    args = parser.parse_args()

    if not args.darshan or not args.composite:
        parser.print_usage()
        print("Error: arguments --darshan (-d) and --composite (-c) are mandatory!")
        sys.exit(1)
    else:
        data_file_darshan = args.darshan
        data_file_composite = args.composite

    if args.month:
        try:
            month = int(args.month)
            if month < 1 or month > 12:
                raise
        except:
            parser.print_usage()
            print("Error: arguments --month (-m) must be an integer in the range [1,12]!")
            sys.exit(1)

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format="[D] %(message)s")


# Convert a string-formated date into a datetime object
def str_to_date(date_str):
    return datetime.datetime.strptime(str(date_str), "%Y%m%d")


# Convert a string-formated timestamp into a timestamp object
def str_to_timestamp(timestamp_str):
    try:
        t = datetime.datetime.strptime(str(timestamp_str), "%Y-%m-%d %H:%M:%S")
        return t
    except:
        return timestamp_str


def main(argv):
    parse_args()

    traces_darshan = pd.read_csv(data_file_darshan, low_memory=False)
    traces_composite = pd.read_csv(data_file_composite, low_memory=False)

    # Left joint the two dataframes
    traces = traces_darshan.merge(traces_composite, on="COBALT_JOBID", how="left")

    # Filter data: keep a subset of useful columns and remove empty rows
    filtered_cols = traces.columns.to_list()[11:186]
    non_empty_rows = traces[filtered_cols].apply(pd.to_numeric).any(axis="columns").to_list()
    subset_traces = traces[non_empty_rows]

    # Filter and keep relevant data
    workload_traces = subset_traces[
        [
            "COBALT_JOBID",
            "RUN_DATE_ID",
            "USER_ID",
            "EXE_NAME_GENID",
            "QUEUED_TIMESTAMP",
            "START_TIMESTAMP",
            "END_TIMESTAMP",
            "NODES_USED",
            "CORES_USED",
            "NPROCS",
            "RUN_TIME",
            "TOTAL_POSIX_OPENS",
            "TOTAL_POSIX_READS",
            "TOTAL_POSIX_WRITES",
            "TOTAL_POSIX_BYTES_READ",
            "TOTAL_POSIX_BYTES_WRITTEN",
            "TOTAL_POSIX_F_READ_TIME",
            "TOTAL_POSIX_F_WRITE_TIME",
            "TOTAL_POSIX_F_META_TIME",
            "TOTAL_MPIIO_INDEP_READS",
            "TOTAL_MPIIO_INDEP_WRITES",
            "TOTAL_MPIIO_COLL_READS",
            "TOTAL_MPIIO_COLL_WRITES",
            "TOTAL_MPIIO_NB_READS",
            "TOTAL_MPIIO_NB_WRITES",
            "TOTAL_MPIIO_BYTES_READ",
            "TOTAL_MPIIO_BYTES_WRITTEN",
            "TOTAL_MPIIO_F_READ_TIME",
            "TOTAL_MPIIO_F_WRITE_TIME",
            "TOTAL_MPIIO_F_META_TIME",
            "TOTAL_STDIO_OPENS",
            "TOTAL_STDIO_READS",
            "TOTAL_STDIO_WRITES",
            "TOTAL_STDIO_BYTES_READ",
            "TOTAL_STDIO_BYTES_WRITTEN",
            "TOTAL_STDIO_F_READ_TIME",
            "TOTAL_STDIO_F_WRITE_TIME",
            "TOTAL_STDIO_F_META_TIME",
        ]
    ]

    # replace the string dates by the datetime format
    workload_traces.loc[:, ("RUN_DATE_ID")] = workload_traces.RUN_DATE_ID.apply(str_to_date)
    workload_traces.loc[:, ("QUEUED_TIMESTAMP")] = workload_traces.QUEUED_TIMESTAMP.apply(
        str_to_timestamp
    )
    workload_traces.loc[:, ("START_TIMESTAMP")] = workload_traces.START_TIMESTAMP.apply(
        str_to_timestamp
    )
    workload_traces.loc[:, ("END_TIMESTAMP")] = workload_traces.END_TIMESTAMP.apply(
        str_to_timestamp
    )

    if month != 0:
        current_traces = workload_traces[(workload_traces["RUN_DATE_ID"].dt.month == month)]
    else:
        current_traces = workload_traces

    io_data = current_traces[
        [
            "COBALT_JOBID",
            "NPROCS",
            "QUEUED_TIMESTAMP",
            "START_TIMESTAMP",
            "END_TIMESTAMP",
            "NODES_USED",
            "CORES_USED",
            "TOTAL_MPIIO_BYTES_WRITTEN",
            "TOTAL_MPIIO_BYTES_READ",
            "TOTAL_MPIIO_F_WRITE_TIME",
            "RUN_TIME",
        ]
    ]

    # Add custom metrics
    io_data.loc[:, ("RatioWriteTimeRunTime")] = (
        io_data.TOTAL_MPIIO_F_WRITE_TIME / io_data.NPROCS
    ) / io_data.RUN_TIME
    io_data.loc[:, ("WAITING_TIME")] = io_data.START_TIMESTAMP - io_data.QUEUED_TIMESTAMP

    print(str(io_data.shape[0]) + " x " + str(io_data.shape[1]))
    # Select I/O intensive jobs
    #   - 10% of the time spent writing data or
    #   - At least 10GB read or written
    io_jobs = io_data.loc[
        (io_data["RatioWriteTimeRunTime"] >= 0.1)
        | (io_data["TOTAL_MPIIO_BYTES_WRITTEN"] >= 10000000000)
        | (io_data["TOTAL_MPIIO_BYTES_READ"] >= 10000000000)
    ]

    print(str(io_jobs.shape[0]) + " x " + str(io_jobs.shape[1]))

    d_io_jobs = {"jobs": []}

    for index, row in io_jobs.iterrows():
        d_io_jobs["jobs"].append(
            {
                "id": int(row["COBALT_JOBID"]),
                "MPIprocs": int(row["NPROCS"]),
                "submissionTime": row["QUEUED_TIMESTAMP"].strftime("%Y-%m-%d %H:%M:%S"),
                "startTime": row["START_TIMESTAMP"].strftime("%Y-%m-%d %H:%M:%S"),
                "endTime": row["END_TIMESTAMP"].strftime("%Y-%m-%d %H:%M:%S"),
                "waitingTime": str(row["WAITING_TIME"]),
                "nodesUsed": int(row["NODES_USED"]),
                "coresUsed": int(row["CORES_USED"]),
                "writtenBytes": int(row["TOTAL_MPIIO_BYTES_WRITTEN"]),
                "readBytes": int(row["TOTAL_MPIIO_BYTES_READ"]),
                "runTime": int(row["RUN_TIME"]),
            }
        )

    with open("IOJobs" + calendar.month_abbr[month] + ".yml", "w") as yamlfile:
        data = yaml.dump(d_io_jobs, yamlfile)
        print(
            calendar.month_name[month]
            + " IO intensive jobs extracted successfully to IOJobs"
            + calendar.month_abbr[month]
            + ".yml"
        )


if __name__ == "__main__":
    main(sys.argv[1:])
