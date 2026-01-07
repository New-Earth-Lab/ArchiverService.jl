#!/usr/bin/env julia

using ArgParse
using Dates
using ArchiverService

function parse_commandline()
    s = ArgParseSettings(
        description = "A tool to dump SPIDERS raw data into FITS cubes. The data will be chunked in 60s increments, with each data stream placed into separate FITS cubes. Each FITS cube has a second HDU with a table of all metadata.",
        prog = "spiders2fits"
    )

    @add_arg_table s begin
        "--start-time"
            help = "Start time for data extraction"
            required = true
            dest_name = "start_time"
        "--end-time"
            help = "End time for data extraction"
            required = true
            dest_name = "end_time"
        "--date"
            help = "Reference date for times (default: today). Use ISO format (e.g., 2023-09-07T12:34:56)"
            dest_name = "ref_date"
            default = nothing
        "--archive-date"
            help = "UTC Data Folder to search in (not necessarily the date/time that you want to query)"
            dest_name = "archive_date"
            default = nothing
        "--tz"
            help = "Local timezone offset from UTC in hours (e.g., -10 for Hawaii, -8 for Pacific). Default is 0 (UTC)."
            dest_name = "timezone_hours"
            default = "0"
    end

    return parse_args(s)
end

function (@main)(ARGS)
    parsed_args = parse_commandline()
    
    # Handle the ref_date - use now() if not provided
    ref_date = now()  # Default to now()
    if parsed_args["ref_date"] !== nothing
        try
            ref_date = DateTime(parsed_args["ref_date"])
        catch e
            error("Invalid date format for --ref-date. Please use ISO format (e.g., 2023-09-07T12:34:56)")
        end
    end

    if parsed_args["archive_date"] !== nothing
        try
            archive_date = Date(parsed_args["archive_date"])
        catch e
            error("Invalid date format for --archive-date. Please use ISO format (e.g., 2023-09-07)")
        end
    else
        archive_date =nothing
    end
    
    # Call the output2fits function
    ArchiverService.output2fits(;
        start_time = parsed_args["start_time"],
        end_time = parsed_args["end_time"],
        ref_date = ref_date,
        timezone_hours = tryparse(Float64,parsed_args["timezone_hours"]),
        archive_date
    )
end