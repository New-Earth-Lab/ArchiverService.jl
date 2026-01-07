include("flexible-time-parse.jl")

const IFTS_OPD_STREAM_NO = 901

using SimpleBinaryEncoding
using Printf

function save_messages(dbfile, indices, output_cube_fname; overwrite=false)
    
    if isfile(output_cube_fname)
        if overwrite
            rm(output_cube_fname)
        else
            error("file already exists")
        end
    end

    # Open DB in read only mode (so that the archiver can keep writing)
    db = SQLite.DB("file:$dbfile?mode=ro")

    # TODO: probably more efficient to make like a temporary table and join against it or something.
    rowids_str = join(string.(indices), ",")
    df = DataFrame(DBInterface.execute(db, "SELECT * FROM message_index where rowid IN ($rowids_str)"))
    # println("Array Messages matching those timestamp criteria:")
    # display(df)
    if size(df,1) == 0
        @warn "no images matching those criteria"
        return []
    end

    println("Array Messages matching those timestamp criteria:")
    display(df)
    if size(df,1) == 0
        @warn "no images matching those criteria"
        return
    end
    # TODO: comparison to search for right schemaId and templateId doesn't work.

    
    FITS(output_cube_fname, "w") do fits
    

        for row in eachrow(df)
            fname = joinpath(dirname(dbfile), row.data_fname)
            if haskey(file_mappers, fname)
                (mapper,io) = file_mappers[fname]
            else
                io = open(fname,read=true)
                (mapper,io) = file_mappers[fname] = (Mmap.mmap(io, Vector{UInt8}),io)
            end
            msg_start_buffer = @view mapper[Int(row.data_start_index)+1:end]
            try
                msg = SpidersMessageEncoding.TensorMessage(msg_start_buffer)
                # display(msg)
                print("Loaded TimestampNs=",msg.header.TimestampNs, " Size=")
                img = AstroImage(collect(SpidersMessageEncoding.arraydata(msg)))
                img["TIME-NS"] = msg.header.TimestampNs
                img["AECORRID"] = row.correlationId
                img["AESCHMID"] = row.schemaId
                img["AETEMPID"] = row.templateId
                img["AEVERSON"] = row.version
                img["AERCTNS"] = row.channelRcvTimestampNs
                img["AETXTNS"] = row.channelSndTimestampNs
                img["AEURI"] = row.aeron_uri
                img["AESTREAM"] = row.aeron_stream
                write(fits, collect(img), header=header(img))
                println(size(msg))
            catch err
                @error "error loading data" exception=(err, Base.catch_backtrace())
                # continue
                break
            end
        end
    end

    println("Done.")

end


# Generate time markers every 60s on even minute boundaries
function _generate_time_markers(start_time_Ns, end_time_Ns)
    # Convert nanoseconds to seconds
    start_time_s = start_time_Ns / 1e9
    end_time_s = end_time_Ns / 1e9
    
    # Convert to DateTime
    start_dt = Dates.unix2datetime(start_time_s)
    end_dt = Dates.unix2datetime(end_time_s + 60)
    
    # Round to nearest even minute boundaries
    start_minute = Dates.minute(start_dt)
    # start_minute_rounded = start_minute % 1 == 0 ? start_minute : start_minute + 1
    start_minute_rounded = start_minute
    start_dt_rounded = DateTime(
        Dates.year(start_dt),
        Dates.month(start_dt),
        Dates.day(start_dt),
        Dates.hour(start_dt),
        start_minute_rounded,
        0
    )
    
    # Ensure the first marker is not before start_time
    if start_dt_rounded < start_dt
        start_dt_rounded += Dates.Minute(1)
    end
    
    # Generate markers every 60s
    markers = DateTime[]
    current_dt = start_dt_rounded
    while current_dt <= end_dt
        push!(markers, current_dt)
        current_dt += Dates.Minute(1)
    end
    
    # Convert to nanoseconds
    markers_Ns = [Int(round(UInt64, Dates.datetime2unix(m) * 1e9)) for m in markers]
    
    # Add start and end times to ensure complete coverage
    prepend!(markers_Ns, start_time_Ns)
    push!(markers_Ns, end_time_Ns)
    
    return unique(sort(markers_Ns))
end


# The main workhorse behind spiders-dump
function output2fits(;
    start_time::AbstractString,
    end_time::AbstractString,
    ref_date::DateTime=now(),
    timezone_hours=0,
    archive_date::Union{Date,Nothing}=nothing  # Explicit archive folder date
)
    
    # If archive_date is specified, use it as the reference date for time parsing
    # This allows querying data that spans midnight from a specific archive folder
    effective_ref_date = isnothing(archive_date) ? ref_date : DateTime(archive_date)
    
    start_time_epoch_utc = parse_time_to_epoch(start_time, effective_ref_date) - timezone_hours*60*60
    end_time_epoch_utc = parse_time_to_epoch(end_time, effective_ref_date) - timezone_hours*60*60
    if end_time_epoch_utc < start_time_epoch_utc 
        end_time_epoch_utc += 24*60*60
    end
    start_time_Ns = Int(round(UInt64, start_time_epoch_utc*1e9))
    end_time_Ns = Int(round(UInt64, end_time_epoch_utc*1e9))

    # Use explicit archive_date if provided, otherwise derive from start time
    # This is crucial for querying data that spans midnight:
    # - The archiver writes to a folder based on when it STARTED
    # - Data captured after midnight still goes in the same folder
    # - So users need to specify the archive date to query cross-midnight data
    date = isnothing(archive_date) ? Date(unix2datetime(start_time_epoch_utc)) : archive_date
    output_dir = "/mnt/datadrive/DATA/$date"
    dbfname = "$output_dir/INDEX-$date.sqlite"

    # Open DB in read only mode (so that the archiver can keep writing)
    @info "Opening index database" dbfname
    db = SQLite.DB("file:$dbfname?mode=ro")
    try
        # @info "Querying for all messages in time range" start_time=Dates.unix2datetime(start_time_epoch_utc) end_time=Dates.unix2datetime(end_time_epoch_utc)
        @info "Querying for all array messages in time range" start_time=start_time_epoch_utc end_time=end_time_epoch_utc
        cmd = @sprintf(
            "SELECT *, rowid FROM message_index WHERE schemaId = 1 AND %d < TimestampNs AND TimestampNs < %d",
            start_time_Ns,
            end_time_Ns
        )

        # Output:
        #    Row │ TimestampNs          correlationId  description  schemaId  templateId  blockLength  version  channelRcvTimestampNs  channelSndTimestampNs  aeron_uri  aeron_stream  data_fname            ⋯
        #        │ Int64                Int64          String       Int64     Int64       Int64        Int64    Int64                  Int64                  String     Int64         String                ⋯
        # ───────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────

        df = DataFrame(DBInterface.execute(db, cmd))
        @info "Found $(size(df,1)) array messages"


        @info "Querying for all event messages in time range" start_time=start_time_epoch_utc end_time=end_time_epoch_utc
        cmd = @sprintf(
            "SELECT *, rowid FROM message_index WHERE schemaId = 6 AND %d < TimestampNs AND TimestampNs < %d",
            start_time_Ns,
            end_time_Ns
        )
        df_events = DataFrame(DBInterface.execute(db, cmd))
        @info "Found $(size(df_events,1)) event messages"

        # Collect all data loss events throughout processing
        all_data_loss_events = DataLossEvent[]

        event_messages, event_data_loss = ArchiverService.select_messages(EventMessage, db, dbfname, df_events.rowid)
        append!(all_data_loss_events, event_data_loss)
        if !isempty(event_data_loss)
            @warn "Some event messages could not be loaded" count=length(event_data_loss)
        end

        # Filter out missing event messages and their corresponding df rows
        valid_event_mask = .!ismissing.(event_messages)
        event_messages = event_messages[valid_event_mask]
        df_events = df_events[valid_event_mask, :]

        df_events.name .= getproperty.(event_messages, :name)
        function _extract_event_value(msg)
            val = getargument(msg)
            # If the value is itself a TensorMessage, extract the array data
            if val isa TensorMessage
                return arraydata(val)
            end
            return val
        end
        df_events.value .= _extract_event_value.(event_messages)
        
        # identify unique combinations of (stream ID, event name)
        # unique!(select(df_events, [:aeron_stream, :name]))
        # gives e.g.
        # 1×3 DataFrame
        #  Row │ aeron_uri  aeron_stream  name
        #      │ String     Int64         SimpleBinaryEncoding.NullTermString
        # ─────┼────────────────────────────────────
        #    1 │ aeron:ipc          4321  test

        # Build event lookup structure
        event_lookup = build_event_lookup(df_events)
        array_cache = ArrayEventCache()
        @info "Built event lookup" unique_events=length(event_lookup) total_events=nrow(df_events)
        

        # Goal:
        # * Go through the table, between $start_time and $end_time (as given by TimestampNs column)
        # * Generate time markers -- if IFTS data present using OPD column (logic to be added later), otherwise every 60s on even minute boundaries
        # * Aggregate rows between each set of time markers, group by unique pairs of (df.aeron_uri (string), df.aeron_stream (Int))
        # * For each group within that time range, for that unique pair of uri, stream...
        #       * Gather raw data:
        #           messages = ArchiverService.select_messages(TensorMessage, db, dbfname, rowid_vector);
        #       * Call `arraydata(message)` on each message, givein an N-D array, and `stack` all of them into a contiguous cube of 2+ dimensions
        #       * Gather all rows from the database corresponding to these messages
        #       * Output a FITS file with two HDUs -- one with the contiguous N-D array of matching messages, another with a Table HDU with the entries from the database.


        # TODO: If OPD telemetry is here, we will use that to specify the chunking (but just for the IFTS? Or all?)
        # if any(==(IFTS_OPD_STREAM_NO), df.aeron_stream)
        #     @info "FTS telemetry present -- we will use it to chunk the data"
        # end


        # Generate time markers (including event-based breakpoints)
        time_markers = _generate_combined_markers(start_time_Ns, end_time_Ns, event_lookup)
        @info "Generated $(length(time_markers)) time markers"

        # Process each time bin
        for i in 1:(length(time_markers)-1)
            bin_start_Ns = time_markers[i]
            bin_end_Ns = time_markers[i+1]
            # TODO: what about last bin?
            bin_start_dt = Dates.unix2datetime(bin_start_Ns / 1e9)
            bin_end_dt = Dates.unix2datetime(bin_end_Ns / 1e9)
            
            # Filter rows within this time bin
            bin_df = df[(df.TimestampNs .>= bin_start_Ns) .& (df.TimestampNs .< bin_end_Ns), :]
            
            # Skip if empty
            if isempty(bin_df)
                continue
            end


            @info "Processing time bin" start=bin_start_dt
            
            # Group by unique pairs of (aeron_uri, aeron_stream)
            uri_stream_pairs = unique([(row.aeron_uri, row.aeron_stream) for row in eachrow(bin_df)])
            
            for (uri, stream) in uri_stream_pairs
                
                # Filter rows for this uri/stream pair
                uri_stream_df = bin_df[(bin_df.aeron_uri .== uri) .& (bin_df.aeron_stream .== stream), :]

                # Re-sort to be based on initial capture time, not time of arrival into the archive
                sort!(uri_stream_df, :TimestampNs)
                
                # Get rowids for these messages
                rowid_vector = uri_stream_df.rowid

                if isempty(rowid_vector)
                    continue
                end
                @info "Processing uri/stream pair" uri=uri stream=stream messages=length(rowid_vector)

                
                try
                    # Gather raw data
                    messages, tensor_data_loss = ArchiverService.select_messages(TensorMessage, db, dbfname, rowid_vector)
                    append!(all_data_loss_events, tensor_data_loss)

                    # Some files might have been already deleted from disk. We need to skip these.
                    ii_missing = ismissing.(messages)
                    messages = messages[.! ii_missing]
                    uri_stream_df = uri_stream_df[.! ii_missing, :]

                    # uri_stream_df is just the index columns for this stream ID.
                    # We also want to keep a running Dict of all event key=values
                    # We put the latest value for each frame
                    # For a special subset of important events, we will place the values of the first frame in the main
                    # header.
                    # TODO: For some special human-identified events, we need to add new breakpoints e.g. loop closed
                    # 
                    
                    if isempty(messages)
                        @warn "All frames missing from disk (RAW file deleted?)"
                        continue
                    end
                    # Process messages into arrays
                    arrays = [arraydata(message) for message in messages]
                    
                    # Check for consistent dimensions
                    first_dims = size(arrays[1])
                    all_compatible = all(size(arr) == first_dims for arr in arrays)
                    
                    if !all_compatible
                        @warn "Arrays have inconsistent dimensions, skipping" uri=uri stream=stream
                        continue
                    end
                    # Stack arrays along a new dimension at the end
                    data_cube = stack(arrays)

                    # Get most recent values of each event
                    event_columns = interpolate_events_to_frames(
                        event_lookup,
                        uri_stream_df.TimestampNs,
                        output_dir,
                        date,
                        array_cache
                    )



                    # Get header events for first frame
                    first_frame_ts = uri_stream_df.TimestampNs[1]
                    header_events = get_header_events_for_timestamp(
                        event_lookup, 
                        first_frame_ts,
                        output_dir,
                        date,
                        array_cache
                    )
                    

                    # Store all data needed for FITS output
                    # fits_data = Dict(
                    #     "time_bin_start" => bin_start_dt,
                    #     "time_bin_end" => bin_end_dt,
                    #     "aeron_uri" => uri,
                    #     "aeron_stream" => stream,
                    #     "data_cube" => data_cube,
                    #     "metadata" => uri_stream_df
                    # )

                    cube_fname = "SPID-$(bin_start_dt)-$uri-$stream.fits"
                    # Sanitize the URI
                    cube_fname = replace(cube_fname, r"[^\w_\.-]"i=>"")
                    cube_fname = "/mnt/datadrive/DATA/$date/"*cube_fname
                    # @time "Saving" 
                    FITS(cube_fname, "w") do f
                        write(f, data_cube)

                        # Add key headers from row 1
                        if !isempty(header_events)
                            write_header_events!(f[1], header_events)
                        end
                        #                ________
                        # write_key(f[1], "binstart", bin_start_dt)
                        # write_key(f[1], "binend", bin_end_dt)
                        # write_key(f[1], "aeron.uri", uri)
                        # write_key(f[1], "aeron.stream", stream)


                        table = uri_stream_df
                        # FITSIO has fairly restrictive input types for writing tables (assertions for documentation only)
                        colname_strings = string.(collect(names(table)))::Vector{String}
                        columns = collect(eachcol(table))::Vector

                        # Add event columns to table
                        for (evt_col_name, evt_col_values) in event_columns
                            push!(colname_strings, evt_col_name)
                            # Convert Any vector to concrete type if possible
                            push!(columns, _concretize_column(evt_col_values))
                        end

                        # We're not able to output a column with empty strings -- I don't know why
                        for i_col in eachindex(columns)
                            col = columns[i_col]
                            if eltype(col) <: SimpleBinaryEncoding.NullTermString
                                col = columns[i_col] = String.(col)
                            end
                            if eltype(col) <: AbstractString
                                col[isempty.(col)] .= "NA"
                            end
                        end
                        write(
                            f,
                            colname_strings,
                            columns;
                            hdutype=TableHDU
                            # header=nothing
                        )
                    end
                    println(cube_fname)
                    ArchiverService.close_mmaps()
                    GC.gc()
                catch e
                    @error "Error processing messages" uri=uri stream=stream exception=(e, catch_backtrace())
                end
            end

        end

        # Write data loss report if any events were lost
        if !isempty(all_data_loss_events)
            write_data_loss_report(output_dir, date, all_data_loss_events)
        end

    finally
        close(db)
    end
end
public output2fits

"""
    write_data_loss_report(output_dir, date, events::Vector{DataLossEvent})

Write a persistent data loss report file indicating time ranges where data
was lost due to file truncation (e.g., disk full) or other issues.

Creates a file named `DATA-LOSS-REPORT-{date}.txt` in the output directory.
"""
function write_data_loss_report(output_dir::String, date::Date, events::Vector{DataLossEvent})
    report_fname = joinpath(output_dir, "DATA-LOSS-REPORT-$date.txt")

    # Group events by reason
    by_reason = Dict{Symbol, Vector{DataLossEvent}}()
    for evt in events
        push!(get!(by_reason, evt.reason, DataLossEvent[]), evt)
    end

    # Find time range of lost data
    all_timestamps = [evt.timestamp_ns for evt in events]
    min_ts = minimum(all_timestamps)
    max_ts = maximum(all_timestamps)
    min_dt = Dates.unix2datetime(min_ts / 1e9)
    max_dt = Dates.unix2datetime(max_ts / 1e9)

    # Group by file for summary
    by_file = Dict{String, Vector{DataLossEvent}}()
    for evt in events
        push!(get!(by_file, evt.filename, DataLossEvent[]), evt)
    end

    open(report_fname, "w") do io
        println(io, "=" ^ 72)
        println(io, "SPIDERS DATA LOSS REPORT")
        println(io, "=" ^ 72)
        println(io)
        println(io, "Date: $date")
        println(io, "Report generated: $(Dates.now())")
        println(io)
        println(io, "-" ^ 72)
        println(io, "SUMMARY")
        println(io, "-" ^ 72)
        println(io)
        println(io, "Total messages lost: $(length(events))")
        println(io, "Time range affected: $min_dt to $max_dt UTC")
        println(io, "Duration: $(round((max_ts - min_ts) / 1e9, digits=3)) seconds")
        println(io)

        println(io, "By reason:")
        for (reason, reason_events) in sort(collect(by_reason), by=x->length(x[2]), rev=true)
            reason_str = if reason == :truncated
                "File truncated (disk full)"
            elseif reason == :file_missing
                "File missing from disk"
            elseif reason == :read_error
                "Read error"
            else
                string(reason)
            end
            println(io, "  - $reason_str: $(length(reason_events)) messages")
        end
        println(io)

        println(io, "By file:")
        for (fname, file_events) in sort(collect(by_file), by=x->length(x[2]), rev=true)
            timestamps = [evt.timestamp_ns for evt in file_events]
            file_min_dt = Dates.unix2datetime(minimum(timestamps) / 1e9)
            file_max_dt = Dates.unix2datetime(maximum(timestamps) / 1e9)
            if !isempty(file_events) && file_events[1].file_size > 0
                println(io, "  - $fname: $(length(file_events)) messages")
                println(io, "    File size: $(file_events[1].file_size) bytes")
                println(io, "    Time range: $file_min_dt to $file_max_dt")
            else
                println(io, "  - $fname: $(length(file_events)) messages (file missing)")
            end
        end
        println(io)

        println(io, "-" ^ 72)
        println(io, "DETAILED EVENTS (first 100)")
        println(io, "-" ^ 72)
        println(io)

        # Sort by timestamp
        sorted_events = sort(events, by=e->e.timestamp_ns)
        for (i, evt) in enumerate(first(sorted_events, 100))
            ts_dt = Dates.unix2datetime(evt.timestamp_ns / 1e9)
            println(io, "[$i] $ts_dt")
            println(io, "    File: $(evt.filename)")
            println(io, "    Reason: $(evt.reason)")
            println(io, "    Index: $(evt.data_start_index), File size: $(evt.file_size)")
            println(io)
        end

        if length(events) > 100
            println(io, "... and $(length(events) - 100) more events")
        end

        println(io)
        println(io, "=" ^ 72)
        println(io, "END OF REPORT")
        println(io, "=" ^ 72)
    end

    @warn "Data loss detected - report written" file=report_fname messages_lost=length(events) time_range="$min_dt to $max_dt"

    return report_fname
end

using MD5

# =============================================================================
# EVENT TO HEADER KEY MAPPING
# =============================================================================
# Map (aeron_stream, event_name) => FITS header keyword
# Only events listed here will be stored in headers.
# Header keywords should be ≤8 chars for standard FITS, or use HIERARCH convention.
const EVENT_HEADER_MAPPING = Dict{Tuple{Int,String}, String}(
    # (stream_id, "event_name") => "KEYWORD",
    # Examples (fill in your actual mappings):
    # (4321, "loop_status")     => "LOOPSTAT",
    # (4321, "telescope_ra")    => "RA",
    # (4321, "telescope_dec")   => "DEC",
    # (4321, "interaction_mat") => "IMATMD5",  # Will store MD5 for arrays
)

const STREAM_MAP = Dict(

    #________
    10 => "META",
    1832 => "CLOCK",
    602 => "SCC.cal" ,
    702 => "Gld.cal" ,
    802 => "FTS.cal" ,
    102 => "DMMixer" ,
    402 => "Integ" ,
    302 => "LOWFS" ,
    304 => "MLOWFS" ,
    502 => "SCC" ,
    902 => "SuperK" ,
    1802 => "Lin.FPM",
    1804 => "Lin.Source.Filt",
    1806 => "Lin.FTS.Block",
    1808 => "Lin.Source.Fold",
    1810 => "Lin.Phase",
    1812 => "Rot.Phase",
    1814 => "Flip.SCC.Block",
    1816 => "Flip.SCC.Pupil",
    1818 => "Flip.LOWFS.Focal",
    1820 => "Flip.LOWFS.Mag",
    1822 => "Filt.FTS",
    1824 => "Filt.LOWFS",
    1826 => "Filt.SCC",
    904 => "FTS" ,
    922 => "Perf.Gold" ,
    942 => "Perf.CRED2" ,
    932 => "Perf.Full" ,
)

# =============================================================================
# BREAKPOINT EVENTS CONFIGURATION
# =============================================================================
# Events that should trigger file splits (breakpoints) when their values change.
# Specify using stream names from STREAM_MAP, e.g., ("META", "OBJECT")
const BREAKPOINT_EVENTS = Set{Tuple{String,String}}([
    ("META", "OBJECT"),      # Object name changes
    ("Integ", "STATE"),      # Integrator state changes
    # Add more breakpoint events as needed
])

# Build reverse lookup: stream_name => stream_id
const STREAM_NAME_TO_ID = Dict(v => k for (k, v) in STREAM_MAP)

"""
    _resolve_breakpoint_events(breakpoint_events) -> Set{Tuple{Int,String}}

Convert user-friendly (stream_name, event_name) pairs to (stream_id, event_name) pairs
using STREAM_NAME_TO_ID lookup.
"""
function _resolve_breakpoint_events(
    breakpoint_events::Set{Tuple{String,String}}
)::Set{Tuple{Int,String}}
    resolved = Set{Tuple{Int,String}}()
    for (stream_name, event_name) in breakpoint_events
        if haskey(STREAM_NAME_TO_ID, stream_name)
            push!(resolved, (STREAM_NAME_TO_ID[stream_name], event_name))
        else
            @warn "Unknown stream name in BREAKPOINT_EVENTS, skipping" stream_name
        end
    end
    return resolved
end

"""
    _find_event_change_timestamps(event_lookup, breakpoint_events) -> Vector{Int64}

Find all timestamps where any of the specified breakpoint events changed value.
Returns a sorted, deduplicated vector of nanosecond timestamps.

The first occurrence of each event is considered a "change" (from no value to some value).
"""
function _find_event_change_timestamps(
    event_lookup::Dict{Tuple{Int,String}, Vector{Tuple{Int64,Any}}},
    breakpoint_events::Set{Tuple{Int,String}}
)::Vector{Int64}
    change_timestamps = Int64[]

    for (stream_id, event_name) in breakpoint_events
        events = get(event_lookup, (stream_id, event_name), Tuple{Int64,Any}[])

        if isempty(events)
            continue
        end

        # First event is always a "change" (from nothing to something)
        push!(change_timestamps, events[1][1])

        # Check subsequent events for value changes
        for i in 2:length(events)
            prev_ts, prev_val = events[i-1]
            curr_ts, curr_val = events[i]

            # Only add breakpoint if value actually changed
            if curr_val != prev_val
                push!(change_timestamps, curr_ts)
            end
        end
    end

    # Return sorted and deduplicated timestamps
    return unique(sort(change_timestamps))
end

"""
    _generate_combined_markers(start_time_Ns, end_time_Ns, event_lookup) -> Vector{Int64}

Generate time markers that combine:
1. Regular 60-second interval markers on minute boundaries
2. Breakpoints at timestamps where BREAKPOINT_EVENTS change value

Returns a sorted, deduplicated vector of nanosecond timestamps within the given range.
"""
function _generate_combined_markers(
    start_time_Ns::Int64,
    end_time_Ns::Int64,
    event_lookup::Dict{Tuple{Int,String}, Vector{Tuple{Int64,Any}}}
)::Vector{Int64}
    # Get regular time-based markers
    time_markers = _generate_time_markers(start_time_Ns, end_time_Ns)

    # Resolve breakpoint events from user-friendly names to stream IDs
    resolved_breakpoints = _resolve_breakpoint_events(BREAKPOINT_EVENTS)

    if isempty(resolved_breakpoints)
        @info "No breakpoint events configured"
        return time_markers
    end

    # Find event change timestamps
    event_change_markers = _find_event_change_timestamps(event_lookup, resolved_breakpoints)

    # Filter to only include changes within our time range
    event_change_markers = filter(ts -> start_time_Ns <= ts <= end_time_Ns, event_change_markers)

    if !isempty(event_change_markers)
        @info "Found event-based breakpoints" count=length(event_change_markers)
    end

    # Merge, sort, and deduplicate all markers
    all_markers = unique(sort(vcat(time_markers, event_change_markers)))

    return all_markers
end

# =============================================================================
# EVENT LOOKUP HELPERS
# =============================================================================

"""
    build_event_lookup(df_events) -> Dict{Tuple{Int,String}, Vector{Tuple{Int64,Any}}}

Build a lookup structure mapping (aeron_stream, event_name) to a sorted vector
of (TimestampNs, value) pairs for efficient temporal interpolation.
"""
function build_event_lookup(df_events::DataFrame)
    lookup = Dict{Tuple{Int,String}, Vector{Tuple{Int64,Any}}}()
    
    for row in eachrow(df_events)
        key = (row.aeron_stream, string(row.name))
        ts_val = (row.TimestampNs, row.value)
        
        if haskey(lookup, key)
            push!(lookup[key], ts_val)
        else
            lookup[key] = [ts_val]
        end
    end
    
    # Sort each vector by timestamp
    for vec in values(lookup)
        sort!(vec, by=first)
    end
    
    return lookup
end

"""
    get_latest_value(events::Vector{Tuple{Int64,Any}}, timestamp_ns::Int64) -> Union{Any, Nothing}

Binary search for the most recent event value at or before the given timestamp.
Returns `nothing` if no event exists before the timestamp.
"""
function get_latest_value(events::Vector{Tuple{Int64,Any}}, timestamp_ns::Int64)
    isempty(events) && return nothing
    
    # Find last index where timestamp <= target
    idx = searchsortedlast(events, timestamp_ns; by=first)
    
    return idx > 0 ? events[idx][2] : nothing
end


"""
    ArrayEventCache

Cache to track previously written array events and avoid duplicate files.
Maps (stream, name) → (content_hash, filename) of most recently written array.
"""
const ArrayEventCache = Dict{Tuple{Int,String}, Tuple{UInt64, String}}

"""
    format_event_value_for_fits(value, timestamp_ns, stream, name, output_dir, date, array_cache) 
        -> (formatted_value, is_array::Bool)

Format an event value for FITS storage:
- Arrays → saved to separate FITS file (if content changed), returns filename
- nothing → missing
- Strings/numbers → pass through

Uses array_cache to deduplicate identical arrays from repeated heartbeats.
"""
function format_event_value_for_fits(
    value,
    timestamp_ns::Int64,
    stream::Int,
    name::String,
    output_dir::String,
    date::Date,
    array_cache::ArrayEventCache
)
    if value isa AbstractArray
        # Hash the array content
        content_hash = hash(value)
        cache_key = (stream, name)
        
        # Check if we've seen identical content before
        if haskey(array_cache, cache_key)
            prev_hash, prev_fname = array_cache[cache_key]
            if prev_hash == content_hash
                # Same content - return existing filename
                return (prev_fname, true)
            end
        end
        
        # New content - write new file
        ts_dt = Dates.unix2datetime(timestamp_ns / 1e9)
        ts_str = Dates.format(ts_dt, "HH-MM-SS.sss")
        sanitized_name = replace(name, r"[^\w]" => "_")
        
        fname = "AUX-$(date)T$(ts_str)-$(stream)-$(sanitized_name).fits"
        fpath = joinpath(output_dir, fname)
        
        FITS(fpath, "w") do f
            write(f, value)
            write_key(f[1], "EVTNAME", name)
            write_key(f[1], "EVTSTRM", stream)
            write_key(f[1], "EVTTS", timestamp_ns)
        end
        @debug "Wrote array event to file" name=name path=fpath size=size(value)
        
        # Update cache
        array_cache[cache_key] = (content_hash, fname)
        
        return (fname, true)
    elseif isnothing(value)
        return (missing, false)
    elseif value isa AbstractString
        return (String(value), false)
    else
        return (value, false)
    end
end

"""
    interpolate_events_to_frames(event_lookup, frame_timestamps_ns, output_dir, date; event_keys=nothing) 
        -> Dict{String, Vector}

For each event key, create a column vector with the most recent value 
interpolated to each frame timestamp. Array values are saved to separate 
FITS files and the filename is stored instead.
"""
function interpolate_events_to_frames(
    event_lookup::Dict{Tuple{Int,String}, Vector{Tuple{Int64,Any}}},
    frame_timestamps_ns::AbstractVector{Int64},
    output_dir::String,
    date::Date,
    array_cache::ArrayEventCache;
    event_keys::Union{Nothing, Set{Tuple{Int,String}}} = nothing
)
    columns = Dict{String, Vector}()
    n_frames = length(frame_timestamps_ns)

    keys_to_process = isnothing(event_keys) ? keys(event_lookup) :
                      filter(k -> k in event_keys, keys(event_lookup))

    for (stream, name) in keys_to_process
        events = get(event_lookup, (stream, name), Tuple{Int64,Any}[])

        col_values = Vector{Any}(undef, n_frames)

        if isempty(events)
            fill!(col_values, missing)
        else
            # Optimized: iterate over event changes instead of frames
            # Events are sorted by timestamp, so we process each event's validity range

            # Handle frames before the first event
            first_event_ts = events[1][1]
            first_frame_idx = searchsortedfirst(frame_timestamps_ns, first_event_ts)
            if first_frame_idx > 1
                @view(col_values[1:first_frame_idx-1]) .= missing
            end

            # Process each event and fill the range of frames it applies to
            for j in eachindex(events)
                event_ts, raw_val = events[j]

                # Find the first frame where this event applies
                start_frame = searchsortedfirst(frame_timestamps_ns, event_ts)

                # Find the last frame where this event applies (before next event)
                if j < length(events)
                    next_event_ts = events[j + 1][1]
                    end_frame = searchsortedfirst(frame_timestamps_ns, next_event_ts) - 1
                else
                    end_frame = n_frames
                end

                # Skip if no frames in this range
                if start_frame > end_frame || start_frame > n_frames
                    continue
                end

                # Format the value once, then fill the range
                formatted, _ = format_event_value_for_fits(
                    raw_val, event_ts, stream, name, output_dir, date, array_cache
                )
                @view(col_values[start_frame:end_frame]) .= Ref(formatted)
            end
        end

        stream_name = get(STREAM_MAP, stream, string(stream))
        col_name = "$(stream_name).$(replace(name, r"[^\w]" => "_"))"
        columns[col_name] = col_values
    end

    return columns
end

"""
    _get_latest_timestamp(events, timestamp_ns) -> Int64

Get the timestamp of the most recent event at or before timestamp_ns.
"""
function _get_latest_timestamp(events::Vector{Tuple{Int64,Any}}, timestamp_ns::Int64)
    idx = searchsortedlast(events, timestamp_ns; by=first)
    return events[idx][1]
end

"""
    get_header_events_for_timestamp(event_lookup, timestamp_ns, output_dir, date) 
        -> Dict{String, Any}

Get all events at a specific timestamp for FITS header.
Returns both:
- Human-readable mapped keys from EVENT_HEADER_MAPPING (e.g., "LOOPSTAT")
- All events with auto-generated keys (e.g., "EVT_4321_loop_status")
"""
function get_header_events_for_timestamp(
    event_lookup::Dict{Tuple{Int,String}, Vector{Tuple{Int64,Any}}},
    timestamp_ns::Int64,
    output_dir::String,
    date::Date,
    array_cache::ArrayEventCache
)
    header_values = Dict{String, Any}()
    

        
    # First pass: add human-readable mapped keys (these take priority if there's overlap)
    for ((stream, name), header_key) in EVENT_HEADER_MAPPING
        events = get(event_lookup, (stream, name), Tuple{Int64,Any}[])
        raw_val = get_latest_value(events, timestamp_ns)
        
        if !isnothing(raw_val)
            event_ts = _get_latest_timestamp(events, timestamp_ns)
            formatted, _ = format_event_value_for_fits(
                raw_val, event_ts, stream, name, output_dir, date, array_cache
                
            )
            if !ismissing(formatted)
                header_values[header_key] = formatted
            end
        end
    end

    # Second pass: add ALL events with auto-generated keys
    for (stream, name) in sort(collect(keys(event_lookup)))
        events = event_lookup[(stream, name)]
        raw_val = get_latest_value(events, timestamp_ns)
        
        if !isnothing(raw_val)
            event_ts = _get_latest_timestamp(events, timestamp_ns)
            formatted, _ = format_event_value_for_fits(
                raw_val, event_ts, stream, name, output_dir, date, array_cache
            )
            if !ismissing(formatted)
                # Auto-generated key matching column names
                stream_name = get(STREAM_MAP, stream, string(stream))
                auto_key = "$(stream_name).$(replace(name, r"[^\w]" => "_"))"
                header_values[auto_key] = formatted
            end
        end
    end

    
    return header_values
end

"""
    write_header_events!(hdu, header_events::Dict{String, Any})

Write event values to a FITS HDU header.
"""
function write_header_events!(hdu, header_events::Dict{String, Any})
    for (key, value) in header_events
        try
            # FITSIO will use HIERARCH for long keywords automatically
            write_key(hdu, key, value)
        catch e
            @warn "Could not write header key" key=key value=value exception=e
        end
    end
end

"""
    _concretize_column(col::Vector{Any}) -> Vector

Try to convert a Vector{Any} to a concrete typed vector for FITSIO.
Falls back to String representation if types are mixed.
"""
function _concretize_column(col::Vector{Any})
    # Filter out missing to determine underlying type
    non_missing = filter(!ismissing, col)
    
    if isempty(non_missing)
        # All missing - return as strings
        return fill("", length(col))
    end
    
    types = unique(typeof.(non_missing))
    
    if length(types) == 1
        T = types[1]
        if T <: Number
            # Numeric column with possible missing
            result = Vector{Union{T, Missing}}(col)
            # FITSIO may not handle Missing well, convert to NaN for floats
            if T <: AbstractFloat
                return [ismissing(x) ? T(NaN) : x for x in result]
            else
                # For integers, use a sentinel or convert to float
                return [ismissing(x) ? NaN : Float64(x) for x in result]
            end
        elseif T <: AbstractString
            return [ismissing(x) ? "NA" : string(x) for x in col]
        end
    end
    
    # Mixed types or complex - stringify everything
    return [ismissing(x) ? "NA" : string(x) for x in col]
end