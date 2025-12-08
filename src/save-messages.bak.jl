include("flexible-time-parse.jl")

const IFTS_OPD_STREAM_NO = 901

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
    timezone_hours=0
)
    
    start_time_epoch_utc = parse_time_to_epoch(start_time, ref_date) - timezone_hours*60*60
    end_time_epoch_utc = parse_time_to_epoch(end_time, ref_date) - timezone_hours*60*60
    if end_time_epoch_utc < start_time_epoch_utc 
        end_time_epoch_utc += 24*60*60
    end
    start_time_Ns = Int(round(UInt64, start_time_epoch_utc*1e9))
    end_time_Ns = Int(round(UInt64, end_time_epoch_utc*1e9))

    # date = Date(unix2datetime(start_time_epoch_utc))
    date = Date(ref_date)
    dbfname = "/mnt/datadrive/DATA/$date/INDEX-$date.sqlite"

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


        @info "Querying for all event messages in time range" start_time=start_time_epoch_utc end_time=end_time_epoch_utc
        cmd = @sprintf(
            "SELECT *, rowid FROM message_index WHERE schemaId = 6 AND %d < TimestampNs AND TimestampNs < %d",
            start_time_Ns,
            end_time_Ns
        )
        df_events = DataFrame(DBInterface.execute(db, cmd))

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


        # Generate time markers
        time_markers = _generate_time_markers(start_time_Ns, end_time_Ns)
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
                    # @time "Gathering" 
                    messages = ArchiverService.select_messages(TensorMessage, db, dbfname, rowid_vector)

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
                    # @time "Loading" 
                    data_cube = stack(arrays)
                    
                    # Store all data needed for FITS output
                    fits_data = Dict(
                        "time_bin_start" => bin_start_dt,
                        "time_bin_end" => bin_end_dt,
                        "aeron_uri" => uri,
                        "aeron_stream" => stream,
                        "data_cube" => data_cube,
                        "metadata" => uri_stream_df
                    )
                    # @info "Data size" 
                    size(data_cube)

                    cube_fname = "SPID-$(bin_start_dt)-$uri-$stream.fits"
                    # Sanitize the URI
                    cube_fname = replace(cube_fname, r"[^\w_\.-]"i=>"")
                    cube_fname = "/mnt/datadrive/DATA/$date/"*cube_fname
                    # @time "Saving" 
                    FITS(cube_fname, "w") do f
                        write(f, data_cube)

                        table = uri_stream_df
                        # FITSIO has fairly restrictive input types for writing tables (assertions for documentation only)
                        colname_strings = string.(collect(names(table)))::Vector{String}
                        columns = collect(eachcol(table))::Vector
                        # We're not able to output a column with empty strings -- I don't know why
                        for col in columns
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

    finally
        close(db)
    end
end
public output2fits


        # opds = ArchiverService.select_messages(TensorMessage, db, dbfname, df.rowid[10000:end]);
        # o = arraydata.(opds)
        # l = only.(o)