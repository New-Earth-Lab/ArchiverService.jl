
Base.@kwdef mutable struct PendingCmd
    stream::Int=0
    uri::String="aeron:ipc"
    enabled::Bool=false
end

"""
Main entrypoint into the archiver service. This runs in the background and responds to 
aeron control messages. You can enable and disable recording of a stream via a an event like so:
```julia
using SpidersMessageSender
sendevents(uri="aeron:ipc", stream=103, enabled=1) # On 
sendevents(uri="aeron:ipc", stream=103, enabled=0) # Off
```
"""
function main(ARGS)

    # SQL index-table bulk insert size. 10k is recommended.
    # Lower values are safer w.r.t. data-loss, but slower.
    chunk_N = 5000

    # Delay after which data is committed to the index even if we 
    # haven't accumulated a full batch.
    commit_delay_timeout = 5 # seconds

    data_archive_rollover_filesize_bytes = 2^30 # approx 1Gb

    # dir = joinpath(pwd(), Dates.format(Dates.now(), "yyyy-mm-dd"))
    dir = joinpath("/mnt/datadrive/DATA", Dates.format(Dates.now(), "yyyy-mm-dd"))
    if !isdir(dir)
        mkdir(dir)
    end
    
    ctx = AeronContext()

    # Dictionary of aeron subscriptions we are actively recording
    # from.
    aeron_stream_recording_dict = Dict{
        Tuple{String,Int},
        Aeron.AeronSubscription
    }()

    # There is a special metadata channel that we will always listen to
    key = (uri="aeron:ipc", stream=10)
    conf_meta = AeronConfig(;key...)
    aeron_stream_recording_dict[Tuple(key)] = Aeron.subscriber(ctx,conf_meta)

    # This configuration object, on the other hand,
    # is the input channel that where we listen for commands about
    # *this* service, ie turning recording of other services on and off.
    conf = AeronConfig(
        uri="aeron:ipc",
        stream=201
    )


    index_db_fname = gen_index_db_filename()
    index_db_fname_dir = joinpath(dir, index_db_fname)
    println("Index of messages will be written to $index_db_fname_dir")
    db = SQLite.DB(index_db_fname_dir)

    # Use Write-Ahead-Logging for hopefully better concurrent access
    DBInterface.execute(db, "PRAGMA journal_mode=WAL")

    # Create table if not exists
    # Order columns in the order they are most likely to be filtered by.
    DBInterface.execute(db, """
        CREATE TABLE IF NOT EXISTS message_index(
            TimestampNs INT NOT NULL,
            correlationId INT NOT NULL,
            description TEXT NOT NULL,
            schemaId INT NOT NULL,
            templateId INT NOT NULL,
            blockLength INT NOT NULL,
            version INT NOT NULL,
            channelRcvTimestampNs INT NOT NULL,
            channelSndTimestampNs INT NOT NULL,
            aeron_uri TEXT NOT NULL,
            aeron_stream INT NOT NULL,
            data_fname TEXT NOT NULL,
            data_start_index INT NOT NULL,
            data_stop_index INT NOT NULL
        )
    """)


    # To keep up with the message rate, we will do bulk inserts.
    # We will create our own table of N rows and write to these as we go.
    # When we reach the end (or maybe a timeout is reached??) or close,
    # we will write these rows en-mass to the SQL DB.

    # Row table version should be faster while building it up.
    bulk_insert_table = fill((;
        TimestampNs   = zero(Int64),
        correlationId = zero(Int64),
        # description   = view(cstatic""32, 1:0),
        # TODO: debuG slow
        description   = "",
        schemaId      = zero(Int64),
        templateId    = zero(Int64),
        blockLength   = zero(Int64),
        version       = zero(Int64),
        channelRcvTimestampNs = zero(Int64),
        channelSndTimestampNs = zero(Int64),
        # Where did we get the data?
        aeron_uri    = "",
        aeron_stream = zero(Int64),
        # These are the actual index values that say where a message is stored in the
        # raw file.
        data_fname   = "",
        data_start_index = zero(Int64),
        data_stop_index = zero(Int64)
    ), chunk_N)




    # To turn on / off recording, send `uri=<uri>`, then `stream=<stream_no>`, 
    # then `enabled=1` or `enabled=0`, and finally a commit all with the same
    # correlation number.
    pending_commands_by_corr_id = Dict{Int,PendingCmd}()

    pub_status = Aeron.publisher(ctx, AeronConfig(stream=conf.stream + 1, uri=conf.uri))

        
    Aeron.subscriber(ctx, conf) do control_subscription

        # Would be more appropriate to allocate these earlier in the code
        # but we trigger the Julia slow closure issue.


        # we use a cstatic string here which is just a view over bytes
        # This is because we have to put it in the datatable repeatedly
        current_data_fname = gen_rawdata_filename()
        current_data_fname_dir = joinpath(dir, current_data_fname)
        current_data_fname_c = convert(CStaticString{32}, current_data_fname)
        

        # When actually opening a file, we make it into a normal string
        data_file = open(current_data_fname_dir, write=true)

        # We increment row_i for each message received. 
        # Once we reach chunk_N, we send to the DB and resume.
        row_i = 0
        last_commit_time = time()

        # Data_i is the end of the data in the raw file on disk.
        # This will be added to the message index table as an offset
        data_i = 0 

        try


            GC.enable_logging(true)
            while true

                # Poll for incoming commands to *this* service
                fragments, frame = Aeron.poll(control_subscription)
                if !isnothing(frame)

                    cmd = GenericMessage(frame.buffer)

                    # We can have multiple event messages concatenated together.
                    # In this case, we apply each sequentually in one go. 
                    # This allows changing multiple parameters "atomically" between
                    # loop updates.
                    last_ind = 0
                    while last_ind < length(frame.buffer) # TODO: don't fail if there are a few bytes left over
                        data_span = @view frame.buffer[last_ind+1:end]
                        event = EventMessage(data_span, initialize=false)
                        event_data = @view data_span[1:sizeof(event)]
                        # if event.name == "StatusRequest"
                        #     # We handle this directly instead of in the state machine
                        #     # since we have access to pub_status and they don't
                        #     # In general, the state machine doesn't have to be aware
                        #     # of the control and status channels
                        #     send_status_update(sm, pub_status, event)
                        #     last_ind += sizeof(event)
                        #     continue
                        # end

                        # Dispatch event
                        if !haskey(pending_commands_by_corr_id, event.header.correlationId)
                            pending_commands_by_corr_id[event.header.correlationId] = PendingCmd()
                        end
                        pending_event = pending_commands_by_corr_id[event.header.correlationId]
                        if event.name == "uri"
                            pending_event.uri = getargument(String, event)
                        elseif event.name == "stream"
                            pending_event.stream = getargument(Float64, event)
                        elseif event.name == "enabled"
                            pending_event.enabled = getargument(Float64, event) != 0
                        elseif event.name == "StatusRequest"
                            @warn "StatusRequest not yet implemented for this service"
                        else
                            @warn "Received unsupported event name" event.name maxlog=1
                        end
                        # Republish this message to our status channel so that senders
                        # can know we have received and dealt with their command
                        # This is a form of *acknowledgement*
                        Aeron.put!(pub_status, event_data)
                        last_ind += sizeof(event)
                    end
                    pending_event = pending_commands_by_corr_id[cmd.header.correlationId]
                    if pending_event.stream == 0
                        @warn "can't subscribe without setting a non-zero stream number"
                    else
                        key = (pending_event.uri, pending_event.stream)
                        if pending_event.enabled && !haskey(aeron_stream_recording_dict, key)
                            println("Opening subscription $key")
                            conf = AeronConfig(;pending_event.uri, pending_event.stream)
                            aeron_stream_recording_dict[key] = Aeron.subscriber(ctx,conf)
                        elseif !pending_event.enabled && haskey(aeron_stream_recording_dict, key)
                            sub = aeron_stream_recording_dict[key] 
                            close(sub)
                            delete!(aeron_stream_recording_dict, key)
                            println("Closed subscription $key")
                        end
                    end
                # If we're receiving a command, process it ASAP 
                elseif fragments > 0
                    continue
                end

                # Periodically do a bulk insert of the data index into the DB.
                # TODO: perhaps I only have to check this periodically, even though 
                # we want to poll very fast.
                t = time()
                # Commit if we are at the batch sized limit
                if row_i == chunk_N ||
                    # Or, if we have data waiting, 
                    ( row_i > 0 && 
                    # It's been more than commit_delay_timeout seconds
                    # This way we don't leave data sitting in memory for long
                    # periods of time if there's not much coming in.
                    last_commit_time + commit_delay_timeout < t
                    )
                    # The call to load! allocates memory so occaisionally
                    # well hit a GC here. Thankfully, these GC pauses are 
                    # usually a few tens of ms, so of a similar cost to 
                    # the data loading itself.
                    println("committing $row_i message receipt(s) to index database.")
                    @time SQLite.load!(@view(bulk_insert_table[begin:row_i]), db, "message_index")
                    # DBInterface.execute(db, "COMMIT")# Not sure this is necessary
                    SQLite.C.sqlite3_wal_checkpoint(db.handle, "main")
                    row_i = 0
                    last_commit_time = t
                    println("flushing raw file")
                    flush(data_file)
                end
                # Periodically roll over the raw data file so it doesn't grow to large
                if data_i > data_archive_rollover_filesize_bytes
                    println("Rolling over to new data archive.")
                    println("Closing $current_data_fname")
                    close(data_file)
                    current_data_fname = gen_rawdata_filename()
                    current_data_fname_dir = joinpath(dir, current_data_fname)
                    current_data_fname_c = convert(CStaticString{32}, current_data_fname)
                    println("New file will be $current_data_fname")
                    data_file = open(current_data_fname_dir, write=true)
                    data_i = 0
                end


                # Loop and poll all recording subscriptions
                for (key, sub) in aeron_stream_recording_dict
                    # bail early if we the next message would put us out of bounds and we have to save the receipts to the DB
                    if row_i + 1 > length(bulk_insert_table)
                        break
                    end
                    fragments, frame = Aeron.poll(sub)
                    if !isnothing(frame)
                        row_i_this = row_i + 1
                        data_len = length(frame.buffer)
                        aeron_uri, aeron_stream = key
                        msg = GenericMessage(frame.buffer; initialize=false) # Don't clobber schemaId etc.
                        # We'd rather not send strings containing NULL to SQlite as it will
                        # store as BLOB instead of TEXT.
                        # This trick gets around it.
                        # desc_no_nulls = view(msg.header.description, 1:lastindex(msg.header.description))
                        # TODO DEBUG SLOW
                        desc_no_nulls = string(view(msg.header.description, 1:lastindex(msg.header.description)))
                        bulk_insert_table[row_i_this] = (;
                            msg.header.TimestampNs,
                            msg.header.correlationId,
                            description=desc_no_nulls,
                            msg.messageHeader.schemaId,
                            msg.messageHeader.templateId,
                            msg.messageHeader.blockLength,
                            msg.messageHeader.version,
                            msg.header.channelRcvTimestampNs,
                            msg.header.channelSndTimestampNs,
                            aeron_uri,
                            aeron_stream,
                            data_fname=current_data_fname,
                            data_start_index = data_i,
                            data_stop_index = data_i+data_len
                        )
                        # display(SpidersMessageEncoding.sbedecode(frame.buffer))
                        write(data_file, frame.buffer)
                        data_i += data_len
                        row_i = row_i_this
                    end
                end
                GC.safepoint() # To allow receiving SIGINT (CTRL+C)
            end # End while-poll loop.
        finally
            for (_, sub) in aeron_stream_recording_dict
                close(sub)
            end
            # Error, only commit up to the last message and not any junk left after it
            if 0 < row_i <= chunk_N
                println("committing partial results to table.")
                # Note: this sometimes fails if the reason we got an exception was on-insert to the table.
                # The end result is we try and commit new data before the previous transaction completed.
                SQLite.rollback(db)
                @time SQLite.load!(@view(bulk_insert_table[1:row_i]), db, "message_index")
                row_i = 0
            end
        end
    end # End Aeron-control subscription

    close(db)
    close(ctx)
end

function gen_index_db_filename()
    dt = Dates.now()
    fname = "INDEX-"*Dates.format(dt, "yyyy-mm-dd")*".sqlite"
    return fname
end


function gen_rawdata_filename()
    dt = Dates.now()
    fname = "RAW-"*Dates.format(dt, "yyyy-mm-dd-HH-MM-SS")*".1.raw"
    i = 1
    # Avoid clobbering files at all cost.
    # We should never hit this realistically since the fname is timestamped down to the second.
    while isfile(fname)
        i += 1
        @warn "File name already exists, will roll over to .$i.raw" fname
        fname = replace(fname, ".$(i-1).raw"=>".$i.raw")
    end
    return fname
end
