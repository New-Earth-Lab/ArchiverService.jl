module ArchiverService
using Aeron
using SQLite
using Dates
using StaticStrings
using SpidersMessageEncoding

# We create a SQLite table for each night of observations. 
# We will watch many aeron streams.
# The SQLite table serves as an index. We store just some basic details and not the 
# message body.
# We write the messages verbatim to disk, all just concatenated together.
# The index will then let us look back through this file efficiently.

# Whenever we receive a message, we will grab out it's header (assuming SpidersMessageEncoding format)
# and enter it as a new row into the database.

# The full message goes onto disk. We roll over the files periodically.

function main(ARGS)

    # SQL index-table bulk insert size. 10k is recommended.
    # Lower values are safer w.r.t. data-loss, but slower.
    chunk_N = 1000

    # Delay after which data is committed to the index even if we 
    # haven't accumulated a full batch.
    commit_delay_timeout = 5#30.0 # seconds

    data_archive_rollover_filesize_bytes = 2^30 # approx 1Gb

    dir = joinpath(pwd(), Dates.format(Dates.now(), "yyyy-mm-dd"))
    # dir = joinpath("/mnt/datadrive/DATA", Dates.format(Dates.now(), "yyyy-mm-dd"))
    if !isdir(dir)
        mkdir(dir)
    end
    
    ctx = AeronContext()

    # TODO: list of multiple streams configured somewhere
    # TODO: do we want to toggle listening to a given stream on and off while running?
    conf = AeronConfig(
        uri="aeron:ipc",
        stream=1006
    )

    confs = [conf]

    index_db_fname = gen_index_db_filename()
    index_db_fname_dir = joinpath(dir, index_db_fname)
    println("Index of messages will be written to $index_db_fname_dir")
    db = SQLite.DB(index_db_fname_dir)

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
            data_fname TEXT NOT NULL,
            data_start_index INT NOT NULL
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
        description   = view(cstatic""32, 1:0),
        schemaId      = zero(Int64),
        templateId    = zero(Int64),
        blockLength   = zero(Int64),
        version       = zero(Int64),
        channelRcvTimestampNs = zero(Int64),
        channelSndTimestampNs = zero(Int64),
        # These are the actual index values that say where a message is stored in the
        # raw file.
        data_fname   = "",
        data_start_index = zero(Int64)
    ), chunk_N)


    # TODO: we can't wait too long after subscribing before we start consuming!
    # Do this at the last moment, and preferably after all code is compiled.
    subscriptions = map(confs) do conf
        Aeron.subscriber(ctx, conf)
    end

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

    GC.enable_logging(true)

    try
        while true
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
                row_i = 0
                last_commit_time = t
            end
            # TODO: periodically roll over the raw data file so it doesn't grow to large
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
            for sub in subscriptions
                fragments, data = Aeron.poll(sub)
                if !isnothing(data)
                    begin
                        row_i_this = row_i + 1
                        msg = GenericMessage(data.buffer; initialize=false) # Don't clobber schemaId etc.
                        # We'd rather not send strings containing NULL to SQlite as it will
                        # store as BLOB instead of TEXT.
                        # This trick gets around it.
                        @time desc_no_nulls = view(msg.header.description, 1:lastindex(msg.header.description))
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
                            data_fname=current_data_fname,
                            data_start_index = data_i
                        )
                        # display(SpidersMessageEncoding.sbedecode(data.buffer))
                        write(data_file, data.buffer)
                        data_i += length(data.buffer)
                        row_i = row_i_this
                    end
                end
            end
            GC.safepoint() # To allow receiving SIGINT (CTRL+C)
            # TODO: file roll-over
        end
    finally
        # Error, only commit up to the last message and not any junk left after it
        if 0 < row_i <= chunk_N
            println("committing partial results to table.")
            @time SQLite.load!(@view(bulk_insert_table[1:row_i]), db, "message_index")
            row_i = 0
        end
    end
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


end