module ArchiverService
using Aeron
using SQLite
using Dates
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
    


    ctx = AeronContext()

    # TODO: list of multiple streams configured somewhere
    # TODO: do we want to toggle listening to a given stream on and off while running?
    conf = AeronConfig(
        uri="aeron:ipc",
        stream=1006
    )

    confs = [conf]

    db = SQLite.DB("data.sqlite")

    # Create table if not exists
    # Order columns in the order they are most likely to be filtered by.
    DBInterface.execute(db, """
        CREATE TABLE IF NOT EXISTS message_index(
            TimestampNs BIGINT NOT NULL,
            correlationId BIGINT NOT NULL,
            description VARCHAR(32) NOT NULL,
            schemaId USMALLINT NOT NULL,
            templateId USMALLINT NOT NULL,
            blockLength USMALLINT NOT NULL,
            version USMALLINT NOT NULL,
            channelRcvTimestampNs BIGINT NOT NULL,
            channelSndTimestampNs BIGINT NOT NULL
        )
    """)

    # To keep up with the message rate, we will do bulk inserts.
    # We will create our own table of N rows and write to these as we go.
    # When we reach the end (or maybe a timeout is reached??) or close,
    # we will write these rows en-mass to the SQL DB.

    # chunk_N = 1000
    # bulk_insert_table = (;
    #     TimestampNs   = zeros(Int64, chunk_N),
    #     correlationId = zeros(Int64, chunk_N),
    #     description   = fill(NTuple{32,UInt8}(zeros(UInt8,32)), chunk_N),
    #     schemaId      = zeros(UInt64, chunk_N),
    #     templateId    = zeros(UInt64, chunk_N),
    #     blockLength   = zeros(UInt64, chunk_N),
    #     version       = zeros(UInt64, chunk_N),
    #     channelRcvTimestampNs = zeros(Int64, chunk_N),
    #     channelSndTimestampNs = zeros(Int64, chunk_N),
    # )
    # Row table version should be faster while building it up.
    bulk_insert_table = fill((;
        TimestampNs   = zero(Int64),
        correlationId = zero(Int64),
        # NTuple{32,UInt8}
        description   = ntuple((_)->0x00, Val(32)),
        schemaId      = zero(UInt64),
        templateId    = zero(UInt64),
        blockLength   = zero(UInt64),
        version       = zero(UInt64),
        channelRcvTimestampNs = zero(Int64),
        channelSndTimestampNs = zero(Int64),
    ), chunk_N)


    # TODO: we can't wait too long after subscribing before we start consuming!
    # Do this at the last moment, and preferably after all code is compiled.
    subscriptions = map(confs) do conf
        Aeron.subscriber(ctx, conf)
    end


    current_data_fname = gen_rawdata_filename()
    data_file = open(current_data_fname, write=true)

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
                @time SQLite.load!(@view(bulk_insert_table[begin:row_i]), db, "temp")
                println("committed to index.")
                row_i = 0
                last_commit_time = t
            end
            # TODO: periodically roll over the raw data file so it doesn't grow to large
            # if data_i 
            # end
            for sub in subscriptions
                fragments, data = Aeron.poll(sub)
                if !isnothing(data)
                    begin
                        row_i += 1
                        println("received")
                        msg = GenericMessage(data.buffer; initialize=false) # Don't clobber schemaId etc.
                        bulk_insert_table[row_i] = (;
                            msg.header.TimestampNs,
                            msg.header.correlationId,
                            msg.header.description,
                            msg.messageHeader.schemaId,
                            msg.messageHeader.templateId,
                            msg.messageHeader.blockLength,
                            msg.messageHeader.version,
                            msg.header.channelRcvTimestampNs,
                            msg.header.channelSndTimestampNs,
                        )
                        # display(SpidersMessageEncoding.sbedecode(data.buffer))
                        write(data_file, data.buffer)
                        data_i += length(data.buffer)
                    end
                end
            end
            GC.safepoint() # To allow receiving SIGINT (CTRL+C)
            # TODO: file roll-over
        end
    finally
        # Error, only commit up to the last message and not any junk left after it
        if 0 < row_i <= chunk_N
            @time SQLite.load!(view(bulk_insert_table[1:row_i]), db, "temp")
            println("committed partial results to table.")
            row_i = 0
        end
    end
    close(db)
    close(ctx)
    
end


function gen_rawdata_filename()
    dt = Dates.now()
    fname = "RAW-"*Dates.format(dt, "yyyy-mm-dd-HH-MM-SS")*".raw"
    if isfile(fname)
        @warn "File name already exists, will roll over to .2.raw" fname
        fname = replace(fname, ".raw"=>".2.raw")
    end
    return fname
end


end