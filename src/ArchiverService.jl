module ArchiverService
using Aeron
using SQLite
# using DuckDB
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

    ctx = AeronContext()

    # TODO: list of multiple streams configured somewhere
    # TODO: do we want to toggle listening to a given stream on and off while running?
    conf = AeronConfig(
        uri="aeron:ipc",
        stream=1006
    )

    confs = [conf]

    # db = SQLite.DB()# in memory (file)
    # create a new in-memory database
    db = BInterface.connect(DuckDB.DB, "tmp.db")


    # Create table if not exists
    # Order columns in the order they are most likely to be filtered by.
    # I don't think this matters for SQLite but matters for eg. Parquette
    # (;
    #     TimestampNs   = Int64[],
    #     correlationId = Int64[],
    #     description   = NTuple{32,UInt8}[],
    #     schemaId      = UInt64[],
    #     templateId    = UInt64[],
    #     blockLength   = UInt64[],
    #     version       = UInt64[],
    #     channelRcvTimestampNs = Int64[],
    #     channelSndTimestampNs = Int64[],
    # ) |> SQLite.load!(db, "message_index"; on_conflict="IGNORE")
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
            channelSndTimestampNs BIGINT NOT NULL,
        )
    """)


    # TODO: we can't wait too long after subscribing before we start consuming!
    # Do this at the last moment, and preferably after all code is compiled.
    subscriptions = map(confs) do conf
        Aeron.subscriber(ctx, conf)
    end

    # # Create an INSERT statement that we will reuse
    # stmt =SQLite.Stmt(
    #     db,
    #     "INSERT INTO message_index (
    #         'TimestampNs',
    #         'correlationId',
    #         'description',
    #         'schemaId',
    #         'templateId',
    #         'blockLength',
    #         'version',
    #         'channelRcvTimestampNs',
    #         'channelSndTimestampNs'
    #     ) VALUES (
    #         ?,
    #         ?,
    #         ?,
    #         ?,
    #         ?,
    #         ?,
    #         ?,
    #         ?,
    #         ?
    #     )";
    #     register = true
    # )
    stmt = DBInterface.prepare(db, """"
        INSERT INTO integers VALUES(?)
            'TimestampNs',
            'correlationId',
            'description',
            'schemaId',
            'templateId',
            'blockLength',
            'version',
            'channelRcvTimestampNs',
            'channelSndTimestampNs'
        ) VALUES (
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?
        )";
    """)

    i = 0
    @time SQLite.transaction(db) do 
        while i < 3
            for sub in subscriptions
                fragments, data = Aeron.poll(sub)
                if !isnothing(data)
                    i+= 1
                    msg = GenericMessage(data.buffer; initialize=false) # Don't clobber schemaId etc.
                    @time DBInterface.execute(stmt, (
                        msg.header.TimestampNs,
                        msg.header.correlationId,
                        msg.header.description,
                        msg.messageHeader.schemaId,
                        msg.messageHeader.templateId,
                        msg.messageHeader.blockLength,
                        msg.messageHeader.version,
                        msg.header.channelRcvTimestampNs,
                        msg.header.channelSndTimestampNs,
                    ))
                    display(SpidersMessageEncoding.sbedecode(data.buffer))
                end
            end
            GC.safepoint()
        end
    end

    close(ctx)
    
    res =  DBInterface.execute(db, "SELECT * FROM message_index")

    return res

end


end