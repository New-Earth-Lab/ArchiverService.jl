
"""
Given a datastore and a vector of message IDs, rep=-publish them at the same rate
on their original streams.

`speed` is a speed multiplier, eg `speed=2.0` replays at twice the speed. `speed=0.1` replays 10x slower.
"""
function replay_indices(aeronctx::AeronContext, dbfile::AbstractString, indices::AbstractVector; speed=1.0)
    invspeed = 1/speed
    
    # Open DB in read only mode (so that the archiver can keep writing)
    db = SQLite.DB("file:$dbfile?mode=ro")
    
    # TODO: probably more efficient to make like a temporary table and join against it or something.
    rowids_str = join(string.(indices), ",")
    # stmt = "SELECT (data_fname,
    # data_start_index,
    # data_stop_index,
    # aeron_uri,
    # aeron_stream) FROM message_index where rowid IN ($rowids_str)"
    stmt = "SELECT * FROM message_index where rowid IN ($rowids_str)"
    df = DataFrame(DBInterface.execute(db,
        stmt))
    
    @info "connecting to streams"

    # Determine what publishers we will have to open:
    publisher_uri_stream_pairs = unique(zip(df.aeron_uri, df.aeron_stream))
    # Open them all
    publishers = Dict(
        pair => Aeron.publisher(aeronctx, AeronConfig(pair[1], pair[2]))
        for pair in publisher_uri_stream_pairs
    )
    @info "publishing..."

    # We have to republish our messages with updated timestamps.
    # Therefore, we will have to modify them. 
    # We don't want to modify them on-disk, so we will have to copy
    # each message into a temporary buffer where the relevant field 
    # can be modified.
    # If julia supported read only + Copy-on-write for Mmap, this wouldn't be 
    # necessary
    msg_buffer = zeros(UInt8, 2^20)

    try
        # TODO: put function barrier to make sure it's compiled and can keep up
        isfirst = true
        last_publication_time = Inf
        last_msg_time = Inf
        for row in eachrow(df)
            fname = joinpath(dirname(dbfile), row.data_fname)
            if haskey(file_mappers, fname)
                mapper = file_mappers[fname]
            else
                mapper = file_mappers[fname] = Mmap.mmap(open(fname,read=true), Vector{UInt8}, shared=false) # shared=false, we don't need to see updates.
            end
            resize!(msg_buffer, row.data_stop_index-row.data_start_index)
            msg_buffer .= @view mapper[Int(row.data_start_index)+1:Int(row.data_stop_index)]
            pub = publishers[(row.aeron_uri, row.aeron_stream)]
            msg = GenericMessage(msg_buffer, initialize=false)
            tmsg = msg.header.TimestampNs/1e9
            now = time()
            our_tdiff = now - last_publication_time
            goal_tdiff = invspeed*(tmsg - last_msg_time)
            if !isfirst
                if our_tdiff > goal_tdiff
                    # println("no sleep")
                else
                    t = goal_tdiff - our_tdiff
                    # println("sleep $t")
                    sleep(t)
                end
            end
            # The one change we make: freshen the timestamp.
            msg.header.TimestampNs = round(UInt64, time()*1e9)
            status = put!(pub, msg_buffer)
            if status != :success
                println(status)
            end
            last_publication_time = time()
            last_msg_time = tmsg
            isfirst = false
        end
    finally 
        close.(values(publishers))
    end
end