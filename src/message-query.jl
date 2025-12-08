

function indices_corr_range(dbfile::AbstractString, streams::AbstractVector, corr_num_start::Integer, corr_num_stop::Integer)
    
    # Open DB in read only mode (so that the archiver can keep writing)
    db = SQLite.DB("file:$dbfile?mode=ro")
    ii = indices_corr_range(db, streams, corr_num_start, corr_num_stop)
    close(db)
    return ii
end
function indices_corr_range(db::SQLite.DB, streams::AbstractVector, corr_num_start::Integer, corr_num_stop::Integer)
    # Is correlation ID really robust enough here? It's just randomly generated.
    df1 = DataFrame(DBInterface.execute(db, "SELECT rowid FROM message_index where correlationId=$corr_num_start LIMIT 1"))
    df2 = DataFrame(DBInterface.execute(db, "SELECT rowid FROM message_index where correlationId=$corr_num_stop LIMIT 1"))
    if size(df1,1) < 1
        dferr = DataFrame(DBInterface.execute(db, "SELECT * FROM message_index LIMIT 10"))
        display(dferr)
        error("starting correlationId not found")
    end
    if size(df2,1) < 1
        dferr = DataFrame(DBInterface.execute(db, "SELECT * FROM message_index LIMIT 10"))
        display(dferr)
        error("stopping correlationId not found")
    end
    startid = df1.rowid[1]
    endid = df2.rowid[1]
    streams = join(string.(streams), ",")# TODO: use temp table
    dfii = DataFrame(DBInterface.execute(db, "SELECT rowid FROM message_index WHERE rowid <= $endid AND $startid <= rowid AND aeron_stream IN ($streams)"))
    return dfii.rowid
end


function indices_time_range(dbfile::AbstractString, streams, timestampns_start::Number, timestampns_stop::Number)
    # Open DB in read only mode (so that the archiver can keep writing)
    db = SQLite.DB("file:$dbfile?mode=ro")
    ii = indices_time_range(db, streams, timestampns_start, timestampns_stop)
    close(db)
    return ii
end
function indices_time_range(db::SQLite.DB, streams::AbstractVector, timestampns_start::Number, timestampns_stop::Number)
    streams_str = join(string.(streams), ",")   # TODO: use temp table 
    stmt = "SELECT rowid FROM message_index WHERE TimestampNs<=$timestampns_stop AND TimestampNs>=$timestampns_start AND aeron_stream IN ($streams_str)"
    dfii = DataFrame(DBInterface.execute(db, stmt))
    return dfii.rowid
end
function indices_time_range(db::SQLite.DB, stream::Integer, timestampns_start::Number, timestampns_stop::Number)
    stmt = "SELECT rowid FROM message_index WHERE TimestampNs<=$timestampns_stop AND TimestampNs>=$timestampns_start AND aeron_stream = $stream"
    dfii = DataFrame(DBInterface.execute(db, stmt))
    return dfii.rowid
end