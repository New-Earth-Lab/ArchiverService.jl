
function dump_timerange(date, output_cube_fname, starttime, endtime)

    @warn "function is deprecated"
    dir = "$date/"

    if isfile(output_cube_fname)
        error("file already exists")
    end

    # Open DB in read only mode (so that the archiver can keep writing)
    dbfile = joinpath(dir,"INDEX-$date.sqlite")
    db = SQLite.DB("file:$dbfile?mode=ro")

    totcount = DataFrame(DBInterface.execute(db, "SELECT COUNT(TimestampNs) FROM message_index"))[1,1]
    startime = DataFrame(DBInterface.execute(db, "SELECT MIN(TimestampNs) FROM message_index"))[1,1]
    endtime = DataFrame(DBInterface.execute(db, "SELECT MAX(TimestampNs) FROM message_index"))[1,1]
    println("$totcount messages recorded between $startime and $endtime")



    df = DataFrame(DBInterface.execute(db, "SELECT * FROM message_index where schemaId = 1 AND  $starttime < TimestampNs AND TimestampNs < $endtime  "))
    # df = DataFrame(DBInterface.execute(db, "SELECT * FROM message_index "))
    println("Array Messages matching those timestamp criteria:")
    display(df)
    if size(df,1) == 0
        @warn "no images matching those criteria"
        return
    end
    # TODO: comparison to search for right schemaId and templateId doesn't work.

    
    FITS(output_cube_fname, "w") do fits
    

        for row in eachrow(df)
            fname = joinpath(dir, row.data_fname)
            if haskey(file_mappers, fname)
                mapper = file_mappers[fname]
            else
                mapper = file_mappers[fname] = Mmap.mmap(open(fname,read=true), Vector{UInt8})
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



"""
Given a date (to specify the database and raw files) and a list of saved message IDs,
return a list of `MessageType` SBE message decoders (eg TensorMessage).
The messages themselves are just views over memory-mapped files, so should have minimal
overhead.

TODO: currenly the SQL query is tremedously poorly optimized. see comment.
"""
function select_messages(MessageType, dbfname::AbstractString, indices)
    
    # Open DB in read only mode (so that the archiver can keep writing)
    db = SQLite.DB("file:$dbfname?mode=ro")
    msgs = select_messages(MessageType, db, dbfname, indices)
    close(db)
    return msgs
end
    


"""
Given a date (to specify the database and raw files) and a list of saved message IDs,
return a list of `MessageType` SBE message decoders (eg TensorMessage).
The messages themselves are just views over memory-mapped files, so should have minimal
overhead.

TODO: currenly the SQL query is tremedously poorly optimized. see comment.
"""
function select_messages(MessageType, db::SQLite.DB, dbfname, indices)
    
    if indices == (:)
        df = DataFrame(DBInterface.execute(db, "SELECT * FROM message_index"))
    else
        # TODO: probably more efficient to make like a temporary table and join against it or something.
        rowids_str = join(string.(indices), ",")
        df = DataFrame(DBInterface.execute(db, "SELECT * FROM message_index where rowid IN ($rowids_str)"))
    end
    # println("Array Messages matching those timestamp criteria:")
    # display(df)
    if size(df,1) == 0
        @warn "no images matching those criteria"
        return Union{Missing,MessageType}[]
    end

    messages = Union{Missing,MessageType}[]
    for row in eachrow(df)
        fname = joinpath(dirname(dbfname), row.data_fname)
        local mapper
        if haskey(file_mappers, fname)
            (mapper,io) = file_mappers[fname]
        else
            try
                io = open(fname,read=true)
                (mapper,io) = file_mappers[fname] = (
                    Mmap.mmap(io, Vector{UInt8}),
                    io
                )
            catch
                # file not found or corrupt or something
                push!(messages, missing)
                continue
            end
        end
        msg_start_buffer = @view mapper[Int(row.data_start_index)+1:end]
        try
            msg = MessageType(msg_start_buffer,initialize=false)
            push!(messages, msg)
        catch err
            @error "error loading data" exception=(err, Base.catch_backtrace())
            # continue
            break
        end
    end
    return messages
end

