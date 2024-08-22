
function save_messages(dbfile, indices, output_cube_fname)
    
    if isfile(output_cube_fname)
        error("file already exists")
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