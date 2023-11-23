#=

This script dumps an archived image into a FITS file

Not sure yet how this should be generalized into an image integrator, etc.
Maybe we should just put each image into its own HDU.
=#
using Mmap
using SQLite
using DataFrames
using StaticStrings
using FITSIO
using SpidersMessageEncoding

function main(ARGS)
        
    output_cube_fname = "cubeout.fits"
    db = SQLite.DB("INDEX-2023-11-23.sqlite")
    df = DataFrame(DBInterface.execute(db, "SELECT * FROM message_index"))
    # TODO: comparison to search for right schemaId and templateId doesn't work.

    file_mappers = Dict{String,Vector{UInt8}}()

    FITS(output_cube_fname, "w") do fits
        
        for row in eachrow(df)
            fname = String(strip(CStaticString(row.data_fname),'\0'))
            if haskey(file_mappers, fname)
                mapper = file_mappers[fname]
            else
                mapper = file_mappers[fname] = Mmap.mmap(open(fname,read=true), Vector{UInt8})
            end
            msg_start_buffer = @view mapper[Int(row.data_start_index)+1:end]
            msg = SpidersMessageEncoding.TensorMessage(msg_start_buffer)
            # display(msg)
            # display(arraydata(msg))
            write(fits, collect(arraydata(msg)))
        end
    end

end
