module ArchiverService
using Aeron
using AstroImages
using DataFrames
using Dates
using FITSIO
using Mmap
using SpidersMessageEncoding
using SQLite
using StaticStrings


# We create a SQLite table for each night of observations. 
# We will watch many aeron streams.
# The SQLite table serves as an index. We store just some basic details and not the 
# message body.
# We write the messages verbatim to disk, all just concatenated together.
# The index will then let us look back through this file efficiently.

# Whenever we receive a message, we will grab out it's header (assuming SpidersMessageEncoding format)
# and enter it as a new row into the database.

# The full message goes onto disk. We roll over the files periodically.

const file_mappers = Dict{String,Tuple{Vector{UInt8},IOStream}}()

function close_mmaps()
    for (dat,io) in values(file_mappers)
        close(io)
    end
    empty!(file_mappers)
    return
end
public close_mmaps

include("service.jl")
include("message-query.jl")
include("replay.jl")
include("select-messages.jl")
include("save-messages.jl")

@main

public main, indices_corr_range, indices_time_range, replay_indices, save_messages, select_messages

end