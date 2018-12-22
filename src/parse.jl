import Base: convert

mutable struct MessageIterator
    data::Vector{UInt8}
    messages::OrderedDict{Int64, OrderedDict{Int64, String}}
    tag::Vector{Char}
    value::Vector{Char}
    state::Bool
    message_id::Int64
    function MessageIterator(data::Vector{UInt8})
        return new(data, OrderedDict{Int64, OrderedDict{Int64, String}}(), Char[], Char[], true, 0)
    end
end

length(fix::MessageIterator) = length(fix.data)

function start(fix::MessageIterator)
    return 1
end

function next(fix::MessageIterator, idx::Int64)::Tuple{MessageIterator, Int64}
    x = fix.data[idx]
    if x == 0x01
        int_tag = parse(Int64, String(fix.tag))
        str_value = String(fix.value)
        !haskey(fix.messages, fix.message_id) && (fix.messages[fix.message_id] = OrderedDict{Int64, String}())
        fix.messages[fix.message_id][int_tag] = str_value
        resize!(fix.tag, 0)
        resize!(fix.value, 0)
        if int_tag == 10
            fix.message_id += 1
        end
        fix.state = true
    elseif x == 0x3d
        fix.state = !fix.state
    else
        if fix.state
            push!(fix.tag, Char(x))
        else
            push!(fix.value, Char(x))
        end
    end

    return (fix, idx + 1)
end

function done(fix::MessageIterator, idx::Int64)
    return length(fix.data) > idx
end

function parse(data::Vector{UInt8})::OrderedDict{Int64, OrderedDict{Int64, String}}
    f = MessageIterator(data)
    for k in eachindex(data)
        next(f, k)
    end
    return f.messages
end

function convert(tags::Dict{Int64, String}, msg::OrderedDict{Int64, String})::OrderedDict{String, String}
    out = OrderedDict{String, String}()
    for (k, v) in msg
        out[tags[k]] = v
    end
    return out
end
