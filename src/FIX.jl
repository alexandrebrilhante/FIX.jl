module FIX

using DataStructures
using Dates
using DandelionWebSockets
using Printf

import Base: length, collect, close, parse, join

global const TAGS_INT_STRING = Dict{Int64, String}()
global const TAGS_STRING_INT = Dict{String, Int64}()

include("parse.jl")
include("management.jl")

abstract type MessageHandler<:DandelionWebSockets.WebSocketHandler end

export TAGS_INT_STRING, TAGS_STRING_INT

export MessageHandler, Client, send_message, start, close, checksum, join

export on_message

function on_message(fix::MessageHandler, x::Any)
    T = typeof(fix)
    X = typeof(x)
    throw(ErrorException("Method `on_message` is not implemented by $T for argument type $X"))
end

function __init__()
    global TAGS_INT_STRING
    global TAGS_STRING_INT

    fid = open(joinpath(@__DIR__, "../data/tags.csv"), "r");
    line_number = 0
    while !eof(fid)
        line_number += 1
        line = readline(fid)
        data = split(line, ",")

        if length(data) ≠ 2
            close(fid)
            throw(ErrorException("Invalid data in `etc/tags.csv` file on line $line_number: $line"))
        end

        tag = parse(Int64, String(data[1]))
        val = String(data[2])

        TAGS_INT_STRING[tag] = val
        TAGS_STRING_INT[val] = tag
    end
    close(fid)

    return nothing
end

mutable struct ClientTasks
    read::Union{Task, Nothing}
    function ClientTasks()
        return new(Union{Task, Nothing}())
    end
end

struct Client{T<:IO, H<:MessageHandler}
    stream::T
    handler::H
    delimiter::Char
    m_head::Dict{Int64, String}
    m_tasks::ClientTasks
    m_messages::MessageManagement
    m_lock::ReentrantLock
    m_intmap::Dict{Int64, String}

    function Client(stream::T, handler::H, header::Dict{Int64, String},
                    ratelimit::RateLimit; delimiter::Char=Char(1)) where {T<:IO, H<:MessageHandler}
        m_intmap = Dict{Int64, String}()
        [m_intmap[id] = string(id) for id = 1:9999]
        return new{T, H}(stream, handler, delimiter, header,
                         ClientTasks(), MessageManagement(ratelimit), ReentrantLock(), m_intmap)
    end
end

checksum(fix::String)::Int64 = sum([Int(x) for x in fix]) % 256

function join(fix::OrderedDict{Int64, String}, delimiter::Char)::String 
    join([string(k) * "=" * v for (k, v) in fix], delimiter) * delimiter
end

function join(fix::OrderedDict{Int64, String}, delimiter::Char, IntMap::Dict{Int64, String})::String
    join([IntMap[k] * "=" * v for (k, v) in fix], delimiter) * delimiter
end

function message(fix::Client, msg::Dict{Int64, String})::OrderedDict{Int64, String}
    ordered = OrderedDict{Int64, String}()

    ordered[8] = fix.m_head[8]
    ordered[9] = ""
    ordered[35] = msg[35]
    ordered[49] = fix.m_head[49]
    ordered[56] = fix.m_head[56]
    ordered[34] = get_next_outgoing_message_num(fix)
    ordered[52] = ""

    body_length = 0
    for (k, v) in msg
        if k ≠ 8 && k ≠ 9 && k ≠ 10
            ordered[k] = v
        end
    end

    for (k, v) in ordered
        if k ≠ 8 && k ≠ 9
            body_length += length(string(k)) + 1 + length(v) + 1
        end
    end

    ordered[9] = string(body_length)

    msg = join(ordered, fix.delimiter, fix.m_intmap)
    c = checksum(msg)
    c_str = string(c)
    while length(c_str) < 3
        c_str = '0' * c_str
    end

    ordered[10] = c_str

    return ordered
end

function send_message(fix::Client, msg::Dict{Int64, String})
    lock(fix.m_lock)
    msg = message(fix, msg)
    msg_str = join(msg, fix.delimiter, fix.m_intmap)
    write(fix.stream, msg_str)
    on_sent(fix.m_messages, msg)
    unlock(fix.m_lock)
    return (msg, msg_str)
end

function start(fix::Client)
    function f()
        @async begin
            while true
                incoming = readavailable(fix.stream)
                if isempty(incoming)
                    @printf("[%ls] Empty FIX message\n", now())
                    break
                end

                for (_, msg) in parse(incoming)
                    on_get(fix, msg)
                    on_message(fix.handler, msg)
                end
            end
            @printf("[%ls] FIX: read task done\n", now())
        end
    end

    fix.m_tasks.read = Union{Task(f()), Nothing}
    return fix.m_tasks
end

function close(fix::Client)
    close(fix.stream)
end

function on_get(fix::Client, msg::MessageDictionnary)
    on_get(fix.m_messages, msg)
end

function get_open_orders(fix::Client)::Vector{Dict{Int64, String}}
    return get_open_orders(fix.m_messages)
end

function get_next_outgoing_message_num(fix::Client)
    return get_next_outgoing_message_num(fix.m_messages)
end

function get_position(fix::Client, instrument::String)
    return get_position(fix.m_messages, instrument)
end

function get_positions(fix::Client)
    return get_positions(fix.m_messages)
end

function messages_to_send(fix::Client)
    return messages_remaining(fix.m_messages.outgoing.ratelimit, now())
end

end # module
