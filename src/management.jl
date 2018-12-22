const MessageDictionnary = OrderedDict{Int64, String}

import Base: push!

export RateLimit, isexceeded, messages_remaining

mutable struct Container{T}
    data::T
end

struct RateLimit
    buffer::CircularBuffer{DateTime}
    period::Dates.Second

    function RateLimit(max_messages::Int64, period::Dates.Second)
        return new(CircularBuffer{DateTime}(max_messages), period)
    end
end

function push!(fix::RateLimit, n::DateTime)
    push!(fix.buffer, n)
end

function isexceeded(fix::RateLimit, n::DateTime)
    return n - first(fix) < fix.period
end

function messages_remaining(fix::RateLimit, n::DateTime)
    out = capacity(fix.buffer) - length(fix.buffer)
    for t in fix.buffer
        if n - t > fix.period
            out += 1
        end
    end
    return out
end

struct IncomingMessages
    login::MessageDictionnary
    logout::MessageDictionnary
    execution_reports::Dict{String, Vector{MessageDictionnary}}
    order_cancel_reject::Vector{MessageDictionnary}
    order_reject::Vector{MessageDictionnary}
    resend::Vector{MessageDictionnary}
    heartbeat::Vector{MessageDictionnary}
    msgseqnum::Container{String}
    ratelimit::RateLimit

    function IncomingMessages(ratelimit::RateLimit)
        return new(MessageDictionnary(),
               MessageDictionnary(),
               Dict{String, Vector{MessageDictionnary}}(),
               Vector{MessageDictionnary}(0),
               Vector{MessageDictionnary}(0),
               Vector{MessageDictionnary}(0),
               Vector{MessageDictionnary}(0),
               Container("0"),
               ratelimit)
    end
end

struct OutgoingMessages
    login::MessageDictionnary
    logout::MessageDictionnary
    new_order_single::Dict{String, MessageDictionnary}
    order_cancel_request::Dict{String, MessageDictionnary}
    order_status_request::Vector{MessageDictionnary}
    order_cancel_replace_request::Dict{String, MessageDictionnary}
    msgseqnum::Container{Int64}
    ratelimit::RateLimit

    function OutgoingMessages(ratelimit::RateLimit)
        return new(MessageDictionnary(),
               MessageDictionnary(),
               Dict{String, MessageDictionnary}(),
               Dict{String, MessageDictionnary}(),
               Vector{MessageDictionnary}(0),
               Dict{String, MessageDictionnary}(),
               Container(0),
               ratelimit)
    end
end

const ClientOrderID = String
const ExchangeOrderID = String

struct OpenOrders
    client_id::Dict{String, MessageDictionnary}
    exchange_id::Dict{String, MessageDictionnary}
    verbose::Bool

    function OpenOrders(verbose::Bool=false)
        return new(Dict{String, MessageDictionnary}(), Dict{String, MessageDictionnary}(), verbose)
    end
end

struct MessageManagement
    incoming::IncomingMessages
    outgoing::OutgoingMessages
    open::OpenOrders

    function MessageManagement(ratelimit::RateLimit, verbose::Bool=false)
        return new(IncomingMessages(ratelimit), OutgoingMessages(ratelimit), OpenOrders(verbose))
    end
end

function get_position(fix::IncomingMessages, instrument::String)
    out = 0.0
    if !haskey(fix.execution_reports, "1")
        return out
    end

    for report in fix.execution_reports["1"]
        if report[55]≠ instrument
            continue
        end
        if report[54] == "1"
            out += parse(Float64, report[32])
        else
            out -= parse(Float64, report[32])
        end
    end

    return out
end

function get_positions(fix::IncomingMessages)
    out = Dict{String, Float64}()
    if !haskey(fix.execution_reports, "1")
        return out
    end

    for report in fix.execution_reports["1"]
        if !haskey(out, report[55])
            out[report[55]] = 0.0
        end
        sz = parse(Float64, report[32])
        if report[54] == "1"
            out[report[55]] = out[report[55]] + sz
        else
            out[report[55]] = out[report[55]] - sz
        end
    end

    return out
end

function on_new_message(fix::IncomingMessages, msg::MessageDictionnary)
    fix.msgseqnum.data = msg[34]
    msg_type = msg[35]

    if msg_type == "8"
        add_execution_report(fix, msg)
    elseif msg_type == "9"
        add_order_cancel_reject(fix, msg)
    elseif msg_type == "3"
        @printf("[%ls][FIX:ER] received order reject. Reason: %ls\n", now(), msg[58])
        add_order_reject(fix, msg)
    elseif msg_type == "0"
        add_heartbeat(fix, msg)
    elseif msg_type == "A"
        add_login(fix, msg)
    elseif msg_type == "5"
        add_logout(fix, msg)
    elseif msg_type == "2"
        add_resend(fix, msg)
    else
        @printf("[%ls] Unknown incoming message type: %ls\n", now(), msg_type)
    end
end

function add_login(fix::IncomingMessages, msg::MessageDictionnary)
    [fix.login[k] = v for (k, v) in msg]
end

function add_logout(fix::IncomingMessages, msg::MessageDictionnary)
    [fix.logout[k] = v for (k, v) in msg]
end

function add_execution_report(fix::IncomingMessages, msg::MessageDictionnary)
    exec_type = msg[150]
    if !haskey(fix.execution_reports, exec_type)
        fix.execution_reports[exec_type] = MessageDictionnary[]
    end
    push!(fix.execution_reports[msg[150]], msg)
end

function add_order_cancel_reject(fix::IncomingMessages, msg::MessageDictionnary)
    push!(fix.order_cancel_reject, msg)
end

function add_order_reject(fix::IncomingMessages, msg::MessageDictionnary)
    push!(fix.order_reject, msg)
end

function add_heartbeat(fix::IncomingMessages, msg::MessageDictionnary)
    push!(fix.heartbeat, msg)
end

function add_resend(fix::IncomingMessages, msg::MessageDictionnary)
    push!(fix.resend, msg)
end

function on_new_message(fix::OutgoingMessages, msg::MessageDictionnary)
    msg_type = msg[35]
    fix.msgseqnum.data = parse(Int64, msg[34])

    if msg_type == "D"
        add_new_order(fix, msg)
    elseif msg_type == "F"
        add_order_cancel_request(fix, msg)
    elseif msg_type == "H"
        add_orde_status_request(fix, msg)
    elseif msg_type == "A"
        add_login(fix, msg)
    elseif msg_type == "5"
        add_logout(fix, msg)
    elseif msg_type == "G"
        add_cancel_replace(fix, msg)
    else
        @printf("[%ls] Unknown outgoing message type: %ls\n", now(), msg_type)
    end

    return nothing
end

function add_login(fix::OutgoingMessages, msg::MessageDictionnary)
    [fix.login[k] = v for (k, v) in msg]
end

function add_logout(fix::OutgoingMessages, msg::MessageDictionnary)
    [fix.logout[k] = v for (k, v) in msg]
end

function add_new_order(fix::OutgoingMessages, msg::MessageDictionnary)
    fix.new_order_single[msg[11]] = msg
end

function add_order_cancel_request(fix::OutgoingMessages, msg::MessageDictionnary)
    fix.order_cancel_request[msg[11]] = msg
end

function add_orde_status_request(fix::OutgoingMessages, msg::MessageDictionnary)
    push!(fix.order_status_request, msg)
end

function add_cancel_replace(fix::OutgoingMessages, msg::MessageDictionnary)
    fix.order_cancel_replace_request[msg[1]]
end

function on_sent(fix::MessageManagement, msg::MessageDictionnary)
    on_new_message(fix.outgoing, msg)
    push!(fix.outgoing.ratelimit, now())
    return nothing
end

function on_get(fix::MessageManagement, msg::MessageDictionnary)
    on_new_message(fix.incoming, msg)

    if msg[35] == "8"
        on_execution_report(fix.open, msg)
    end

    return nothing
end

function on_cancel_reject(fix::MessageManagement, msg::MessageDictionnary)
    add_order_cancel_reject(fix.incoming, msg)
end

function on_order_reject(fix::MessageManagement, msg::MessageDictionnary)
    add_order_reject(fix.incoming, msg)
end

function on_new_order(fix::OpenOrders, msg::MessageDictionnary)
    fix.client_id[msg[11]] = msg
    fix.exchange_id[msg[37]] = msg
    return nothing
end

function on_fill(fix::OpenOrders, msg::MessageDictionnary)
    exchange_id = msg[37]
    filled = msg[32]
    orig_exec = get_order_by_exch_id(fix, exchange_id)
    prev_amount = parse(Float64, orig_exec[38])
    filled_amount = parse(Float64, filled)
    cur_amount = prev_amount - filled_amount
    orig_exec[38] = string(cur_amount)
end

function on_done(fix::OpenOrders, msg::MessageDictionnary)
    delete_order_by_exch_id(fix, msg[37])
end

function on_cancel(fix::OpenOrders, msg::MessageDictionnary)
    delete_order_by_exch_id(fix, msg[37])
end

function on_reject(fix::OpenOrders, msg::MessageDictionnary)
    delete_order_by_exch_id(fix, msg[37])
end

function get_order_by_exch_id(fix::OpenOrders, exchange_id::String)
    return fix.exchange_id[exchange_id]
end

function has_order_by_client_id(fix::OpenOrders, client_id::String)
    return haskey(fix.client_id, client_id)
end

function has_order_by_exch_id(fix::OpenOrders, exchange_id::String)
    return haskey(fix.exchange_id, exchange_id)
end

function delete_order_by_exch_id(fix::OpenOrders, exchange_id::String)::Void
    if !has_order_by_exch_id(fix, exchange_id)
        return nothing
    end

    orig_exec = get_order_by_exch_id(fix, exchange_id)

    client_id = orig_exec[11]
    exchange_id = orig_exec[37]

    delete!(fix.client_id, client_id)
    delete!(fix.exchange_id, exchange_id)

    return nothing
end

function on_execution_report(fix::OpenOrders, msg::MessageDictionnary)
    exec_type = msg[150]

    if exec_type == "0"
        fix.verbose && @printf("[%ls][FIX:ER] new order\n", now())
        on_new_order(fix, msg)
    elseif exec_type == "1" #fill
        fix.verbose && @printf("[%ls][FIX:ER] fill\n", now())
        on_fill(fix, msg)
    elseif exec_type == "3"
        fix.verbose && @printf("[%ls][FIX:ER] done\n", now())
        on_done(fix, msg)
    elseif exec_type == "4"
        fix.verbose && @printf("[%ls][FIX:ER] cancel\n", now())
        on_cancel(fix, msg)
    elseif exec_type == "8"
        on_reject(fix, msg)
        fix.verbose && @printf("[%ls][FIX:ER] reject\n", now())
    else
        @printf("Received unhandled execution report of type %ls\n", exec_type)
    end

end

function get_order(fix::OpenOrders)::Vector{MessageDictionnary}
    return collect(values(fix.client_id))
end

function get_open_orders(fix::MessageManagement)::Vector{MessageDictionnary}
    return get_order(fix.open)
end

function get_next_outgoing_message_num(fix::MessageManagement)::String
    return get_next_outgoing_message_num(fix.outgoing)
end

function get_next_outgoing_message_num(fix::OutgoingMessages)::String
    return string(fix.msgseqnum.data + 1)
end

function get_position(fix::MessageManagement, instrument::String)
    return get_position(fix.incoming, instrument)
end

function get_positions(fix::MessageManagement)
    return get_positions(fix.incoming)
end
