# FIX.jl

A basic implementation of the `Financial Information eXchange` protocol in Julia.

## Installation
```julia
(v1.0) pkg> add https://github.com/brilhana/FIX.jl
```

## Usage
```julia
julia> using FIX, MbedTLS
julia> sock = MbedTLS.connect("127.0.0.1", 1498)
julia> client = Client(sock, message_handler, header, ratelimit)
```
