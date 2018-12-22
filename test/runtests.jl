using FIX
using Test

import DataStructures: OrderedDict

@test checksum("123ioн") == 171

sample = OrderedDict(1 => "g", 2=> "h")

@test join(sample, Char(1)) == "1=g" * Char(1) * "2=h" * Char(1)

@test convert(TAGS_INT_STRING, sample) == Dict("AdvId" => "h", "Account" => "g")
