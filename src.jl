#=
Create synthetic data set split into multiple .csv files, each with 1,000,000 rows.

If the input is a single, large .csv, it can be split up using:

    mkdir data
    split -l 1000000 data.csv data/data
=#

cd(@__DIR__)

using Combinatorics

function makedata(dir, nstrains=10000, nclusters=1000)
    isdir(dir) || mkdir(dir)

    fname = 1
    lines = 0
    io = open("$(dir)/$(fname).csv", "w")

    for (strain1, strain2) = combinations(1:nstrains, 2)
        write(io, string(strain1), ",", string(strain2), ",",
                  string(strain1%nclusters), ",", string(strain2%nclusters), ",",
                  string(rand()), ",", string(rand()), "\n")

        lines += 1

        if lines == 1_000_000
            close(io)
            fname += 1
            lines = 0
            io = open("$(dir)/$(fname).csv", "w")
        end
    end
end

@time makedata("data")

#=
Load .csv files from a directory using JuliaDB. Splits are loaded one at a time and
saved to a binary format that JuliaDB uses for out-of-core processing. This allows data sets
that will not fit into memory to be processed.
=#

using Distributed; addprocs()
using JuliaDB
@everywhere using Statistics

# Load split csv into JuliaDB. NB only need to do this once.
@time t = loadtable(
    "data",
    output="juliadb",
    indexcols=[:cluster1,:cluster2],
    header_exists=false,
    colnames=["strain1","strain2","cluster1","cluster2","coredist","accdist"],
    )

# Load the data set from disk. NB do this every other time.
t = load("juliadb")

# Calculate mean distances between clusters
@time coredists = groupby(mean, t, (:cluster1, :cluster2), select=:coredist)
