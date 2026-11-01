module CloudBenchmarks

using CloudStore, CloudBase, ConcurrentUtilities, HTTP, Mmap, Profile

const VER = "post"

# Process CPU seconds, all threads (CLOCK_PROCESS_CPUTIME_ID: 2 on Linux, 12 on macOS).
function cputime()
    ts = Ref{NTuple{2,Int64}}()
    ccall(:clock_gettime, Cint, (Cint, Ptr{NTuple{2,Int64}}), Sys.isapple() ? 12 : 2, ts) == 0 ||
        error("clock_gettime failed")
    return ts[][1] + ts[][2] / 1e9
end

# A connection pool for one timed batch, like CloudBase 1.6's `CloudPool(limit)`: HTTP/1.1,
# at most `limit` connections to the host (further requests wait), all kept for reuse.
# `CLOUDBENCH_CLIENT=default` uses HTTP's shared default client instead (`nothing`).
function batchclient(limit::Int)
    mode = get(ENV, "CLOUDBENCH_CLIENT", "batch")
    mode == "default" && return nothing
    mode == "batch" || error("CLOUDBENCH_CLIENT must be `batch` or `default`; got $(repr(mode))")
    transport = HTTP.Transport(; max_idle_per_host=limit, max_idle_total=limit, max_conns_per_host=limit)
    return HTTP.Client(; transport, prefer_http2=false)
end

const worker_pool = Pool{Int, Worker}()

@inline _sizes_snapshot() = Dict(size => (label, nparts) for (size, (label, nparts)) in SIZES)

function _install_sizes!(snapshot::AbstractDict{Int, <:Tuple})
    for (size, value) in snapshot
        SIZES[size] = value
    end
    return nothing
end

function runbenchmarks(cloud_machine_specs::String, creds::Union{CloudBase.CloudCredentials, Function}, bucket::CloudBase.AbstractStore;
        nthreads::Vector{Int}=[Threads.nthreads()],
        nworkers::Vector{Int}=[0],
        tls::Vector{Symbol}=[:openssl],
        semaphore_limit::Vector{Int}=[4 * Threads.nthreads()],
        operation::Vector{Symbol}=[:put, :get, :prefetchdownloadstream],
        sizes::Vector{Int}=[2^18, 2^20, 2^21, 2^22, 2^23, 2^26, 2^28, 2^30, 2^32],
        ntimes::Int=3,
        profile::Bool=false,
    )
    results = []
    sizes_snapshot = _sizes_snapshot()
    for nth in nthreads
        # create our worker where we'll run the benchmark from
        if nth == Threads.nthreads()
            # don't need to run on a separate worker
            append!(results, CloudBenchmarks.runbenchmarks(creds, bucket, nth, nworkers, tls, semaphore_limit, operation, sizes, ntimes, profile))
        else
            worker = acquire(worker_pool, nth) do
                w = Worker(; threads=string(nth))
                remote_fetch(w, quote
                    using CloudBenchmarks
                    CloudBenchmarks._install_sizes!($sizes_snapshot)
                end)
                w
            end
            try
                append!(results, remote_fetch(worker, quote
                    CloudBenchmarks.runbenchmarks(
                        $creds, $bucket,
                        $nth,
                        $nworkers,
                        $tls,
                        $semaphore_limit,
                        $operation,
                        $sizes,
                        $ntimes,
                        $profile,
                    )
                end))
            finally
                release(worker_pool, nth, worker)
            end
        end
    end
    file = "$cloud_machine_specs.tsv"
    open(file, "w+") do io
        isempty(results) || println(io, "specs\t", join(keys(first(results)), '\t'))
        for res in results
            write(io, cloud_machine_specs, '-', VER, '\t')
            join(io, values(res), '\t')
            write(io, '\n')
        end
    end
    return file
end

function makeworkers(n, creds, bucket)
    tasks = Task[]
    sizes_snapshot = _sizes_snapshot()
    for _ = 1:n
        push!(tasks, Threads.@spawn begin
            w = Worker(; threads=string(Threads.nthreads()))
            remote_fetch(w, quote
                using CloudBenchmarks
                CloudBenchmarks._install_sizes!($sizes_snapshot)
            end)
            remote_fetch(w, quote
                const credentials = $creds
                const bucket = $bucket
            end)
            w
        end)
    end
    return Worker[fetch(task) for task in tasks]
end

function runbenchmarks(creds::Union{CloudBase.CloudCredentials, Function}, bucket::CloudBase.AbstractStore,
        nthreads::Int,
        nworkers::Vector{Int},
        tls::Vector{Symbol},
        semaphore_limit::Vector{Int},
        operation::Vector{Symbol},
        sizes::Vector{Int},
        ntimes::Int,
        profile::Bool
    )
    results = []
    for nwork in nworkers
        credentials = creds isa Function ? creds() : creds
        workers = makeworkers(nwork, credentials, bucket)
        for type in tls
            (type == :openssl || type == :reseau) || throw(ArgumentError("Reseau-backed benchmarks only support `:openssl`/`:reseau` TLS labels; got `$type`"))
            for sem in semaphore_limit
                for op in operation
                    for sz in sizes
                        push!(results, runbenchmarks(credentials, bucket, nthreads, nwork, type, sem, op, sz, ntimes, profile, workers))
                    end
                end
            end
        end
    end
    return results
end

# [2^20, 2^21, 2^22, 2^23, 2^26, 2^28, 2^30, 2^32]
const SIZES = Dict(
    2^18 => ("256kb", 16384),
    2^20 => ("1mb", 4096),
    2^21 => ("2mb", 2048),
    2^22 => ("4mb", 1024),
    2^23 => ("8mb", 512),
    2^26 => ("64mb", 64),
    2^28 => ("256mb", 16),
    2^30 => ("1gb", 4),
    2^32 => ("4gb", 1),
)

function do_op(credentials, bucket, nm, client, op, data, i)
    if op == :get
        return length(CloudStore.get(bucket, "data.$nm.$i"; credentials, client))
    elseif op == :prefetchdownloadstream
        m = Mmap.mmap(Vector{UInt8}, 2^25)
        len = 0
        io = CloudStore.PrefetchedDownloadStream(bucket, "data.$nm.$i"; credentials, client)
        while !eof(io)
            len += readbytes!(io, m)
        end
        return len
    else
        @assert op == :put
        data = data === nothing ? rand(UInt8, 2^20) : data
        obj = CloudStore.put(bucket, "data.$nm.$i", data; credentials, client)
        return obj.size
    end
end

# Runs `n` operations at once through a fresh `batchclient(semaphore_limit)`, whose
# connection limit bounds the requests in flight. The shared default client has no such
# limit, so with it at most `semaphore_limit` operations run at a time. Returns the bytes
# moved and this process's CPU and GC seconds over the batch.
function do_op_n(credentials, bucket, nm, semaphore_limit, op, n, size, i)
    client = batchclient(semaphore_limit)
    gate = Base.Semaphore(client === nothing ? semaphore_limit : max(1, n))
    data = op == :put ? rand(UInt8, size) : nothing
    cpu0, gc0 = cputime(), Base.gc_time_ns()
    try
        nbytes = Threads.Atomic{Int}(0)
        @sync for j = 1:n
            errormonitor(Threads.@spawn Base.acquire(gate) do
                k = i * n + $j
                len = do_op(credentials, bucket, nm, client, op, data, k)
                len == size || error("$op transferred $len bytes; expected $size")
                Threads.atomic_add!(nbytes, len)
            end)
        end
        return (nbytes[], cputime() - cpu0, (Base.gc_time_ns() - gc0) / 1e9)
    finally
        client === nothing || close(client)
    end
end

median(x) = (s = sort(x); n = length(s); isodd(n) ? s[(n + 1) ÷ 2] : (s[n ÷ 2] + s[n ÷ 2 + 1]) / 2)

function record_sample(config::NamedTuple, repeat::Int, sample::NamedTuple)
    path = get(ENV, "CLOUDBENCH_SAMPLES_FILE", "")
    isempty(path) && return nothing
    mkpath(dirname(path))
    row = (; config..., repeat, sample...)
    open(path, "a+") do io
        if filesize(io) == 0
            println(io, join(keys(row), '\t'))
        end
        println(io, join(values(row), '\t'))
        flush(io)
    end
    return nothing
end

function runbenchmarks(credentials::CloudBase.CloudCredentials, bucket::CloudBase.AbstractStore,
        nthreads::Int,
        nworkers::Int,
        tls::Symbol,
        semaphore_limit::Int,
        operation::Symbol,
        size::Int,
        ntimes::Int,
        profile::Bool,
        workers::Vector{Worker},
    )
    nm, nparts = SIZES[size]
    @info "running benchmark with $nthreads threads, $nworkers workers, $tls tls, $semaphore_limit semaphore limit, $operation operation, $nparts operations on $nm size files"
    function tester()
        # warm up
        futures = []
        for (i, worker) in enumerate(workers)
            push!(futures, remote_eval(worker, quote
                CloudBenchmarks.do_op(credentials, bucket, string("1mb.", $i), nothing, $(Meta.quot(operation)), nothing, 1)
            end))
        end
        # on on coordinator
        do_op(credentials, bucket, "1mb.0", nothing, operation, nothing, 1)
        foreach(fetch, futures)
        empty!(futures)
        @debug "done warming up"
        # done warming up
        start = time_ns()
        n = div(nparts, length(workers) + 1)
        for (i, worker) in enumerate(workers)
            push!(futures, remote_eval(worker, quote
                CloudBenchmarks.do_op_n(credentials, bucket, $nm, $semaphore_limit, $(Meta.quot(operation)), $n, $size, $i)
            end))
        end
        nbytes, cpu, gc = do_op_n(credentials, bucket, nm, semaphore_limit, operation, max(1, n), size, 0)
        for (b, c, g) in map(fetch, futures)
            nbytes += b; cpu += c; gc += g
        end
        wall = Float64(time_ns() - start) / 1e9
        gbits_per_second = nbytes == 0 ? 0 : (((8 * nbytes) / 1e9) / wall)
        @info "single benchmark completed with bandwidth: $(gbits_per_second) Gbps" wall cpu_s=cpu cpu_cores=cpu / wall gc_s=gc
        GC.gc(true)
        GC.gc()
        return (; gbps=gbits_per_second, wall, cpu, gc, nbytes)
    end
    samples = []
    config = (; nthreads, nworkers, tls, semaphore_limit, operation, size_bytes=size)
    if profile
        Profile.clear()
        @profile for repeat = 1:ntimes
            sample = tester()
            push!(samples, sample)
            record_sample(config, repeat, sample)
        end
    else
        for repeat = 1:ntimes
            sample = tester()
            push!(samples, sample)
            record_sample(config, repeat, sample)
        end
    end
    m(f) = median(map(f, samples))
    # `rate` (best Gbps) is the original column; the rest are medians over the `ntimes` samples.
    # CPU and GC time are summed over the coordinator and worker processes.
    return (; nthreads, nworkers, tls, semaphore_limit, operation, size=nm,
        rate=maximum(x -> x.gbps, samples), gbps_median=m(x -> x.gbps), wall_s_median=m(x -> x.wall),
        cpu_s_median=m(x -> x.cpu), cpu_cores_median=m(x -> x.cpu / x.wall),
        cpu_s_per_gb_median=m(x -> x.nbytes == 0 ? 0.0 : x.cpu / (x.nbytes / 1e9)), gc_s_median=m(x -> x.gc))
end

end # module CloudBenchmarks
