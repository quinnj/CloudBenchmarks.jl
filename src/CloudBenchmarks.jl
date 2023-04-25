module CloudBenchmarks

using CloudStore, CloudBase, HTTP, MbedTLS, OpenSSL, ConcurrentUtilities, Mmap

const VER = "post"

const worker_pool = Pool{Int, Worker}()

function runbenchmarks(cloud_machine_specs::String, creds::Union{CloudBase.CloudCredentials, Function}, bucket::CloudBase.AbstractStore;
        nthreads::Vector{Int}=[16, 32, 64],
        nworkers::Vector{Int}=[0],
        tls::Vector{Symbol}=[:mbedtls, :openssl],
        semaphore_limit::Vector{Int}=[512],
        operation::Vector{Symbol}=[:put, :get, :prefetchdownloadstream],
        sizes::Vector{Int}=[2^20, 2^21, 2^22, 2^23, 2^26, 2^28, 2^30, 2^32],
        ntimes::Int=3,
    )
    results = []
    for nth in nthreads
        # create our worker where we'll run the benchmark from
        worker = acquire(worker_pool, nth) do
            w = Worker(; threads=string(nth))
            remote_fetch(w, :(using CloudBenchmarks))
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
                )
            end))
        finally
            release(worker_pool, nth, worker)
        end
    end
    file = "$cloud_machine_specs.tsv"
    open(file, "w+") do io
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
    for _ = 1:n
        push!(tasks, Threads.@spawn begin
            w = Worker(; threads=string(Threads.nthreads()))
            remote_fetch(w, :(using CloudBenchmarks))
            remote_fetch(w, quote
                const credentials = $creds
                const bucket = $bucket
            end)
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
    )
    results = []
    for nwork in nworkers
        credentials = creds isa Function ? creds() : creds
        workers = makeworkers(nwork, credentials, bucket)
        for type in tls
            if type == :mbedtls
                HTTP.SOCKET_TYPE_TLS[] = MbedTLS.SSLContext
            else
                @assert type == :openssl
                HTTP.SOCKET_TYPE_TLS[] = OpenSSL.SSLStream
            end
            for sem in semaphore_limit
                for op in operation
                    for sz in sizes
                        push!(results, runbenchmarks(credentials, bucket, nthreads, nwork, type, sem, op, sz, ntimes, workers))
                    end
                end
            end
        end
    end
    return results
end

# [2^20, 2^21, 2^22, 2^23, 2^26, 2^28, 2^30, 2^32]
const SIZES = Dict(
    2^20 => ("1mb", 4096),
    2^21 => ("2mb", 2048),
    2^22 => ("4mb", 1024),
    2^23 => ("8mb", 512),
    2^26 => ("64mb", 64),
    2^28 => ("256mb", 16),
    2^30 => ("1gb", 4),
    2^32 => ("4gb", 1),
)

function do_op(credentials, bucket, nm, pool, op, data, i)
    if op == :get
        return length(CloudStore.get(bucket, "data.$nm.$i"; credentials, pool, logerrors=true, nowarn=true))
    elseif op == :prefetchdownloadstream
        m = Mmap.mmap(Vector{UInt8}, 2^25)
        len = 0
        io = CloudStore.PrefetchedDownloadStream(bucket, "data.$nm.$i"; credentials, pool, nowarn=true)
        while !eof(io)
            len += readbytes!(io, m)
        end
        return len
    else
        @assert op == :put
        data = data === nothing ? rand(UInt8, 2^20) : data
        obj = CloudStore.put(bucket, "data.$nm.$i", data; credentials, pool, logerrors=true, nowarn=true)
        return obj.size
    end
end

function do_op_n(credentials, bucket, nm, semaphore_limit, op, n, size, i)
    pool = HTTP.Pool(semaphore_limit)
    data = op == :put ? rand(UInt8, size) : nothing
    nbytes = Threads.Atomic{Int}(0)
    @sync for j = 1:n
        Threads.@spawn begin
            k = i * n + $j
            len = do_op(credentials, bucket, nm, pool, op, data, k)
            Threads.atomic_add!(nbytes, len)
        end
    end
    return nbytes[]
end

function runbenchmarks(credentials::CloudBase.CloudCredentials, bucket::CloudBase.AbstractStore,
        nthreads::Int,
        nworkers::Int,
        tls::Symbol,
        semaphore_limit::Int,
        operation::Symbol,
        size::Int,
        ntimes::Int,
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
        start = time()
        n = div(nparts, length(workers) + 1)
        for (i, worker) in enumerate(workers)
            push!(futures, remote_eval(worker, quote
                CloudBenchmarks.do_op_n(credentials, bucket, $nm, $semaphore_limit, $(Meta.quot(operation)), $n, $size, $i)
            end))
        end
        nbytes = do_op_n(credentials, bucket, nm, semaphore_limit, operation, max(1, n), size, 0)
        nbytes = sum(fetch, futures; init=0) + nbytes[]
        stop = time()
        gbits_per_second = nbytes == 0 ? 0 : (((8 * nbytes) / 1e9) / (stop - start))
        @info "single benchmark completed with bandwidth: $(gbits_per_second) Gbps"
        GC.gc(true)
        GC.gc()
        return gbits_per_second
    end
    curmax = 0.0
    for _ = 1:ntimes
        curmax = max(curmax, tester())
    end
    return (; nthreads, nworkers, tls, semaphore_limit, operation, size=nm, rate=curmax)
end

end # module CloudBenchmarks
