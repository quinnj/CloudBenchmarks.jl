module CloudBenchmarks

using CloudStore, CloudBase, HTTP, MbedTLS, OpenSSL, ConcurrentUtilities, Mmap

const VER = "post"

const worker_pool = Pool{Int, Worker}()

function runbenchmarks(cloud_machine_specs::String, creds::CloudBase.CloudCredentials, bucket::CloudBase.AbstractStore;
        nthreads::Vector{Int}=[8, 16, 32, 64],
        nworkers::Vector{Int}=[0, 1, 3],
        tls::Vector{Symbol}=[:mbedtls, :openssl],
        semaphore_limit::Vector{Int}=[16, 32, 64, 128, 256, 512, 1024, 4096],
        operation::Vector{Symbol}=[:put, :get, :prefetchdownloadstream],
        sizes::Vector{Int}=[2^20, 2^21, 2^22, 2^23, 2^26, 2^28, 2^30, 2^32],
        ntimes::Int=3,
    )
    results = []
    for nth in nthreads
        # create our worker where we'll run the benchmark from
        worker = acquire(() -> Worker(; threads=string(nth)), worker_pool, nth)
        try
            remote_fetch(worker, :(using CloudBenchmarks))
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

function makeworkers(n)
    tasks = Task[]
    for _ = 1:n
        push!(tasks, Threads.@spawn begin
            worker = acquire(() -> Worker(; threads=string(Threads.nthreads())), worker_pool, Threads.nthreads())
            try
                remote_fetch(worker, :(using CloudBenchmarks, CloudStore, HTTP, OpenSSL, MbedTLS))
            catch
                release(worker_pool, Threads.nthreads())
                rethrow()
            end
            worker
        end)
    end
    return Worker[fetch(task) for task in tasks]
end

function runbenchmarks(creds::CloudBase.CloudCredentials, bucket::CloudBase.AbstractStore,
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
        workers = makeworkers(nwork)
        try
            for type in tls
                if type == :mbedtls
                    HTTP.SOCKET_TYPE_TLS[] = MbedTLS.SSLContext
                else
                    @assert type == :openssl
                    HTTP.SOCKET_TYPE_TLS[] = OpenSSL.SSLStream
                end
                for sem in semaphore_limit
                    pool = HTTP.Pool(sem)
                    for op in operation
                        for sz in sizes
                            push!(results, runbenchmarks(creds, bucket, nthreads, nwork, type, sem, op, sz, ntimes, workers, pool))
                        end
                    end
                end
            end
        finally
            for worker in workers
                release(worker_pool, Threads.nthreads(), worker)
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
        return length(CloudStore.get(bucket, "data.$nm.$i"; credentials, pool, nowarn=true))
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
        obj = CloudStore.put(bucket, "data.$nm.$i", data; credentials, pool, nowarn=true)
        return obj.size
    end
end

function do_op_n(credentials, bucket, nm, pool, op, n, data, i)
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
        pool::HTTP.Pool,
    )
    nm, nparts = SIZES[size]
    data = operation == :put ? rand(UInt8, size) : nothing
    @info "running benchmark with $nthreads threads, $nworkers workers, $tls tls, $semaphore_limit semaphore limit, $operation operation, $nparts operations on $nm size files"
    function tester()
        # warm up
        futures = []
        for worker in workers
            push!(futures, remote_eval(worker, quote
                op = $(Meta.quot(operation))
                data = op == :put ? rand(UInt8, $size) : nothing
                CloudBenchmarks.do_op($credentials, $bucket, "1mb.1", $pool, op, data, 1)
            end))
        end
        # on on coordinator
        do_op(credentials, bucket, "1mb.1", pool, operation, data, 1)
        foreach(fetch, futures)
        empty!(futures)
        # done warming up
        start = time()
        n = div(nparts, length(workers) + 1)
        for (i, worker) in enumerate(workers)
            push!(futures, remote_eval(worker, quote
                CloudBenchmarks.do_op_n($credentials, $bucket, $nm, $pool, $(Meta.quot(operation)), $n, data, $i)
            end))
        end
        nbytes = do_op_n(credentials, bucket, nm, pool, operation, n, data, 0)
        nbytes = sum(fetch, futures; init=0) + nbytes[]
        stop = time()
        gbits_per_second = nbytes == 0 ? 0 : (((8 * nbytes) / 1e9) / (stop - start))
        @info "single benchmark completed with bandwidth: $(gbits_per_second) Gbps"
        GC.gc(true)
        return gbits_per_second
    end
    curmax = 0.0
    for _ = 1:ntimes
        curmax = max(curmax, tester())
    end
    return (; nthreads, nworkers, tls, semaphore_limit, operation, size, rate=curmax)
end

end # module CloudBenchmarks
