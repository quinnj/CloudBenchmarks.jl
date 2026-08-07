include(joinpath(@__DIR__, "run_azure_put_get_matrix.jl"))

using Profile

function profile_allocs_main()
    config = CloudBenchVM.load_azure_config()
    credentials = load_azure_credentials(config)
    bucket = load_azure_container(config)
    output_dir = CloudBenchVM.resolve_output_dir(joinpath(VM_DIR, "results"))

    operation = CloudBenchVM.parse_symbol_env("CLOUDBENCH_PROFILE_OPERATION", :get)
    size = CloudBenchVM.parse_int_env("CLOUDBENCH_PROFILE_SIZE", 1024 * 1024)
    nparts = CloudBenchVM.parse_int_env("CLOUDBENCH_PROFILE_PARTS", 512)
    sem = CloudBenchVM.parse_int_env("CLOUDBENCH_PROFILE_SEMAPHORE", 128)
    allow_multipart = CloudBenchVM.parse_bool_env("CLOUDBENCH_PROFILE_ALLOW_MULTIPART", false)
    sample_rate = parse(Float64, get(ENV, "CLOUDBENCH_PROFILE_ALLOC_SAMPLE_RATE", "0.01"))
    prefix = get(ENV, "CLOUDBENCH_PROFILE_PREFIX",
        "profile-allocs-$(operation)-$(size_label(size))-s$(sem)-t$(Threads.nthreads())")
    output = get(ENV, "CLOUDBENCH_PROFILE_ALLOCS_OUTPUT",
        joinpath(output_dir, "$(prefix).txt"))

    request_options, transport = transport_options(sem)
    gate = Base.Semaphore(sem)
    data = rand(UInt8, size)
    outputs = operation === :get ? [Vector{UInt8}(undef, size) for _ in 1:nparts] : nothing
    run_prefix = string(prefix, '-', time_ns())
    try
        if operation === :get
            seed_get_inputs!(bucket, credentials, request_options, gate, run_prefix, data,
                nparts; allow_multipart)
        end
        warmup_prefix = operation === :get ? run_prefix : string(run_prefix, ".warmup")
        timed_batch(bucket, credentials, request_options, gate, warmup_prefix, operation,
            data, min(nparts, sem); allow_multipart, outputs)

        Profile.Allocs.clear()
        result = Profile.Allocs.@profile sample_rate=sample_rate timed_batch(
            bucket, credentials, request_options, gate,
            operation === :get ? run_prefix : string(run_prefix, ".profile"),
            operation, data, nparts; allow_multipart, outputs)
        allocs = Profile.Allocs.fetch()
        open(output, "w") do io
            Profile.Allocs.print(io, allocs; format=:flat, sortedby=:count, mincount=1)
        end
        nbytes, seconds, stats = result
        gbps = ((8 * nbytes) / 1e9) / seconds
        sampled_bytes = sum(alloc.size for alloc in allocs.allocs)
        println("gbps=", gbps)
        println("sample_rate=", sample_rate)
        println("sampled_allocations=", length(allocs.allocs))
        println("sampled_bytes=", sampled_bytes)
        println("latency_p50_ms=", stats[3])
        println("latency_p99_ms=", stats[5])
        println("profile=", output)
    finally
        cleanup_prefix!(bucket, credentials, request_options, gate, prefix)
        transport === nothing || (applicable(close, transport) && close(transport))
    end
    return nothing
end

profile_allocs_main()
