include(joinpath(@__DIR__, "run_azure_put_get_matrix.jl"))

using Profile
using PProf

function profile_main()
    config = CloudBenchVM.load_azure_config()
    credentials = load_azure_credentials(config)
    bucket = load_azure_container(config)
    output_dir = CloudBenchVM.resolve_output_dir(joinpath(VM_DIR, "results"))

    operation = CloudBenchVM.parse_symbol_env("CLOUDBENCH_PROFILE_OPERATION", :get)
    size = CloudBenchVM.parse_int_env("CLOUDBENCH_PROFILE_SIZE", 64 * 1024 * 1024)
    nparts = CloudBenchVM.parse_int_env("CLOUDBENCH_PROFILE_PARTS", 64)
    sem = CloudBenchVM.parse_int_env("CLOUDBENCH_PROFILE_SEMAPHORE", 16)
    allow_multipart = CloudBenchVM.parse_bool_env("CLOUDBENCH_PROFILE_ALLOW_MULTIPART", false)
    prefix = get(ENV, "CLOUDBENCH_PROFILE_PREFIX",
        "profile-$(operation)-$(size_label(size))-s$(sem)-t$(Threads.nthreads())")
    profile_path = get(ENV, "CLOUDBENCH_PROFILE_OUTPUT",
        joinpath(output_dir, "$(prefix)-cpu.txt"))
    pprof_path = get(ENV, "CLOUDBENCH_PROFILE_PPROF_OUTPUT",
        joinpath(output_dir, "$(prefix)-cpu.pb.gz"))
    metrics_path = get(ENV, "CLOUDBENCH_PROFILE_METRICS",
        joinpath(output_dir, "$(prefix)-metrics.txt"))

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

        Profile.clear()
        result = Profile.@profile timed_batch(bucket, credentials, request_options, gate,
            operation === :get ? run_prefix : string(run_prefix, ".profile"),
            operation, data, nparts; allow_multipart, outputs)
        open(profile_path, "w") do io
            Profile.print(io; format=:flat, sortedby=:count, mincount=1)
        end
        PProf.pprof(; web=false, out=pprof_path)
        nbytes, seconds, stats = result
        gbps = ((8 * nbytes) / 1e9) / seconds
        open(metrics_path, "w") do io
            println(io, "operation=", operation)
            println(io, "size_bytes=", size)
            println(io, "nparts=", nparts)
            println(io, "semaphore=", sem)
            println(io, "threads=", Threads.nthreads())
            println(io, "allow_multipart=", allow_multipart)
            println(io, "seconds=", seconds)
            println(io, "gbps=", gbps)
            println(io, "latency_p50_ms=", stats[3])
            println(io, "latency_p99_ms=", stats[5])
            println(io, "profile=", profile_path)
            println(io, "pprof=", pprof_path)
        end
        println("gbps=", gbps)
        println("profile=", profile_path)
        println("pprof=", pprof_path)
        println("metrics=", metrics_path)
    finally
        cleanup_prefix!(bucket, credentials, request_options, gate, prefix)
        transport === nothing || (applicable(close, transport) && close(transport))
    end
    return nothing
end

profile_main()
