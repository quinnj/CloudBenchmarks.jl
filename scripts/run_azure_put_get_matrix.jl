const ROOT_DIR = normpath(joinpath(@__DIR__, ".."))
const VM_DIR = joinpath(ROOT_DIR, "vm")
const ROOT_ENV_FILE = joinpath(ROOT_DIR, ".env")

include(joinpath(VM_DIR, "src", "CloudBenchVM.jl"))
using .CloudBenchVM

CloudBenchVM.load_env_file!(ROOT_ENV_FILE; optional=true)
const ENV_FILE = get(ENV, "CLOUDBENCH_ENV_FILE", CloudBenchVM.default_env_file(VM_DIR, "azure"))
ENV_FILE != ROOT_ENV_FILE && CloudBenchVM.load_env_file!(ENV_FILE; optional=true)

using CloudBase
using CloudStore
using Dates
using HTTP
using Pkg
using Random

function load_azure_credentials(config::CloudBenchVM.AzureConfig)::CloudBase.Azure.Credentials
    if config.key !== nothing
        return CloudBase.Azure.Credentials(config.account, config.key::String)
    end
    return CloudBase.Azure.Credentials(config.token::String)
end

function load_azure_container(config::CloudBenchVM.AzureConfig)
    if config.host === nothing
        return CloudBase.Azure.Container(config.container_name, config.account)
    end
    return CloudBase.Azure.Container(config.container_name, config.account; host=config.host::String)
end

function create_container!(container::CloudBase.Azure.Container, credentials::CloudBase.Azure.Credentials)
    resp = CloudBase.Azure.put("$(container.baseurl)?restype=container", String[]; credentials, status_exception=false)
    resp.status in (201, 202, 409) || error("failed to create Azure container $(container.name); status=$(resp.status)")
    return nothing
end

function delete_container!(container::CloudBase.Azure.Container, credentials::CloudBase.Azure.Credentials)
    resp = CloudBase.Azure.delete("$(container.baseurl)?restype=container"; credentials, status_exception=false)
    resp.status in (202, 404) || error("failed to delete Azure container $(container.name); status=$(resp.status)")
    return nothing
end

function cleanup_container!(container::CloudBase.Azure.Container, credentials::CloudBase.Azure.Credentials)
    for object in CloudStore.list(container; credentials)
        CloudStore.delete(object; credentials, nowarn=true)
    end
    delete_container!(container, credentials)
    return nothing
end

function split_bool_csv(name::String, default::Vector{Bool})
    raw = strip(get(ENV, name, ""))
    isempty(raw) && return copy(default)
    values = Bool[]
    for item in CloudBenchVM.split_csv(raw)
        value = lowercase(item)
        value in ("1", "true", "yes", "y", "on", "multipart") && (push!(values, true); continue)
        value in ("0", "false", "no", "n", "off", "single") && (push!(values, false); continue)
        error("invalid boolean/mode value for $name: $(repr(item))")
    end
    return values
end

function size_label(size::Int)::String
    units = ((1 << 30, "gb"), (1 << 20, "mb"), (1 << 10, "kb"))
    for (scale, suffix) in units
        size % scale == 0 && return string(size ÷ scale, suffix)
    end
    return string(size, "b")
end

function package_info(name::String)
    for pkg in values(Pkg.dependencies())
        pkg.name == name || continue
        version = pkg.version === nothing ? "unknown" : string(pkg.version)
        tree = pkg.tree_hash === nothing ? "none" : string(pkg.tree_hash)
        source = pkg.source === nothing ? "none" : string(pkg.source)
        return (; version, tree, source)
    end
    return (; version="missing", tree="none", source="none")
end

function print_stack(stack_label::String)
    println("stack=", stack_label)
    println("julia=", VERSION)
    println("threads=", Threads.nthreads())
    for name in ("HTTP", "Reseau", "CloudBase", "CloudStore")
        info = package_info(name)
        println("package=", name, " version=", info.version, " tree=", info.tree,
            " source=", info.source)
    end
    return nothing
end

function probe_protocol(container, credentials)::String
    resp = CloudBase.Azure.get(container.baseurl;
        query=Dict("restype" => "container", "comp" => "list"),
        credentials, status_exception=false)
    if hasproperty(resp, :proto_major) && hasproperty(resp, :proto_minor)
        return string(getproperty(resp, :proto_major), '.', getproperty(resp, :proto_minor))
    elseif hasproperty(resp, :version)
        return string(getproperty(resp, :version))
    end
    return "unknown"
end

function do_one(bucket, credentials, request_options::NamedTuple, prefix, op::Symbol, data, i::Int;
        allow_multipart::Bool, output=nothing)
    key = string(prefix, ".", i)
    if op === :put
        obj = CloudStore.put(bucket, key, data; credentials, request_options...,
            allowMultipart=allow_multipart, logerrors=true)
        return Int(obj.size)
    elseif op === :get
        output === nothing && (output = Vector{UInt8}(undef, length(data)))
        bytes = CloudStore.get(bucket, key, output; credentials, request_options...,
            allowMultipart=allow_multipart, logerrors=true)
        return length(bytes)
    end
    error("unsupported matrix operation $(repr(op)); expected :put or :get")
end

function with_gate(f, gate::Base.Semaphore)
    Base.acquire(gate)
    try
        return f()
    finally
        Base.release(gate)
    end
end

function seed_get_inputs!(bucket, credentials, request_options, gate, prefix::String, data,
        nparts::Int; allow_multipart::Bool)
    @sync for i in 1:nparts
        Threads.@spawn with_gate(gate) do
            do_one(bucket, credentials, request_options, prefix, :put, data, $i; allow_multipart)
        end
    end
    return nothing
end

function latency_stats(latencies_ms::Vector{Float64})
    ordered = sort(latencies_ms)
    percentile(p) = ordered[clamp(ceil(Int, p * length(ordered)), 1, length(ordered))]
    return (
        sum(ordered) / length(ordered),
        first(ordered),
        percentile(0.50),
        percentile(0.95),
        percentile(0.99),
        last(ordered),
    )
end

function timed_batch(bucket, credentials, request_options, gate, prefix, op, data, nparts;
        allow_multipart, outputs=nothing)
    nbytes = Threads.Atomic{Int}(0)
    latencies_ms = Vector{Float64}(undef, nparts)
    started = time_ns()
    @sync for i in 1:nparts
        Threads.@spawn begin
            n, latency_ms = with_gate(gate) do
                request_started = time_ns()
                output = outputs === nothing ? nothing : outputs[$i]
                transferred = do_one(bucket, credentials, request_options, prefix, op, data, $i;
                    allow_multipart, output)
                return transferred, Float64(time_ns() - request_started) / 1e6
            end
            latencies_ms[$i] = latency_ms
            Threads.atomic_add!(nbytes, n)
        end
    end
    seconds = Float64(time_ns() - started) / 1e9
    return nbytes[], seconds, latency_stats(latencies_ms)
end

function cleanup_prefix!(bucket, credentials, request_options, gate, prefix::String)
    objects = CloudStore.list(bucket; prefix, credentials, request_options...)
    @sync for object in objects
        Threads.@spawn with_gate(gate) do
            CloudStore.delete(object; credentials, request_options..., logerrors=true)
        end
    end
    return length(objects)
end

function transport_options(sem::Int)
    if isdefined(HTTP, :Client) && isdefined(HTTP, :Transport)
        transport = getproperty(HTTP, :Transport)(;
            max_idle_per_host=sem,
            max_idle_total=sem,
            max_conns_per_host=sem,
        )
        client = getproperty(HTTP, :Client)(; transport)
        return (; client), client
    elseif isdefined(HTTP, :Pool)
        pool = getproperty(HTTP, :Pool)(sem)
        return (; pool), pool
    end
    return NamedTuple(), nothing
end

function run_cell!(io, bucket, credentials, run_id::String, account::String, container::String,
        stack_label::String, protocol::String, sem::Int, allow_multipart::Bool, op::Symbol,
        size::Int, nparts::Int, repeats::Int, warmup_parts::Int, cleanup_objects::Bool)
    request_options, transport = transport_options(sem)
    gate = Base.Semaphore(sem)
    try
        data = rand(UInt8, size)
        outputs = op === :get ? [Vector{UInt8}(undef, size) for _ in 1:nparts] : nothing
        mode = allow_multipart ? "multipart" : "single"
        label = size_label(size)
        prefix = "$(run_id).s$(sem).$(mode).$(label)"
        if op === :get
            seed_get_inputs!(bucket, credentials, request_options, gate, prefix, data, nparts;
                allow_multipart)
        end

        nwarm = min(warmup_parts, nparts)
        if nwarm > 0
            warmup_prefix = op === :get ? prefix : string(prefix, ".warmup")
            timed_batch(bucket, credentials, request_options, gate, warmup_prefix, op, data,
                nwarm; allow_multipart, outputs)
        end

        rates = Float64[]
        for repeat in 1:repeats
            GC.gc(false)
            repeat_prefix = op === :get ? prefix : string(prefix, ".r", repeat)
            nbytes, seconds, stats = timed_batch(
                bucket, credentials, request_options, gate, repeat_prefix, op, data, nparts;
                allow_multipart, outputs)
            gbps = ((8 * nbytes) / 1e9) / seconds
            push!(rates, gbps)
            row = (
                run_id,
                stack_label,
                VERSION,
                protocol,
                account,
                container,
                Threads.nthreads(),
                sem,
                mode,
                allow_multipart,
                op,
                label,
                size,
                nparts,
                repeat,
                nbytes,
                seconds,
                gbps,
                stats...,
            )
            println(io, join(row, '\t'))
            flush(io)
            @info "matrix repeat" stack_label protocol threads=Threads.nthreads() sem mode op label nparts repeat gbps seconds latency_p50_ms=stats[3] latency_p99_ms=stats[5]
        end
        @info "matrix cell complete" stack_label sem mode op label nparts median_gbps=sort(rates)[cld(length(rates), 2)] max_gbps=maximum(rates)
        return rates
    finally
        if cleanup_objects
            deleted = cleanup_prefix!(bucket, credentials, request_options, gate,
                "$(run_id).s$(sem).")
            @info "matrix cell cleanup" sem deleted
        end
        transport === nothing || (applicable(close, transport) && close(transport))
    end
end

function main()
    config = CloudBenchVM.load_azure_config()
    credentials = load_azure_credentials(config)
    bucket = load_azure_container(config)
    output_dir = CloudBenchVM.resolve_output_dir(joinpath(VM_DIR, "results"))
    stamp = Dates.format(Dates.now(Dates.UTC), dateformat"yyyymmddTHHMMSSZ")
    run_prefix = get(ENV, "CLOUDBENCH_MATRIX_PREFIX", "azure-matrix")
    run_id = "$(run_prefix)-$(stamp)-t$(Threads.nthreads())"
    output = get(ENV, "CLOUDBENCH_MATRIX_OUTPUT", joinpath(output_dir, "$(run_id).tsv"))
    stack_label = get(ENV, "CLOUDBENCH_STACK_LABEL", "unspecified")

    sems = CloudBenchVM.parse_int_vector_env("CLOUDBENCH_MATRIX_SEMAPHORES", [Threads.nthreads()])
    sizes = CloudBenchVM.parse_int_vector_env("CLOUDBENCH_MATRIX_SIZES", [64 * 1024 * 1024])
    nparts = CloudBenchVM.parse_int_env("CLOUDBENCH_MATRIX_PARTS", 8)
    repeats = CloudBenchVM.parse_int_env("CLOUDBENCH_MATRIX_REPEATS", 3)
    warmup_parts = CloudBenchVM.parse_int_env("CLOUDBENCH_MATRIX_WARMUP_PARTS", min(nparts, 8))
    modes = split_bool_csv("CLOUDBENCH_MATRIX_ALLOW_MULTIPART", [true, false])
    ops = CloudBenchVM.parse_symbol_vector_env("CLOUDBENCH_MATRIX_OPERATIONS", [:put, :get])
    keep_container = CloudBenchVM.parse_bool_env(["CLOUDBENCH_KEEP_STORE", "CLOUDBENCH_AZURE_KEEP_CONTAINER"], false)
    cleanup_objects = CloudBenchVM.parse_bool_env("CLOUDBENCH_MATRIX_CLEANUP_OBJECTS", true)

    nparts > 0 || error("CLOUDBENCH_MATRIX_PARTS must be positive")
    repeats > 0 || error("CLOUDBENCH_MATRIX_REPEATS must be positive")
    warmup_parts >= 0 || error("CLOUDBENCH_MATRIX_WARMUP_PARTS must not be negative")
    all(>(0), sems) || error("all semaphore limits must be positive")
    all(>(0), sizes) || error("all object sizes must be positive")

    print_stack(stack_label)
    println("matrix_output=", output)
    println("account=", config.account)
    println("container=", bucket.name)
    println("generated_container=", config.generated_container)
    println("semaphores=", join(sems, ','))
    println("sizes=", join(sizes, ','))
    println("parts=", nparts)
    println("repeats=", repeats)
    println("warmup_parts=", warmup_parts)
    println("allow_multipart=", join(modes, ','))
    println("operations=", join(ops, ','))
    CloudBenchVM.parse_bool_env("CLOUDBENCH_DRY_RUN", false) && return nothing

    if config.generated_container
        create_container!(bucket, credentials)
    end
    try
        protocol = probe_protocol(bucket, credentials)
        println("protocol=", protocol)
        open(output, "w") do io
            println(io, "run_id\tstack\tjulia\tprotocol\taccount\tcontainer\tthreads\tsem\tmode\tallow_multipart\top\tsize_label\tsize_bytes\tnparts\trepeat\tnbytes\tseconds\tgbps\tlatency_mean_ms\tlatency_min_ms\tlatency_p50_ms\tlatency_p95_ms\tlatency_p99_ms\tlatency_max_ms")
            for sem in sems, allow_multipart in modes, op in ops, size in sizes
                run_cell!(io, bucket, credentials, run_id, config.account, bucket.name,
                    stack_label, protocol, sem, allow_multipart, op, size, nparts,
                    repeats, warmup_parts, cleanup_objects)
            end
        end
    finally
        if config.generated_container && !keep_container
            cleanup_container!(bucket, credentials)
        end
    end

    println("results=", output)
    return nothing
end

abspath(PROGRAM_FILE) == abspath(@__FILE__) && main()
