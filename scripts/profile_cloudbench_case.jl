using CloudBenchVM

const ROOT_DIR = normpath(joinpath(@__DIR__, ".."))
const VM_DIR = joinpath(ROOT_DIR, "vm")
const ROOT_ENV_FILE = joinpath(ROOT_DIR, ".env")
provider_name = CloudBenchVM.provider_name("azure")
env_file = get(ENV, "CLOUDBENCH_ENV_FILE", CloudBenchVM.default_env_file(VM_DIR, provider_name))
CloudBenchVM.load_env_file!(ROOT_ENV_FILE; optional=true)
env_file != ROOT_ENV_FILE && CloudBenchVM.load_env_file!(env_file; optional=true)
CloudBenchVM.instantiate_project!(VM_DIR)

using CloudBenchmarks
using CloudBase
using CloudStore
using Profile

function load_gcp_credentials(config::CloudBenchVM.GCPConfig)::CloudBase.GCP.Credentials
    if config.access_token !== nothing
        if config.quota_project_id === nothing
            return CloudBase.GCP.Credentials(config.access_token::String)
        end
        return CloudBase.GCP.Credentials(config.access_token::String; quota_project_id=config.quota_project_id::String)
    end
    return CloudBase.GCP.Credentials(; application_credentials_file=config.credentials_file::String)
end

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

function ensure_size_entry!(size::Int, nparts::Int)
    if haskey(CloudBenchmarks.SIZES, size)
        CloudBenchmarks.SIZES[size] = (CloudBenchmarks.SIZES[size][1], nparts)
        return CloudBenchmarks.SIZES[size][1]
    end
    label = string(size)
    CloudBenchmarks.SIZES[size] = (label, nparts)
    return label
end

sanitize_label(label::AbstractString) = replace(lowercase(String(label)), r"[^a-z0-9]+" => "-")

function case_prefix(provider::String, operation::Symbol, size::Int, semaphore_limit::Int)::String
    size_name = haskey(CloudBenchmarks.SIZES, size) ? CloudBenchmarks.SIZES[size][1] : string(size)
    return "prof-$(provider)-$(sanitize_label(size_name))-$(sanitize_label(String(operation)))-t$(Threads.nthreads())-s$(semaphore_limit)"
end

function run_case(credentials, store, nworkers::Int, tls::Symbol, semaphore_limit::Int, operation::Symbol, size::Int, ntimes::Int)
    result = only(CloudBenchmarks.runbenchmarks(
        credentials,
        store,
        Threads.nthreads(),
        [nworkers],
        [tls],
        [semaphore_limit],
        [operation],
        [size],
        ntimes,
        false,
    ))
    return result.rate
end

function first_frame_label(alloc)::String
    for frame in alloc.stacktrace
        frame.from_c && continue
        return string(basename(String(frame.file)), ':', frame.line, ' ', frame.func)
    end
    return "<unknown>"
end

function write_alloc_summary(path::AbstractString, alloc_results)
    totals = Dict{String, Tuple{Int, Int}}()
    for alloc in alloc_results.allocs
        label = first_frame_label(alloc)
        bytes, samples = get(() -> (0, 0), totals, label)
        totals[label] = (bytes + alloc.size, samples + 1)
    end
    rows = collect(totals)
    sort!(rows; by=x -> first(x.second), rev=true)
    open(path, "w") do io
        println(io, "bytes\tsamples\tframe")
        for (label, (bytes, samples)) in Iterators.take(rows, 100)
            println(io, bytes, '\t', samples, '\t', label)
        end
    end
    return nothing
end

function write_metrics(path::AbstractString; metrics...)
    open(path, "w") do io
        for (key, value) in metrics
            println(io, key, '=', value)
        end
    end
    return nothing
end

function main()
    provider = lowercase(get(ENV, "CLOUDBENCH_PROVIDER", "azure"))
    operation = CloudBenchVM.parse_symbol_env("CLOUDBENCH_PROFILE_OPERATION", :get)
    size = CloudBenchVM.parse_int_env("CLOUDBENCH_PROFILE_SIZE", 2^20)
    nworkers = CloudBenchVM.parse_int_env("CLOUDBENCH_PROFILE_NWORKERS", 0)
    tls = CloudBenchVM.parse_symbol_env("CLOUDBENCH_PROFILE_TLS", :reseau)
    semaphore_limit = CloudBenchVM.parse_int_env("CLOUDBENCH_PROFILE_SEMAPHORE_LIMIT", 4 * Threads.nthreads())
    ntimes = CloudBenchVM.parse_int_env("CLOUDBENCH_PROFILE_NTIMES", 3)
    nparts = CloudBenchVM.parse_int_env("CLOUDBENCH_PROFILE_PARTS", get(CloudBenchmarks.SIZES, size, (string(size), 1))[2])
    size_name = ensure_size_entry!(size, nparts)
    prefix = get(ENV, "CLOUDBENCH_PROFILE_PREFIX", case_prefix(provider, operation, size, semaphore_limit))
    machine_specs = get(ENV, "CLOUDBENCH_MACHINE_SPECS", CloudBenchVM.default_machine_specs("profile"))
    output_dir = CloudBenchVM.resolve_output_dir(joinpath(VM_DIR, "results"))
    cpu_path = joinpath(output_dir, "$(prefix)-cpu.txt")
    wall_path = joinpath(output_dir, "$(prefix)-wall.txt")
    alloc_path = joinpath(output_dir, "$(prefix)-alloc.txt")
    alloc_summary_path = joinpath(output_dir, "$(prefix)-alloc-summary.tsv")
    metrics_path = joinpath(output_dir, "$(prefix)-metrics.txt")

    println("provider=", provider)
    println("machine_specs=", machine_specs)
    println("operation=", operation)
    println("size=", size_name)
    println("nparts=", nparts)
    println("threads=", Threads.nthreads())
    println("nworkers=", nworkers)
    println("tls=", tls)
    println("semaphore_limit=", semaphore_limit)
    println("ntimes=", ntimes)

    provider_config = nothing
    if provider == "gcp"
        provider_config = CloudBenchVM.load_gcp_config()
    elseif provider == "azure"
        provider_config = CloudBenchVM.load_azure_config()
    else
        error("unsupported CLOUDBENCH_PROVIDER=$(repr(provider)); expected `gcp` or `azure`")
    end
    CloudBenchVM.parse_bool_env("CLOUDBENCH_DRY_RUN", false) && return nothing

    credentials = nothing
    store = nothing
    cleanup = nothing
    if provider == "gcp"
        credentials = load_gcp_credentials(provider_config::CloudBenchVM.GCPConfig)
        store = CloudBase.GCP.Bucket((provider_config::CloudBenchVM.GCPConfig).bucket_name)
    else
        credentials = load_azure_credentials(provider_config::CloudBenchVM.AzureConfig)
        store = load_azure_container(provider_config::CloudBenchVM.AzureConfig)
        if (provider_config::CloudBenchVM.AzureConfig).generated_container
            create_container!(store::CloudBase.Azure.Container, credentials::CloudBase.Azure.Credentials)
            cleanup = () -> cleanup_container!(store::CloudBase.Azure.Container, credentials::CloudBase.Azure.Credentials)
        end
    end

    warmup_rate = 0.0
    cpu_rate = 0.0
    wall_rate = 0.0
    alloc_rate = 0.0
    try
        warmup_rate = run_case(credentials, store, nworkers, tls, semaphore_limit, operation, size, 1)

        Profile.clear()
        cpu_rate = @profile run_case(credentials, store, nworkers, tls, semaphore_limit, operation, size, ntimes)
        open(cpu_path, "w") do io
            Profile.print(io; format=:flat, sortedby=:count)
        end

        Profile.clear()
        wall_rate = Profile.@profile_walltime run_case(credentials, store, nworkers, tls, semaphore_limit, operation, size, ntimes)
        open(wall_path, "w") do io
            Profile.print(io; format=:flat, sortedby=:count)
        end

        Profile.Allocs.clear()
        alloc_rate = Profile.Allocs.@profile sample_rate=1.0 run_case(credentials, store, nworkers, tls, semaphore_limit, operation, size, ntimes)
        alloc_results = Profile.Allocs.fetch()
        open(alloc_path, "w") do io
            Profile.Allocs.print(io, alloc_results; format=:flat, sortedby=:count)
        end
        write_alloc_summary(alloc_summary_path, alloc_results)
    finally
        cleanup === nothing || cleanup()
    end

    write_metrics(
        metrics_path;
        provider,
        machine_specs,
        operation,
        size=size_name,
        nparts,
        threads=Threads.nthreads(),
        nworkers,
        tls,
        semaphore_limit,
        ntimes,
        warmup_rate_gbps=warmup_rate,
        cpu_profiled_rate_gbps=cpu_rate,
        wall_profiled_rate_gbps=wall_rate,
        alloc_profiled_rate_gbps=alloc_rate,
        cpu_profile=cpu_path,
        wall_profile=wall_path,
        alloc_profile=alloc_path,
        alloc_summary=alloc_summary_path,
    )

    println("cpu_profile=", cpu_path)
    println("wall_profile=", wall_path)
    println("alloc_profile=", alloc_path)
    println("alloc_summary=", alloc_summary_path)
    println("metrics=", metrics_path)
    return nothing
end

main()
