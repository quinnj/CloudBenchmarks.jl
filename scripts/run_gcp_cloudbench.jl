using Dates
using CloudBenchmarks
using CloudBase
using Sockets

const DEFAULT_OPERATIONS = [:put, :get, :prefetchdownloadstream]
const DEFAULT_SIZES = [2^18, 2^20, 2^21, 2^22, 2^23, 2^26, 2^28, 2^30, 2^32]
const SMOKE_SIZES = [2^20]
const SMOKE_PARTS = Dict(2^20 => 4)

function getenv_first(names::Vector{String})::Union{Nothing, String}
    for name in names
        value = get(ENV, name, "")
        isempty(value) || return value
    end
    return nothing
end

function require_env(names::Vector{String})::String
    value = getenv_first(names)
    value === nothing && error("missing required environment variable; set one of $(join(names, ", "))")
    return value
end

function split_csv(value::AbstractString)::Vector{String}
    parts = String[]
    for part in split(value, ',')
        stripped = strip(part)
        isempty(stripped) || push!(parts, stripped)
    end
    return parts
end

function parse_int_vector_env(name::String, default::Vector{Int})::Vector{Int}
    raw = get(ENV, name, "")
    isempty(raw) && return copy(default)
    return parse.(Int, split_csv(raw))
end

function parse_symbol_vector_env(name::String, default::Vector{Symbol})::Vector{Symbol}
    raw = get(ENV, name, "")
    isempty(raw) && return copy(default)
    return Symbol.(split_csv(raw))
end

function parse_bool_env(name::String, default::Bool)::Bool
    raw = lowercase(strip(get(ENV, name, "")))
    isempty(raw) && return default
    raw in ("1", "true", "yes", "y", "on") && return true
    raw in ("0", "false", "no", "n", "off") && return false
    error("invalid boolean value for $name: $(repr(raw))")
end

function default_machine_specs()::String
    host = gethostname()
    date = Dates.format(Dates.now(Dates.UTC), dateformat"yyyymmdd-HHMMSS")
    return string(host, "-", date)
end

function maybe_token_credentials()::Union{Nothing, CloudBase.GCP.Credentials}
    token = getenv_first(["CLOUDBASE_GCP_LIVE_ACCESS_TOKEN", "CLOUDBENCH_GCP_ACCESS_TOKEN"])
    token === nothing && return nothing
    quota_project_id = get(ENV, "CLOUDBENCH_GCP_QUOTA_PROJECT", "")
    return CloudBase.GCP.Credentials(token; quota_project_id=quota_project_id)
end

function load_credentials()::CloudBase.GCP.Credentials
    token_creds = maybe_token_credentials()
    token_creds === nothing || return token_creds
    credentials_file = getenv_first(["CLOUDBASE_GCP_LIVE_CREDENTIALS_FILE", "GOOGLE_APPLICATION_CREDENTIALS"])
    credentials_file === nothing && error("no GCP credentials configured")
    return CloudBase.GCP.Credentials(; application_credentials_file=credentials_file)
end

function configure_profile!(profile::String)
    if profile == "smoke"
        for (size, nparts) in SMOKE_PARTS
            CloudBenchmarks.SIZES[size] = (CloudBenchmarks.SIZES[size][1], nparts)
        end
        return SMOKE_SIZES, 1
    elseif profile == "full"
        return DEFAULT_SIZES, 3
    end
    error("unsupported CLOUDBENCH_PROFILE=$(repr(profile)); expected `smoke` or `full`")
end

function benchmark_config()
    profile = lowercase(get(ENV, "CLOUDBENCH_PROFILE", "full"))
    default_sizes, default_ntimes = configure_profile!(profile)
    nthreads = parse_int_vector_env("CLOUDBENCH_NTHREADS", [Threads.nthreads()])
    nworkers = parse_int_vector_env("CLOUDBENCH_NWORKERS", [0])
    tls = parse_symbol_vector_env("CLOUDBENCH_TLS", [:reseau])
    semaphore_limit = parse_int_vector_env("CLOUDBENCH_SEMAPHORE_LIMITS", [4 * Threads.nthreads()])
    operation = parse_symbol_vector_env("CLOUDBENCH_OPERATIONS", copy(DEFAULT_OPERATIONS))
    sizes = parse_int_vector_env("CLOUDBENCH_SIZES", default_sizes)
    ntimes = parse(Int, get(ENV, "CLOUDBENCH_NTIMES", string(default_ntimes)))
    record_profile = parse_bool_env("CLOUDBENCH_RECORD_PROFILE", false)
    return (; profile, nthreads, nworkers, tls, semaphore_limit, operation, sizes, ntimes, record_profile)
end

function print_config(machine_specs::String, bucket::CloudBase.GCP.Bucket, config)
    println("machine_specs=", machine_specs)
    println("bucket=", bucket.name)
    println("profile=", config.profile)
    println("nthreads=", join(config.nthreads, ","))
    println("nworkers=", join(config.nworkers, ","))
    println("tls=", join(string.(config.tls), ","))
    println("semaphore_limit=", join(config.semaphore_limit, ","))
    println("operation=", join(string.(config.operation), ","))
    println("sizes=", join(config.sizes, ","))
    println("ntimes=", config.ntimes)
    println("record_profile=", config.record_profile)
    return nothing
end

function main()
    bucket_name = require_env(["CLOUDBENCH_GCP_BUCKET"])
    bucket = CloudBase.GCP.Bucket(bucket_name)
    machine_specs = get(ENV, "CLOUDBENCH_MACHINE_SPECS", default_machine_specs())
    config = benchmark_config()
    print_config(machine_specs, bucket, config)
    parse_bool_env("CLOUDBENCH_DRY_RUN", false) && return nothing
    credentials = load_credentials()
    file = CloudBenchmarks.runbenchmarks(
        machine_specs,
        credentials,
        bucket;
        nthreads=config.nthreads,
        nworkers=config.nworkers,
        tls=config.tls,
        semaphore_limit=config.semaphore_limit,
        operation=config.operation,
        sizes=config.sizes,
        ntimes=config.ntimes,
        profile=config.record_profile,
    )
    output_dir = get(ENV, "CLOUDBENCH_OUTPUT_DIR", joinpath(@__DIR__, "..", "vm", "results"))
    mkpath(output_dir)
    src = abspath(file)
    dest = joinpath(output_dir, basename(file))
    if src != dest
        cp(src, dest; force=true)
        rm(src; force=true)
    end
    println("results=", dest)
    return nothing
end

main()
