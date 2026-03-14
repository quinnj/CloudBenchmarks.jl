module CloudBenchVM

using Dates
using Pkg
using Sockets

export AzureConfig,
    BenchmarkConfig,
    GCPConfig,
    apply_size_part_overrides!,
    default_container_name,
    default_machine_specs,
    getenv_first,
    instantiate_project!,
    load_azure_config,
    load_benchmark_config,
    load_env_file!,
    load_gcp_config,
    move_results_to_output_dir!,
    parse_bool_env,
    parse_int_env,
    parse_int_vector_env,
    parse_symbol_env,
    parse_symbol_vector_env,
    require_env,
    resolve_output_dir,
    split_csv

struct BenchmarkConfig
    profile::String
    nthreads::Vector{Int}
    nworkers::Vector{Int}
    tls::Vector{Symbol}
    semaphore_limit::Vector{Int}
    operation::Vector{Symbol}
    sizes::Vector{Int}
    ntimes::Int
    record_profile::Bool
    part_overrides::Dict{Int, Int}
end

struct GCPConfig
    bucket_name::String
    access_token::Union{Nothing, String}
    credentials_file::Union{Nothing, String}
    quota_project_id::Union{Nothing, String}
end

struct AzureConfig
    account::String
    key::Union{Nothing, String}
    token::Union{Nothing, String}
    host::Union{Nothing, String}
    container_name::String
    generated_container::Bool
end

const _LOCAL_SOURCE_OVERRIDES = (
    "CloudBase" => "CLOUDBASE_PATH",
    "CloudStore" => "CLOUDSTORE_PATH",
    "Reseau" => "RESEAU_PATH",
)

function _resolve_local_source_path(project_dir::AbstractString, path::AbstractString)::String
    return isabspath(path) ? normpath(path) : normpath(joinpath(project_dir, path))
end

function _has_local_source_overrides()::Bool
    for (_, env_name) in _LOCAL_SOURCE_OVERRIDES
        isempty(strip(get(ENV, env_name, ""))) || return true
    end
    return false
end

function _activate_local_overlay_env!(project_dir::AbstractString)
    overlay_dir = joinpath(project_dir, ".local-overrides")
    mkpath(overlay_dir)
    Pkg.activate(overlay_dir)
    Pkg.develop(; path=normpath(joinpath(project_dir, "..")))
    for (pkg_name, env_name) in _LOCAL_SOURCE_OVERRIDES
        raw_path = strip(get(ENV, env_name, ""))
        isempty(raw_path) && continue
        resolved = _resolve_local_source_path(project_dir, raw_path)
        isdir(resolved) || error("$(env_name) points to a missing directory: $(resolved)")
        _ = pkg_name
        Pkg.develop(; path=resolved)
    end
    return nothing
end

function instantiate_project!(project_dir::AbstractString = normpath(joinpath(@__DIR__, "..")))
    parse_bool_env("CLOUDBENCH_SKIP_INSTANTIATE", false) && return nothing
    if _has_local_source_overrides()
        _activate_local_overlay_env!(project_dir)
    else
        Pkg.activate(project_dir)
    end
    ENV["JULIA_PROJECT"] = Base.active_project()
    Pkg.instantiate()
    return nothing
end

function load_env_file!(path::AbstractString; optional::Bool = true)
    if !isfile(path)
        optional && return false
        error("environment file not found: $(abspath(path))")
    end
    for raw in eachline(path)
        line = strip(raw)
        isempty(line) && continue
        startswith(line, '#') && continue
        startswith(line, "export ") && (line = strip(line[8:end]))
        eq = findfirst(==('='), line)
        eq === nothing && error("invalid env line in $(abspath(path)): $(repr(raw))")
        key = strip(line[firstindex(line):prevind(line, eq)])
        isempty(key) && error("invalid env key in $(abspath(path)): $(repr(raw))")
        value = strip(line[nextind(line, eq):lastindex(line)])
        if length(value) >= 2
            first_char = first(value)
            last_char = last(value)
            if (first_char == '"' && last_char == '"') || (first_char == '\'' && last_char == '\'')
                value = value[nextind(value, firstindex(value)):prevind(value, lastindex(value))]
            end
        end
        ENV[key] = value
    end
    return true
end

function getenv_first(names::AbstractVector{<:AbstractString})::Union{Nothing, String}
    for name in names
        value = strip(get(ENV, String(name), ""))
        isempty(value) || return value
    end
    return nothing
end

function require_env(names::AbstractVector{<:AbstractString})::String
    value = getenv_first(names)
    value === nothing && error("missing required environment variable; set one of $(join(names, ", "))")
    return value
end

function split_csv(value::AbstractString)::Vector{String}
    parts = String[]
    for part in split(value, ',')
        item = strip(part)
        isempty(item) || push!(parts, item)
    end
    return parts
end

nonempty_or_nothing(value::AbstractString) = isempty(strip(value)) ? nothing : String(strip(value))

function parse_bool_env(name::String, default::Bool)::Bool
    raw = lowercase(strip(get(ENV, name, "")))
    isempty(raw) && return default
    raw in ("1", "true", "yes", "y", "on") && return true
    raw in ("0", "false", "no", "n", "off") && return false
    error("invalid boolean value for $name: $(repr(raw))")
end

function parse_int_env(name::String, default::Int)::Int
    raw = strip(get(ENV, name, ""))
    isempty(raw) && return default
    return parse(Int, raw)
end

function parse_symbol_env(name::String, default::Symbol)::Symbol
    raw = strip(get(ENV, name, ""))
    isempty(raw) && return default
    return Symbol(raw)
end

function parse_int_vector_env(name::String, default::Vector{Int})::Vector{Int}
    raw = strip(get(ENV, name, ""))
    isempty(raw) && return copy(default)
    return parse.(Int, split_csv(raw))
end

function parse_symbol_vector_env(name::String, default::Vector{Symbol})::Vector{Symbol}
    raw = strip(get(ENV, name, ""))
    isempty(raw) && return copy(default)
    return Symbol.(split_csv(raw))
end

function default_machine_specs(tag::AbstractString = "")::String
    suffix = isempty(tag) ? "" : string("-", tag)
    stamp = Dates.format(Dates.now(Dates.UTC), dateformat"yyyymmdd-HHMMSS")
    return string(gethostname(), suffix, "-", stamp)
end

function default_container_name()::String
    stamp = Dates.format(Dates.now(Dates.UTC), dateformat"yyyymmddHHMMSS")
    suffix = lowercase(string(time_ns(), base=16))
    return string("cbsmoke-", stamp, "-", suffix[max(1, end - 7):end])
end

function load_benchmark_config(;
        default_profile::String,
        default_sizes::Vector{Int},
        smoke_sizes::Vector{Int},
        smoke_parts::AbstractDict{Int, Int} = Dict{Int, Int}(),
        default_operations::Vector{Symbol} = [:put, :get, :prefetchdownloadstream],
        default_nworkers::Vector{Int} = [0],
        smoke_ntimes::Int = 1,
        full_ntimes::Int = 3,
    )::BenchmarkConfig
    profile = lowercase(get(ENV, "CLOUDBENCH_PROFILE", default_profile))
    sizes = Int[]
    part_overrides = Dict{Int, Int}()
    ntimes = 0
    if profile == "smoke"
        sizes = copy(smoke_sizes)
        for (size, nparts) in smoke_parts
            part_overrides[size] = nparts
        end
        ntimes = smoke_ntimes
    elseif profile == "full"
        sizes = copy(default_sizes)
        ntimes = full_ntimes
    else
        error("unsupported CLOUDBENCH_PROFILE=$(repr(profile)); expected `smoke` or `full`")
    end
    sizes = parse_int_vector_env("CLOUDBENCH_SIZES", sizes)
    nthreads = parse_int_vector_env("CLOUDBENCH_NTHREADS", [Threads.nthreads()])
    nworkers = parse_int_vector_env("CLOUDBENCH_NWORKERS", copy(default_nworkers))
    tls = parse_symbol_vector_env("CLOUDBENCH_TLS", [:reseau])
    semaphore_limit = parse_int_vector_env("CLOUDBENCH_SEMAPHORE_LIMITS", [4 * Threads.nthreads()])
    operation = parse_symbol_vector_env("CLOUDBENCH_OPERATIONS", copy(default_operations))
    ntimes = parse_int_env("CLOUDBENCH_NTIMES", ntimes)
    record_profile = parse_bool_env("CLOUDBENCH_RECORD_PROFILE", false)
    retained_part_overrides = Dict{Int, Int}()
    for size in sizes
        haskey(part_overrides, size) || continue
        retained_part_overrides[size] = part_overrides[size]
    end
    return BenchmarkConfig(
        profile,
        nthreads,
        nworkers,
        tls,
        semaphore_limit,
        operation,
        sizes,
        ntimes,
        record_profile,
        retained_part_overrides,
    )
end

function apply_size_part_overrides!(sizes, config::BenchmarkConfig)
    for (size, nparts) in config.part_overrides
        haskey(sizes, size) || continue
        sizes[size] = (sizes[size][1], nparts)
    end
    return sizes
end

function load_gcp_config()::GCPConfig
    bucket_name = require_env(["CLOUDBENCH_GCP_BUCKET"])
    access_token = getenv_first(["CLOUDBASE_GCP_LIVE_ACCESS_TOKEN", "CLOUDBENCH_GCP_ACCESS_TOKEN"])
    credentials_file = getenv_first(["CLOUDBASE_GCP_LIVE_CREDENTIALS_FILE", "GOOGLE_APPLICATION_CREDENTIALS"])
    access_token === nothing && credentials_file === nothing && error("no GCP credentials configured")
    quota_project_id = nonempty_or_nothing(get(ENV, "CLOUDBENCH_GCP_QUOTA_PROJECT", ""))
    return GCPConfig(bucket_name, access_token, credentials_file, quota_project_id)
end

function parse_connection_string(raw::AbstractString)::Dict{String, String}
    parts = Dict{String, String}()
    for part in split(raw, ';')
        item = strip(part)
        isempty(item) && continue
        pair = split(item, '='; limit = 2)
        length(pair) == 2 || continue
        parts[strip(pair[1])] = strip(pair[2])
    end
    return parts
end

function load_azure_config()::AzureConfig
    conn = getenv_first(["CLOUDBENCH_AZURE_CONNECTION_STRING", "AZURE_STORAGE_CONNECTION_STRING"])
    parts = conn === nothing ? Dict{String, String}() : parse_connection_string(conn)
    account = get(parts, "AccountName", "")
    isempty(account) && (account = require_env(["CLOUDBENCH_AZURE_ACCOUNT", "AZURE_STORAGE_ACCOUNT"]))
    key = nonempty_or_nothing(get(parts, "AccountKey", ""))
    key === nothing && (key = getenv_first(["CLOUDBENCH_AZURE_KEY", "AZURE_STORAGE_KEY"]))
    token = nonempty_or_nothing(get(parts, "SharedAccessSignature", ""))
    if token === nothing
        token = getenv_first([
            "CLOUDBENCH_AZURE_ACCESS_TOKEN",
            "AZURE_STORAGE_ACCESS_TOKEN",
            "AZURE_STORAGE_SAS_TOKEN",
            "AZURE_SAS_TOKEN",
            "SAS_TOKEN",
        ])
    end
    key === nothing && token === nothing && error("no Azure credentials configured")
    host = getenv_first(["CLOUDBENCH_AZURE_HOST"])
    container_name = getenv_first(["CLOUDBENCH_AZURE_CONTAINER"])
    generated_container = container_name === nothing
    container_name = generated_container ? default_container_name() : container_name
    return AzureConfig(account, key, token, host, container_name, generated_container)
end

function resolve_output_dir(default_path::AbstractString)::String
    output_dir = abspath(get(ENV, "CLOUDBENCH_OUTPUT_DIR", default_path))
    mkpath(output_dir)
    return output_dir
end

function move_results_to_output_dir!(path::AbstractString, output_dir::AbstractString)::String
    src = abspath(path)
    dest = joinpath(output_dir, basename(src))
    if src != dest
        cp(src, dest; force = true)
        rm(src; force = true)
    end
    return dest
end

end
