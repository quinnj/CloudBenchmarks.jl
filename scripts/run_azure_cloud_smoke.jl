using Dates
using CloudBenchmarks
using CloudBase
using CloudStore

const DEFAULT_OPERATIONS = [:put, :get, :prefetchdownloadstream]
const DEFAULT_SIZE = 2^20
const DEFAULT_NTIMES = 1

function getenv_first(names::Vector{String})::Union{Nothing, String}
    for name in names
        value = strip(get(ENV, name, ""))
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
        item = strip(part)
        isempty(item) || push!(parts, item)
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

function parse_connection_string(raw::AbstractString)::Dict{String, String}
    parts = Dict{String, String}()
    for part in split(raw, ';')
        item = strip(part)
        isempty(item) && continue
        pair = split(item, '='; limit=2)
        length(pair) == 2 || continue
        parts[strip(pair[1])] = strip(pair[2])
    end
    return parts
end

function default_machine_specs()::String
    host = gethostname()
    date = Dates.format(Dates.now(Dates.UTC), dateformat"yyyymmdd-HHMMSS")
    return string(host, "-azure-smoke-", date)
end

function default_container_name()::String
    ts = Dates.format(Dates.now(Dates.UTC), dateformat"yyyymmddHHMMss")
    suffix = lpad(string(rand(UInt32), base=16), 8, '0')
    return lowercase(string("cbsmoke-", ts, "-", suffix))
end

function configure_smoke!(size::Int, nparts::Int)::Nothing
    name = CloudBenchmarks.SIZES[size][1]
    CloudBenchmarks.SIZES[size] = (name, nparts)
    return nothing
end

function load_connection_settings()
    conn = getenv_first(["CLOUDBENCH_AZURE_CONNECTION_STRING", "AZURE_STORAGE_CONNECTION_STRING"])
    return conn === nothing ? Dict{String, String}() : parse_connection_string(conn)
end

function load_account(parts::Dict{String, String})::String
    account = get(parts, "AccountName", "")
    isempty(account) || return account
    return require_env(["CLOUDBENCH_AZURE_ACCOUNT", "AZURE_STORAGE_ACCOUNT"])
end

function load_credentials(account::String, parts::Dict{String, String})::CloudBase.Azure.Credentials
    if haskey(parts, "AccountKey")
        return CloudBase.Azure.Credentials(account, parts["AccountKey"])
    end
    if haskey(parts, "SharedAccessSignature")
        return CloudBase.Azure.Credentials(parts["SharedAccessSignature"])
    end
    key = getenv_first(["CLOUDBENCH_AZURE_KEY", "AZURE_STORAGE_KEY"])
    key === nothing || return CloudBase.Azure.Credentials(account, key)
    token = getenv_first([
        "CLOUDBENCH_AZURE_ACCESS_TOKEN",
        "AZURE_STORAGE_ACCESS_TOKEN",
        "AZURE_STORAGE_SAS_TOKEN",
        "AZURE_SAS_TOKEN",
        "SAS_TOKEN",
    ])
    token === nothing && error("no Azure credentials configured")
    return CloudBase.Azure.Credentials(token)
end

function load_container(account::String)
    name = getenv_first(["CLOUDBENCH_AZURE_CONTAINER"])
    generated = name === nothing
    name = generated ? default_container_name() : name
    host = getenv_first(["CLOUDBENCH_AZURE_HOST"])
    container = host === nothing ? CloudBase.Azure.Container(name, account) : CloudBase.Azure.Container(name, account; host=host)
    return container, generated
end

function create_container!(container::CloudBase.Azure.Container, credentials::CloudBase.Azure.Credentials)::Nothing
    resp = CloudBase.Azure.put("$(container.baseurl)?restype=container", String[]; credentials, status_exception=false)
    resp.status in (201, 202, 409) || error("failed to create Azure container $(container.name); status=$(resp.status)")
    return nothing
end

function delete_container!(container::CloudBase.Azure.Container, credentials::CloudBase.Azure.Credentials)::Nothing
    resp = CloudBase.Azure.delete("$(container.baseurl)?restype=container"; credentials, status_exception=false)
    resp.status in (202, 404) || error("failed to delete Azure container $(container.name); status=$(resp.status)")
    return nothing
end

function cleanup_container!(container::CloudBase.Azure.Container, credentials::CloudBase.Azure.Credentials)::Nothing
    for object in CloudStore.list(container; credentials)
        CloudStore.delete(object; credentials, nowarn=true)
    end
    delete_container!(container, credentials)
    return nothing
end

function print_config(machine_specs::String, container::CloudBase.Azure.Container, generated::Bool, nthreads::Vector{Int}, semaphore_limit::Vector{Int}, tls::Vector{Symbol}, operations::Vector{Symbol}, size::Int, nparts::Int)::Nothing
    println("machine_specs=", machine_specs)
    println("container=", container.name)
    println("generated_container=", generated)
    println("tls=", join(string.(tls), ","))
    println("nthreads=", join(nthreads, ","))
    println("semaphore_limit=", join(semaphore_limit, ","))
    println("operations=", join(string.(operations), ","))
    println("size=", size)
    println("nparts=", nparts)
    println("ntimes=", DEFAULT_NTIMES)
    return nothing
end

function main()
    size = parse(Int, get(ENV, "CLOUDBENCH_AZURE_SMOKE_SIZE", string(DEFAULT_SIZE)))
    nparts = parse(Int, get(ENV, "CLOUDBENCH_AZURE_SMOKE_PARTS", "4"))
    configure_smoke!(size, nparts)

    connection_settings = load_connection_settings()
    account = load_account(connection_settings)
    credentials = load_credentials(account, connection_settings)
    container, generated = load_container(account)

    tls = parse_symbol_vector_env("CLOUDBENCH_TLS", [:reseau])
    nthreads = parse_int_vector_env("CLOUDBENCH_NTHREADS", [Threads.nthreads()])
    semaphore_limit = parse_int_vector_env("CLOUDBENCH_SEMAPHORE_LIMITS", [4 * Threads.nthreads()])
    operations = parse_symbol_vector_env("CLOUDBENCH_OPERATIONS", copy(DEFAULT_OPERATIONS))
    machine_specs = get(ENV, "CLOUDBENCH_MACHINE_SPECS", default_machine_specs())
    keep_container = parse_bool_env("CLOUDBENCH_AZURE_KEEP_CONTAINER", false)

    print_config(machine_specs, container, generated, nthreads, semaphore_limit, tls, operations, size, nparts)
    parse_bool_env("CLOUDBENCH_DRY_RUN", false) && return nothing

    create_container!(container, credentials)
    results_file = ""
    try
        results_file = CloudBenchmarks.runbenchmarks(
            machine_specs,
            credentials,
            container;
            nthreads=nthreads,
            nworkers=[0],
            tls=tls,
            semaphore_limit=semaphore_limit,
            operation=operations,
            sizes=[size],
            ntimes=DEFAULT_NTIMES,
            profile=false,
        )
    finally
        if generated && !keep_container
            cleanup_container!(container, credentials)
        end
    end

    output_dir = get(ENV, "CLOUDBENCH_OUTPUT_DIR", joinpath(@__DIR__, "..", "vm", "results"))
    mkpath(output_dir)
    src = abspath(results_file)
    dest = joinpath(output_dir, basename(results_file))
    if src != dest
        cp(src, dest; force=true)
        rm(src; force=true)
    end
    println("results=", dest)
    return nothing
end

main()
