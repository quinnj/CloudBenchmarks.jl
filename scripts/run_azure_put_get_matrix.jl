using CloudBenchVM

const ROOT_DIR = normpath(joinpath(@__DIR__, ".."))
const VM_DIR = joinpath(ROOT_DIR, "vm")
const ROOT_ENV_FILE = joinpath(ROOT_DIR, ".env")

CloudBenchVM.load_env_file!(ROOT_ENV_FILE; optional=true)
const ENV_FILE = get(ENV, "CLOUDBENCH_ENV_FILE", CloudBenchVM.default_env_file(VM_DIR, "azure"))
ENV_FILE != ROOT_ENV_FILE && CloudBenchVM.load_env_file!(ENV_FILE; optional=true)
CloudBenchVM.instantiate_project!(VM_DIR)

using CloudBase
using CloudStore
using Dates
using HTTP
using Random
using Reseau

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

function print_stack()
    println("julia=", VERSION)
    println("threads=", Threads.nthreads())
    println("HTTP_path=", pathof(HTTP))
    println("HTTP_VERSION=", isdefined(HTTP, :VERSION) ? HTTP.VERSION : nothing)
    println("Reseau_path=", pathof(Reseau))
    println("CloudBase_path=", pathof(CloudBase))
    println("CloudStore_path=", pathof(CloudStore))
    return nothing
end

function do_one(bucket, credentials, pool, prefix, op::Symbol, data, i::Int; allow_multipart::Bool)
    key = string(prefix, ".", i)
    if op === :put
        obj = CloudStore.put(bucket, key, data; credentials, pool, allowMultipart=allow_multipart, logerrors=true, nowarn=true)
        return Int(obj.size)
    elseif op === :get
        bytes = CloudStore.get(bucket, key; credentials, pool, allowMultipart=allow_multipart, logerrors=true, nowarn=true)
        return length(bytes)
    end
    error("unsupported matrix operation $(repr(op)); expected :put or :get")
end

function seed_get_inputs!(bucket, credentials, pool, prefix::String, data, nparts::Int; allow_multipart::Bool)
    @sync for i in 1:nparts
        Threads.@spawn do_one(bucket, credentials, pool, prefix, :put, data, i; allow_multipart)
    end
    return nothing
end

function run_cell!(io, bucket, credentials, run_id::String, account::String, container::String,
        sem::Int, allow_multipart::Bool, op::Symbol, size::Int, nparts::Int)
    pool = CloudBase.CloudPool(sem)
    try
        data = rand(UInt8, size)
        mode = allow_multipart ? "multipart" : "single"
        label = size_label(size)
        prefix = "$(run_id).s$(sem).$(mode).$(label)"
        if op === :get
            seed_get_inputs!(bucket, credentials, pool, prefix, data, nparts; allow_multipart)
        end
        nbytes = Threads.Atomic{Int}(0)
        seconds = @elapsed begin
            @sync for i in 1:nparts
                Threads.@spawn begin
                    n = do_one(bucket, credentials, pool, prefix, op, data, i; allow_multipart)
                    Threads.atomic_add!(nbytes, n)
                end
            end
        end
        gbps = ((8 * nbytes[]) / 1e9) / seconds
        row = (
            run_id,
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
            nbytes[],
            seconds,
            gbps,
        )
        println(io, join(row, '\t'))
        flush(io)
        @info "matrix cell" threads=Threads.nthreads() sem mode op label nparts gbps seconds
        return gbps
    finally
        close(pool)
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

    sems = CloudBenchVM.parse_int_vector_env("CLOUDBENCH_MATRIX_SEMAPHORES", [Threads.nthreads()])
    sizes = CloudBenchVM.parse_int_vector_env("CLOUDBENCH_MATRIX_SIZES", [64 * 1024 * 1024])
    nparts = CloudBenchVM.parse_int_env("CLOUDBENCH_MATRIX_PARTS", 8)
    modes = split_bool_csv("CLOUDBENCH_MATRIX_ALLOW_MULTIPART", [true, false])
    ops = CloudBenchVM.parse_symbol_vector_env("CLOUDBENCH_MATRIX_OPERATIONS", [:put, :get])
    keep_container = CloudBenchVM.parse_bool_env(["CLOUDBENCH_KEEP_STORE", "CLOUDBENCH_AZURE_KEEP_CONTAINER"], false)

    print_stack()
    println("matrix_output=", output)
    println("account=", config.account)
    println("container=", bucket.name)
    println("generated_container=", config.generated_container)
    println("semaphores=", join(sems, ','))
    println("sizes=", join(sizes, ','))
    println("parts=", nparts)
    println("allow_multipart=", join(modes, ','))
    println("operations=", join(ops, ','))
    CloudBenchVM.parse_bool_env("CLOUDBENCH_DRY_RUN", false) && return nothing

    if config.generated_container
        create_container!(bucket, credentials)
    end
    try
        open(output, "w") do io
            println(io, "run_id\taccount\tcontainer\tthreads\tsem\tmode\tallow_multipart\top\tsize_label\tsize_bytes\tnparts\tnbytes\tseconds\tgbps")
            for sem in sems, allow_multipart in modes, op in ops, size in sizes
                run_cell!(io, bucket, credentials, run_id, config.account, bucket.name, sem, allow_multipart, op, size, nparts)
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

main()
