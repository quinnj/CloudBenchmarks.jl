using CloudBenchVM

const ROOT_DIR = normpath(joinpath(@__DIR__, ".."))
const VM_DIR = joinpath(ROOT_DIR, "vm")
const ROOT_ENV_FILE = joinpath(ROOT_DIR, ".env")
const ENV_FILE = get(ENV, "CLOUDBENCH_ENV_FILE", joinpath(VM_DIR, "azure.env"))

CloudBenchVM.load_env_file!(ROOT_ENV_FILE; optional=true)
ENV_FILE != ROOT_ENV_FILE && CloudBenchVM.load_env_file!(ENV_FILE; optional=true)
CloudBenchVM.instantiate_project!(VM_DIR)

using CloudBenchmarks
using CloudBase
using CloudStore

const DEFAULT_OPERATIONS = [:put, :get, :prefetchdownloadstream]

function load_credentials(config::CloudBenchVM.AzureConfig)::CloudBase.Azure.Credentials
    if config.key !== nothing
        return CloudBase.Azure.Credentials(config.account, config.key::String)
    end
    return CloudBase.Azure.Credentials(config.token::String)
end

function load_container(config::CloudBenchVM.AzureConfig)
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

function print_config(machine_specs::String, container::CloudBase.Azure.Container, provider::CloudBenchVM.AzureConfig, config::CloudBenchVM.BenchmarkConfig)
    println("machine_specs=", machine_specs)
    println("container=", container.name)
    println("generated_container=", provider.generated_container)
    println("profile=", config.profile)
    println("tls=", join(string.(config.tls), ","))
    println("nthreads=", join(config.nthreads, ","))
    println("nworkers=", join(config.nworkers, ","))
    println("semaphore_limit=", join(config.semaphore_limit, ","))
    println("operations=", join(string.(config.operation), ","))
    println("sizes=", join(config.sizes, ","))
    println("ntimes=", config.ntimes)
    return nothing
end

function main()
    smoke_size = CloudBenchVM.parse_int_env("CLOUDBENCH_AZURE_SMOKE_SIZE", 2^20)
    smoke_parts = CloudBenchVM.parse_int_env("CLOUDBENCH_AZURE_SMOKE_PARTS", 4)
    provider = CloudBenchVM.load_azure_config()
    container = load_container(provider)
    machine_specs = get(ENV, "CLOUDBENCH_MACHINE_SPECS", CloudBenchVM.default_machine_specs("azure-smoke"))
    config = CloudBenchVM.load_benchmark_config(
        default_profile="smoke",
        default_sizes=[smoke_size],
        smoke_sizes=[smoke_size],
        smoke_parts=Dict(smoke_size => smoke_parts),
        default_operations=DEFAULT_OPERATIONS,
        full_ntimes=1,
        smoke_ntimes=1,
    )
    CloudBenchVM.apply_size_part_overrides!(CloudBenchmarks.SIZES, config)
    print_config(machine_specs, container, provider, config)
    CloudBenchVM.parse_bool_env("CLOUDBENCH_DRY_RUN", false) && return nothing

    credentials = load_credentials(provider)
    keep_container = CloudBenchVM.parse_bool_env("CLOUDBENCH_AZURE_KEEP_CONTAINER", false)
    results_file = ""
    try
        provider.generated_container && create_container!(container, credentials)
        results_file = CloudBenchmarks.runbenchmarks(
            machine_specs,
            credentials,
            container;
            nthreads=config.nthreads,
            nworkers=config.nworkers,
            tls=config.tls,
            semaphore_limit=config.semaphore_limit,
            operation=config.operation,
            sizes=config.sizes,
            ntimes=config.ntimes,
            profile=config.record_profile,
        )
    finally
        if provider.generated_container && !keep_container
            cleanup_container!(container, credentials)
        end
    end

    output_dir = CloudBenchVM.resolve_output_dir(joinpath(VM_DIR, "results"))
    println("results=", CloudBenchVM.move_results_to_output_dir!(results_file, output_dir))
    return nothing
end

main()
