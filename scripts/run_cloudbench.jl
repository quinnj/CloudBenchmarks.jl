using CloudBenchVM

const ROOT_DIR = normpath(joinpath(@__DIR__, ".."))
const VM_DIR = joinpath(ROOT_DIR, "vm")
const ROOT_ENV_FILE = joinpath(ROOT_DIR, ".env")

const DEFAULT_OPERATIONS = [:put, :get, :prefetchdownloadstream]
const DEFAULT_SIZES = [2^18, 2^20, 2^21, 2^22, 2^23, 2^26, 2^28, 2^30, 2^32]

CloudBenchVM.load_env_file!(ROOT_ENV_FILE; optional=true)
const PROVIDER = CloudBenchVM.provider_name("azure")
const ENV_FILE = get(ENV, "CLOUDBENCH_ENV_FILE", CloudBenchVM.default_env_file(VM_DIR, PROVIDER))
ENV_FILE != ROOT_ENV_FILE && CloudBenchVM.load_env_file!(ENV_FILE; optional=true)
CloudBenchVM.instantiate_project!(VM_DIR)

using CloudBenchmarks
using CloudBase
using CloudStore

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

function benchmark_config()
    smoke_size_names = PROVIDER == "azure" ? ["CLOUDBENCH_SMOKE_SIZE", "CLOUDBENCH_AZURE_SMOKE_SIZE"] : ["CLOUDBENCH_SMOKE_SIZE", "CLOUDBENCH_GCP_SMOKE_SIZE"]
    smoke_parts_names = PROVIDER == "azure" ? ["CLOUDBENCH_SMOKE_PARTS", "CLOUDBENCH_AZURE_SMOKE_PARTS"] : ["CLOUDBENCH_SMOKE_PARTS", "CLOUDBENCH_GCP_SMOKE_PARTS"]
    smoke_size = CloudBenchVM.parse_int_env(smoke_size_names, 2^20)
    smoke_parts = CloudBenchVM.parse_int_env(smoke_parts_names, 4)
    return CloudBenchVM.load_benchmark_config(
        default_profile="full",
        default_sizes=DEFAULT_SIZES,
        smoke_sizes=[smoke_size],
        smoke_parts=Dict(smoke_size => smoke_parts),
        default_operations=DEFAULT_OPERATIONS,
        full_ntimes=3,
        smoke_ntimes=1,
    )
end

function print_config(machine_specs::String, resource_name::String, generated_resource::Bool, config::CloudBenchVM.BenchmarkConfig)
    println("provider=", PROVIDER)
    println("machine_specs=", machine_specs)
    println("resource=", resource_name)
    println("generated_resource=", generated_resource)
    println("profile=", config.profile)
    println("tls=", join(string.(config.tls), ","))
    println("nthreads=", join(config.nthreads, ","))
    println("nworkers=", join(config.nworkers, ","))
    println("semaphore_limit=", join(config.semaphore_limit, ","))
    println("operations=", join(string.(config.operation), ","))
    println("sizes=", join(config.sizes, ","))
    println("ntimes=", config.ntimes)
    println("record_profile=", config.record_profile)
    return nothing
end

function load_provider_resources()
    if PROVIDER == "gcp"
        provider = CloudBenchVM.load_gcp_config()
        credentials = load_gcp_credentials(provider)
        bucket = CloudBase.GCP.Bucket(provider.bucket_name)
        return provider, credentials, bucket, false, nothing
    end

    provider = CloudBenchVM.load_azure_config()
    credentials = load_azure_credentials(provider)
    container = load_azure_container(provider)
    cleanup = provider.generated_container ? () -> cleanup_container!(container, credentials) : nothing
    return provider, credentials, container, provider.generated_container, cleanup
end

function main()
    config = benchmark_config()
    CloudBenchVM.apply_size_part_overrides!(CloudBenchmarks.SIZES, config)
    provider, credentials, store, generated_resource, cleanup = load_provider_resources()
    resource_name = PROVIDER == "gcp" ? (provider::CloudBenchVM.GCPConfig).bucket_name : (store::CloudBase.Azure.Container).name
    machine_specs = get(ENV, "CLOUDBENCH_MACHINE_SPECS", CloudBenchVM.default_machine_specs("$(PROVIDER)-$(config.profile)"))
    print_config(machine_specs, resource_name, generated_resource, config)
    CloudBenchVM.parse_bool_env("CLOUDBENCH_DRY_RUN", false) && return nothing

    keep_container = PROVIDER == "azure" && CloudBenchVM.parse_bool_env(["CLOUDBENCH_KEEP_STORE", "CLOUDBENCH_AZURE_KEEP_CONTAINER"], false)
    results_file = ""
    try
        if PROVIDER == "azure" && generated_resource
            create_container!(store::CloudBase.Azure.Container, credentials::CloudBase.Azure.Credentials)
        end
        results_file = CloudBenchmarks.runbenchmarks(
            machine_specs,
            credentials,
            store;
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
        if PROVIDER == "azure" && generated_resource && !keep_container
            cleanup === nothing || cleanup()
        end
    end

    output_dir = CloudBenchVM.resolve_output_dir(joinpath(VM_DIR, "results"))
    println("results=", CloudBenchVM.move_results_to_output_dir!(results_file, output_dir))
    return nothing
end

main()
