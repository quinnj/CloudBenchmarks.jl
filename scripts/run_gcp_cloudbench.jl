using CloudBenchVM

const ROOT_DIR = normpath(joinpath(@__DIR__, ".."))
const VM_DIR = joinpath(ROOT_DIR, "vm")
const ROOT_ENV_FILE = joinpath(ROOT_DIR, ".env")
const ENV_FILE = get(ENV, "CLOUDBENCH_ENV_FILE", joinpath(VM_DIR, "bench.env"))

CloudBenchVM.load_env_file!(ROOT_ENV_FILE; optional=true)
ENV_FILE != ROOT_ENV_FILE && CloudBenchVM.load_env_file!(ENV_FILE; optional=true)
CloudBenchVM.instantiate_project!(VM_DIR)

using CloudBenchmarks
using CloudBase

const DEFAULT_OPERATIONS = [:put, :get, :prefetchdownloadstream]
const DEFAULT_SIZES = [2^18, 2^20, 2^21, 2^22, 2^23, 2^26, 2^28, 2^30, 2^32]
const SMOKE_SIZES = [2^20]
const SMOKE_PARTS = Dict(2^20 => 4)

function load_credentials(config::CloudBenchVM.GCPConfig)::CloudBase.GCP.Credentials
    if config.access_token !== nothing
        if config.quota_project_id === nothing
            return CloudBase.GCP.Credentials(config.access_token::String)
        end
        return CloudBase.GCP.Credentials(config.access_token::String; quota_project_id=config.quota_project_id::String)
    end
    return CloudBase.GCP.Credentials(; application_credentials_file=config.credentials_file::String)
end

function print_config(machine_specs::String, bucket::CloudBase.GCP.Bucket, config::CloudBenchVM.BenchmarkConfig)
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
    provider = CloudBenchVM.load_gcp_config()
    bucket = CloudBase.GCP.Bucket(provider.bucket_name)
    machine_specs = get(ENV, "CLOUDBENCH_MACHINE_SPECS", CloudBenchVM.default_machine_specs())
    config = CloudBenchVM.load_benchmark_config(
        default_profile="full",
        default_sizes=DEFAULT_SIZES,
        smoke_sizes=SMOKE_SIZES,
        smoke_parts=SMOKE_PARTS,
        default_operations=DEFAULT_OPERATIONS,
        full_ntimes=3,
        smoke_ntimes=1,
    )
    CloudBenchVM.apply_size_part_overrides!(CloudBenchmarks.SIZES, config)
    print_config(machine_specs, bucket, config)
    CloudBenchVM.parse_bool_env("CLOUDBENCH_DRY_RUN", false) && return nothing

    credentials = load_credentials(provider)
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
    output_dir = CloudBenchVM.resolve_output_dir(joinpath(VM_DIR, "results"))
    println("results=", CloudBenchVM.move_results_to_output_dir!(file, output_dir))
    return nothing
end

main()
