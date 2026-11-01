using Test, CloudBenchmarks

struct ByteCountStore
    count::Int
end
CloudBenchmarks.do_op(::Nothing, store::ByteCountStore, nm, client, op, data, i) = store.count

@testset "Transferred bytes must match each requested object" begin
    bytes, cpu, gc = CloudBenchmarks.do_op_n(nothing, ByteCountStore(4), "fixture", 2, :get, 3, 4, 0)
    @test bytes == 12
    @test cpu >= 0
    @test gc >= 0
    @test_throws CompositeException CloudBenchmarks.do_op_n(nothing, ByteCountStore(3), "fixture", 2, :get, 1, 4, 0)
end

@testset "Individual samples survive independently of summary output" begin
    mktempdir() do dir
        path = joinpath(dir, "results", "samples.tsv")
        config = (; nthreads=2, nworkers=0, tls=:reseau, semaphore_limit=2, operation=:get, size_bytes=4)
        sample = (; gbps=1.0, wall=2.0, cpu=3.0, gc=0.0, nbytes=12)
        withenv("CLOUDBENCH_SAMPLES_FILE" => path) do
            CloudBenchmarks.record_sample(config, 1, sample)
            CloudBenchmarks.record_sample(config, 2, sample)
        end
        rows = split.(readlines(path), '\t')
        @test length(rows) == 3
        @test rows[1] == string.([keys(config)..., :repeat, keys(sample)...])
        @test rows[2][7] == "1"
        @test rows[3][7] == "2"
        @test rows[2][end] == "12"
    end
end
