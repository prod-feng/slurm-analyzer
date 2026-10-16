# tests/test_parser.py

from slurm_analytics.parser import (
    parse_gpu_count,
    parse_gpu_memory,
    parse_gpu_utilization,
    parse_job_id,
    parse_slurm_duration,
    parse_slurm_memory,
    parse_tres,
)


def test_parse_normal_job():

    result = parse_job_id(
        "48327"
    )

    assert result.canonical_job_id == "48327"
    assert result.step_id is None
    assert result.step_type == "job"


def test_parse_batch_step():

    result = parse_job_id(
        "48327.batch"
    )

    assert result.canonical_job_id == "48327"
    assert result.step_id == "batch"
    assert result.step_type == "batch"


def test_parse_extern_step():

    result = parse_job_id(
        "48327.extern"
    )

    assert result.canonical_job_id == "48327"
    assert result.step_id == "extern"
    assert result.step_type == "extern"


def test_parse_application_step():

    result = parse_job_id(
        "48327.99"
    )

    assert result.canonical_job_id == "48327"
    assert result.step_id == "99"
    assert result.step_type == "application"


def test_parse_array_job():

    result = parse_job_id(
        "49898_3"
    )

    assert result.canonical_job_id == "49898_3"
    assert result.step_id is None
    assert result.step_type == "job"


def test_parse_array_application_step():

    result = parse_job_id(
        "49898_3.99"
    )

    assert result.canonical_job_id == "49898_3"
    assert result.step_id == "99"
    assert result.step_type == "application"


def test_duration():

    assert (
        parse_slurm_duration(
            "00:01:30"
        )
        == 90
    )


def test_duration_with_days():

    assert (
        parse_slurm_duration(
            "1-02:03:04"
        )
        == 93784
    )


def test_memory():

    assert (
        parse_slurm_memory(
            "4K"
        )
        == 4096
    )

    assert (
        parse_slurm_memory(
            "1G"
        )
        == 1024 ** 3
    )


def test_tres():

    result = parse_tres(
        "cpu=4,gres/gpu=1,mem=128G,node=1"
    )

    assert result["cpu"] == "4"
    assert result["gres/gpu"] == "1"
    assert result["mem"] == "128G"


def test_gpu_count():

    assert (
        parse_gpu_count(
            "cpu=8,gres/gpu=2"
        )
        == 2
    )


def test_typed_gpu_count():

    assert (
        parse_gpu_count(
            "gres/gpu:a100=4"
        )
        == 4
    )


def test_multiple_typed_gpus():

    assert (
        parse_gpu_count(
            "gres/gpu:a100=4,gres/gpu:h100=2"
        )
        == 6
    )


def test_gpu_utilization():

    assert (
        parse_gpu_utilization(
            "cpu=1,gres/gpuutil=76.5"
        )
        == 76.5
    )


def test_gpu_memory():

    assert (
        parse_gpu_memory(
            "gres/gpumem=4G"
        )
        == 4 * 1024 ** 3
    )



def test_parse_gpu_count_prefers_typed_counts_over_aggregate_total():
    # The generic total and typed values describe the same GPUs.
    assert parse_gpu_count("gres/gpu=4,gres/gpu:a100=4") == 4.0


def test_parse_gpu_count_sums_multiple_typed_gpu_models_without_generic_double_count():
    assert parse_gpu_count("gres/gpu=10,gres/gpu:a100=8,gres/gpu:h100=2") == 10.0


def test_parse_gpu_count_supports_generic_gpu_count():
    assert parse_gpu_count("cpu=8,gres/gpu=2,node=1") == 2.0


def test_gpu_utilization_from_gres_usage_in_ave_short_key():
    assert parse_gpu_utilization("gpuutil=99,gpumem=22524M") == 99.0


def test_gpu_utilization_key_is_case_insensitive():
    assert parse_gpu_utilization("GPUUTIL=76.5") == 76.5


def test_gpu_memory_from_gres_usage_in_ave_short_key():
    assert parse_gpu_memory("gpumem=4G") == 4 * 1024 ** 3
