import pytest
from vonnegut.pipeline.schema.types import Schema
from vonnegut.pipeline.results import (
    CheckResult, CheckStatus,
    ValidationSuccess, ValidationFailure, NodeValidationResult,
    ExecutionSuccess, ExecutionFailure, ExecutionResult,
)


def test_check_result_creation():
    cr = CheckResult(rule_name="syntax", status=CheckStatus.PASSED, message="OK")
    assert cr.status == CheckStatus.PASSED


def test_validation_success_pattern_match():
    result: NodeValidationResult = ValidationSuccess(
        output_schema=Schema(columns=[]),
        output_data=None,
        checks=[],
    )
    match result:
        case ValidationSuccess(output_schema=s):
            assert s is not None
        case ValidationFailure():
            pytest.fail("Should not match failure")


def test_execution_failure_pattern_match():
    result: ExecutionResult = ExecutionFailure(node_id="abc", error="boom")
    match result:
        case ExecutionFailure(node_id=nid, error=err):
            assert nid == "abc"
            assert err == "boom"
        case ExecutionSuccess():
            pytest.fail("Should not match success")
