"""Submission behavior must match the threaded executor used by Query."""

import threading
import traceback
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, as_completed, wait

import pytest

from util.exec_util import DummyThreadPoolExecutor


@pytest.fixture(params=[DummyThreadPoolExecutor, ThreadPoolExecutor])
def executor(request):
    with request.param(max_workers=1) as pool:
        yield pool


def _submit(pool, function, *args, **kwargs):
    # Bound the baseline failure, including SystemExit and KeyboardInterrupt,
    # so it cannot terminate pytest instead of reporting the regression.
    try:
        return pool.submit(function, *args, **kwargs)
    except BaseException as error:
        raise AssertionError("submit raised instead of returning a Future") from error


def _raise(error):
    raise error


@pytest.mark.parametrize("error_type", [ValueError, RuntimeError, SystemExit, KeyboardInterrupt])
def test_task_errors_are_stored_in_the_future(executor, error_type):
    error = error_type("task failed")
    future = _submit(executor, _raise, error)
    assert future.exception(timeout=5) is error
    assert future.done()
    assert not future.cancelled()
    with pytest.raises(error_type) as raised:
        future.result(timeout=5)
    assert raised.value is error
    assert "_raise" in [frame.name for frame in traceback.extract_tb(error.__traceback__)]


@pytest.mark.parametrize("value", [None, 0, {"granule": "G1"}])
def test_successful_result_is_preserved(executor, value):
    future = _submit(executor, lambda: value)
    assert future.result(timeout=5) is value
    assert future.exception(timeout=5) is None


def test_keyword_arguments_reach_the_callable(executor):
    def task(fn, *, suffix):
        return fn + suffix

    assert _submit(executor, task, fn="granule", suffix="-1").result(timeout=5) == "granule-1"


def test_failure_does_not_prevent_later_submissions(executor):
    visited = []
    error = RuntimeError("bad granule")

    def catalog(granule):
        visited.append(granule)
        if granule == "bad":
            raise error
        return granule

    futures = [_submit(executor, catalog, granule) for granule in ["first", "bad", "last"]]
    done, pending = wait(futures, timeout=5)
    assert not pending
    assert len(done) == len(futures)
    assert visited == ["first", "bad", "last"]
    assert futures[0].result() == "first"
    assert futures[1].exception() is error
    assert futures[2].result() == "last"
    assert set(as_completed(futures, timeout=5)) == set(futures)


def test_failed_future_supports_callbacks_and_wait(executor):
    error = ValueError("bad batch")
    future = _submit(executor, _raise, error)
    assert future.exception(timeout=5) is error
    callbacks = []
    future.add_done_callback(callbacks.append)
    assert callbacks == [future]
    done, pending = wait([future], timeout=5, return_when=FIRST_EXCEPTION)
    assert done == {future}
    assert not pending
    assert future.cancel() is False


def test_non_callable_submission_defers_type_error(executor):
    future = _submit(executor, None)
    with pytest.raises(TypeError):
        future.result(timeout=5)


def test_dummy_executes_immediately_on_the_calling_thread():
    caller = threading.get_ident()
    calls = []
    with DummyThreadPoolExecutor() as pool:
        future = pool.submit(lambda: calls.append(threading.get_ident()))
        assert calls == [caller]
        assert future.done()
        assert future.result() is None
