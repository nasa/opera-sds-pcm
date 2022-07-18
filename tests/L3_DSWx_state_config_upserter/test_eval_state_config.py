from unittest.mock import MagicMock

from L3_DSWx_state_config_upserter import eval_state_config
from L3_DSWx_state_config_upserter.eval_state_config import get_updated_state_config_doc


def test_get_updated_state_config_doc():
    # ARRANGE
    grq_es_mock = MagicMock()
    grq_es_mock.query.return_value = [{}]
    eval_state_config.grq_es = grq_es_mock

    # ACT
    result = get_updated_state_config_doc("dummy_state_config_doc_id")

    # ASSERT
    assert result is not None
