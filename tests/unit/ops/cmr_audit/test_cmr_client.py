import pytest
from unittest.mock import AsyncMock, patch

from tools.ops.cmr_audit.cmr_client import async_cmr_post


def make_mock_fetch_post_url(response_pages: list[dict]):
    mock_responses = []
    for i, page in enumerate(response_pages):
        mock_response = AsyncMock()
        mock_response.json.return_value = page
        # provide CMR-Search-After on all but the last page
        if i < len(response_pages) - 1:
            mock_response.headers = {"CMR-Search-After": f"[{i},{i}]"}
        else:
            mock_response.headers = {}

        mock_response_context_manager = AsyncMock()  # because of use in `async with await ... as response` statement
        mock_response_context_manager.__aenter__.return_value = mock_response
        mock_responses.append(mock_response_context_manager)

    return AsyncMock(side_effect=mock_responses)


@pytest.mark.asyncio
async def test_async_cmr_post__when_1_page_returned__and__items_less_than_page_size():
    # ARRANGE
    cmr_page = {"hits": 2000, "items": [{"id": i} for i in range(1950)]}
    mock_fetch_post_url = make_mock_fetch_post_url([cmr_page])

    with patch("tools.ops.cmr_audit.cmr_client.fetch_post_url", mock_fetch_post_url):
        # ACT
        result = await async_cmr_post("dummy_url", "?dummy_data", session=AsyncMock())

    # ASSERT
    assert len(result) == 1
    assert result[0]["hits"] == 2000
    mock_fetch_post_url.assert_awaited_once()

@pytest.mark.asyncio
async def test_async_cmr_post__when_multi_pages_returned__but__items_less_than_page_size():
    # ARRANGE
    cmr_page1 = {"hits": 2001, "items": [{"id": i} for i in range(1950)]}
    cmr_page2 = {"hits": 2001, "items": [{"id": i} for i in range(1)]}
    mock_fetch_post_url = make_mock_fetch_post_url([cmr_page1, cmr_page2])

    with patch("tools.ops.cmr_audit.cmr_client.fetch_post_url", mock_fetch_post_url):
        # ACT
        result = await async_cmr_post("dummy_url", "?dummy_data", session=AsyncMock())

    # ASSERT
    assert len(result) == 2
    assert len(result[0]["items"]) == 1950
    assert len(result[1]["items"]) == 1
    assert mock_fetch_post_url.await_count == 2
