from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Literal, Union, Tuple
import string
import random
from os.path import join


class Attachment:
    def __init__(
            self,
            content: bytes | str,
            file_name: str,
            content_type: str,
            content_disposition: Literal['ATTACHMENT', 'INLINE'] = 'ATTACHMENT',
            content_description: str | None = None,
            content_id: str | None = None,
    ):
        if isinstance(content, str):
            with open(content, "rb") as f:
                self._content = f.read()
        else:
            self._content = content

        if content_disposition == 'INLINE' and content_id is None:
            content_id = Attachment.get_random_id()

        self._file_name = file_name
        self._content_type = content_type
        self._content_disposition = content_disposition
        self._content_description = content_description
        self._content_id = content_id

    def __repr__(self):
        if self._content_disposition == 'ATTACHMENT':
            dispo = self._content_disposition
        else:
            dispo = f'{self._content_disposition} [{self._content_id}]'
        return f'Attachment<{self._file_name} {len(self._content):,} bytes {self._content_type} {dispo}>'

    def serialize(self, dst_dir) -> dict:
        with open(join(dst_dir, self._file_name), 'wb') as f:
            f.write(self._content)

        d = {
            "ContentDisposition": self._content_disposition,
            "ContentType": self._content_type,
            "FileName": self._file_name,
        }

        if self._content_description is not None:
            d['ContentDescription'] = self._content_description

        if self._content_id is not None:
            d['ContentId'] = self._content_id

        return d

    # TODO: How to serialize to HySDS?

    @staticmethod
    def get_random_id(prefix=None, n=8) -> str:
        random_string = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(n))

        if prefix is not None:
            random_string = f'{prefix}_{random_string}'

        return random_string


class Source(ABC):
    """
    Abstract base class for report data sources.
    """

    _type = ''

    def __init__(
            self,
            source_id,
            window: Tuple[datetime, datetime] | None = None,
            **kwargs
    ):
        self._source_id = source_id
        self._data: dict | None = None
        self._attachments: List[Attachment] = []
        self._errors: List[str] = []

        if window is not None:
            window_start, window_end = window
            if window_start >= window_end:
                raise ValueError(f'Window start {window_start} must be before window end {window_end}')

            self._window_start = window_start
            self._window_end = window_end
        else:
            self._window_start = None
            self._window_end = None

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @property
    def source_id(self) -> str:
        """The identifier of this data source"""
        if self._type:
            return f'{self._type}-{self._source_id}'
        else:
            return self._source_id

    @abstractmethod
    def run(self):
        """Begin collecting report data from this source asynchronously"""
        ...

    def results(self) -> Dict[str, Union[dict, List[Attachment], List[str]]]:
        """Fetch the report results"""
        self._join()

        if self._data is None:
            raise ValueError('No data collected')

        return {
            'data': self._data,
            'attachments': self._attachments,
            'errors': self._errors,
        }

    @abstractmethod
    def _join(self):
        """Block until report data is collected, then sets the result variables"""
        ...
