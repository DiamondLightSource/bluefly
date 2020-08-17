from typing import AsyncGenerator, Callable, Dict, Generic, List, Type, TypeVar, Union

T = TypeVar("T")

"""Channels are like ophyd Signals, but async"""


class Channel:
    source: str  # like ca://MyPv


class ChannelProvider:
    def make_channel(
        self, channel_type: Type[Channel], device_id: str, channels
    ) -> Channel:
        """Handle mapping from attr_name to AttrName"""
        raise NotImplementedError(self)


class _Readable(Generic[T]):
    @property
    async def value(self) -> T:
        raise NotImplementedError(self)

    @property
    async def values(self) -> AsyncGenerator[T, None]:
        raise NotImplementedError(self)


class _Writable(Generic[T]):
    async def put(self, value: T) -> T:
        raise NotImplementedError(self)


class ChannelRO(_Readable[T], Channel):
    """Channel that can be read from and monitored"""


class ChannelRW(_Readable[T], _Writable[T], Channel):
    """Channel that can be read from, monitored, and put to"""


class ChannelWO(_Writable[T], Channel):
    """Channel that can be put to"""


class ChannelX(Channel):
    """Channel that can be executed"""

    async def __call__(self):
        raise NotImplementedError(self)


ChannelSource = Union[List[str], Dict[str, str]]


class ChannelTree:
    """Containers for Channels, not a fully fledged ophyd Device"""

    # {attr_name: [channel_name]}
    __channel_source__: Dict[str, ChannelSource] = {}

    def __init__(self, name: str, provider: ChannelProvider):
        self.name = name
        # Fill in all annotated attributes with channels
        for attr_name, ann in self.__annotations__.items():
            child = provider.make_channel(
                ann, name, self.__channel_source__.get(attr_name, attr_name)
            )
            setattr(self, attr_name, child)


F = TypeVar("F", bound=Type[ChannelTree])


def channel_source(**attrs: ChannelSource) -> Callable[[F], F]:
    """Add details about how to map lists and dicts of channels"""

    def decorator(typ: F) -> F:
        for attr_name in attrs:
            ann = typ.__annotations__[attr_name]
            assert isinstance(
                ann, (List, Dict)
            ), f"Expecting List[Channel...] or Dict[str, Channel...], got {ann}"
        typ.__channel_source__ = getattr(typ, "__channel_source__", {}).copy() + attrs
        return typ

    return decorator
