from typing import (
    AsyncGenerator,
    Callable,
    Dict,
    Generic,
    Sequence,
    Type,
    TypeVar,
    Union,
    cast,
    get_type_hints,
)


def _fail(*args, **kwargs):
    raise ValueError("Did you mean 'await channel.get()' instead?")


# Would love to write this recursively, but I don't think you can
ValueT = TypeVar(
    "ValueT",
    bool,
    int,
    float,
    str,
    Sequence[bool],
    Sequence[int],
    Sequence[float],
    Sequence[str],
    Dict[str, bool],
    Dict[str, int],
    Dict[str, float],
    Dict[str, str],
    Dict[str, Sequence[bool]],
    Dict[str, Sequence[int]],
    Dict[str, Sequence[float]],
    Dict[str, Sequence[str]],
)


class Channel:
    """Channels are like ophyd Signals, but async"""

    source: str  # like ca://MyPv

    # Try and catch people using the channel rather than its value
    __lt__ = __le__ = __eq__ = __ne__ = __gt__ = __ge__ = __bool__ = _fail


ChannelT = TypeVar("ChannelT", bound=Channel)
ChannelSource = Union[str, Dict[str, str]]


class ChannelProvider:
    def make_channels(self, device_id: str) -> Dict[str, Channel]:
        """Create the Channel for device_id with AttrName mapped to attr_name"""
        raise NotImplementedError(self)

    def make_channel(
        self, device_id: str, source: ChannelSource, channel_type: Type[ChannelT]
    ) -> ChannelT:
        """Handle mapping from attr_name to AttrName"""
        raise NotImplementedError(self)


class ChannelRO(Channel, Generic[ValueT]):
    """Channel that can be read from and monitored"""

    async def get(self) -> ValueT:
        raise NotImplementedError(self)

    async def observe(self) -> AsyncGenerator[ValueT, None]:
        raise NotImplementedError(self)
        yield


class ChannelWO(Channel, Generic[ValueT]):
    """Channel that can be put to"""

    async def set(self, value: ValueT) -> ValueT:
        raise NotImplementedError(self)


class ChannelRW(ChannelRO, ChannelWO):
    """Channel that can be read from, monitored, and put to"""


class ChannelX(Channel):
    """Channel that can be executed"""

    async def __call__(self):
        raise NotImplementedError(self)


class ChannelTree:
    """Containers for Channels, not a fully fledged ophyd Device"""

    # {attr_name: channel_name | [channel_name] | {key: channel_name}}
    __channel_sources__: Dict[str, ChannelSource] = {}

    def __init__(self, name: str, provider: ChannelProvider):
        self.name = name
        hints = get_type_hints(cast(Callable, self))
        if hints:
            # Fill in all annotated attributes with channels
            channels = {}
            for attr_name, ann in hints.items():
                source = self.__channel_sources__.get(attr_name, attr_name)
                try:
                    channels[attr_name] = provider.make_channel(name, source, ann)
                except Exception as e:
                    raise ValueError(f"Can't make channel for {attr_name}") from e
        else:
            # Fill in everything with defaults
            channels = provider.make_channels(name)
        for attr_name, channel in channels.items():
            setattr(self, attr_name, channel)


ChannelTreeT = TypeVar("ChannelTreeT", bound=Type[ChannelTree])


def channel_sources(**sources: ChannelSource) -> Callable[[ChannelTreeT], ChannelTreeT]:
    """Add details about how to map dicts of channels"""

    def decorator(typ: ChannelTreeT) -> ChannelTreeT:
        extra = sorted(set(sources) - set(get_type_hints(typ)))
        assert not extra, f"Channel sources {extra} not described with annotations"
        typ.__channel_sources__ = {**typ.__channel_sources__, **sources}
        return typ

    return decorator
