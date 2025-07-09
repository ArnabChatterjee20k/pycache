from abc import ABC, abstractmethod


class Datatype(ABC):
    _allowed_classes = ()
    _disallowed_classes = ()

    def __init__(self, value):
        self._value = value
        self.validate(value)

    @property
    @abstractmethod
    def value(self):
        """for getting the python native version of the datatype"""
        pass

    def validate(self, value):
        if isinstance(value, tuple(self._disallowed_classes)):
            disallowed = ", ".join(cls.__name__ for cls in self._disallowed_classes)
            raise TypeError(
                f"Invalid type: {type(value).__name__} is disallowed (Disallowed: {disallowed})"
            )

        if self._allowed_classes and not isinstance(
            value, tuple(self._allowed_classes)
        ):
            allowed = ", ".join(cls.__name__ for cls in self._allowed_classes)
            raise TypeError(
                f"Invalid type: expected one of ({allowed}), but got {type(value).__name__}"
            )
