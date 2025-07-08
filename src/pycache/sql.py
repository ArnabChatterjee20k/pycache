from abc import ABC, abstractmethod
import string

_formatter = string.Formatter()


class Composable(ABC):
    def __init__(self, fragment):
        self._fragments = fragment

    @abstractmethod
    def to_string(self):
        pass

    def __add__(self, other) -> "Composed":
        if isinstance(other, Composable):
            return Composed([self]) + Composed([other])
        elif isinstance(other, Composed):
            return Composed([self]) + other

        raise NotImplementedError()


class Composed(Composable):
    def __init__(self, fragments: list[Composable]):
        seq = []
        for i in fragments:
            if not isinstance(i, Composable):
                raise TypeError(f"{i!r} Not a type of 'Composable'")
            seq.append(i)
        super().__init__(seq)

    def to_string(self):
        composables = []
        for fragement in self._fragments:
            if isinstance(fragement, SQL):
                composables.append(fragement.to_string())
                composables.append(" ")
            else:
                composables.append(fragement.to_string())
        return "".join(composables)

    def __add__(self, other):
        if isinstance(other, Composable):
            return Composed(self._fragments + [other])
        elif isinstance(other, Composed):
            return Composed(self._fragments + other._fragments)

        raise NotImplementedError()


class SQL(Composable):
    def __init__(self, statement):
        if not isinstance(statement, str):
            raise TypeError("SQL statement must be a string")
        super().__init__(statement)

    def to_string(self):
        return self._fragments

    def format(self, *args, **kwargs):
        compose_list = []
        for pre, name, spec, conv in _formatter.parse(self._fragments):
            if spec or conv:
                raise TypeError("Non sql standard")

            if pre:
                compose_list.append(SQL(pre))

            if not name:
                continue

            if name.isdigit():
                raise TypeError("Format placeholder can't be string")

            compose_list.append(kwargs[name])

        return Composed(compose_list)


# SQL specific values. ex-> table name, column name
# TODO: can be made more specific by having dot between table name and column name. Ex -> table.coumn
class Identifier(Composable):
    def __init__(self, value):
        if not isinstance(value, str):
            raise TypeError("Identifer must be a string")
        super().__init__(value)

    def to_string(self):
        # backtick for the identifier
        return f"`{self._fragments.replace('`', '``')}`"


# User provided values
class Literal(Composable):
    def __init__(self, value):
        super().__init__(value)

    def to_string(self):
        if self._fragments is None:
            return "NULL"
        elif isinstance(self._fragments, bool):
            return "1" if self._fragments else "0"
        elif isinstance(self._fragments, str):
            return f"'{self._fragments.replace("'","''")}'"
        return str(self._fragments)


class Placeholder(Composable):
    def __init__(self, format="%s", multiplier=1):
        if not isinstance(format, str):
            raise TypeError("Placeholder format must be a string")
        if not isinstance(multiplier, int):
            raise TypeError("Multiplier must be an int")
        if multiplier < 0:
            raise ValueError("Multiplier must be atleast 1")
        super().__init__(", ".join([format] * multiplier))

    def to_string(self):
        return self._fragments
