import uuid
from datetime import datetime, timezone
from dataclasses import dataclass, field


@dataclass
class MixinId:
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class MixinDate:
    created: datetime = field(default=datetime.now(timezone.utc))
    modified: datetime = field(default=datetime.now(timezone.utc))


@dataclass
class FilmWork(MixinId, MixinDate):
    title: str = field(default_factory=str)
    film_type: str = field(default_factory=str)
    description: str = None
    creation_date: datetime = field(default=datetime.date(datetime.now()))
    rating: float = field(default=0.0)


@dataclass
class Genre(MixinId, MixinDate):
    name: str = field(default_factory=str)
    description: str = None


@dataclass
class Person(MixinId, MixinDate):
    full_name: str = field(default_factory=str)


@dataclass
class GenreFilmWork(MixinId):
    film_work_id: uuid.UUID = field(default_factory=uuid.UUID)
    genre_id: uuid.UUID = field(default_factory=uuid.UUID)
    created: datetime = field(default=datetime.now(timezone.utc))


@dataclass
class PersonFilmWork(MixinId):
    role: str = field(default_factory=str)
    person_id: uuid.UUID = field(default_factory=uuid.UUID)
    film_work_id: uuid.UUID = field(default_factory=uuid.UUID)
    created: datetime = field(default=datetime.now(timezone.utc))
