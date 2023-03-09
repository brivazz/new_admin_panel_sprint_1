import uuid
from datetime import datetime, timezone
from dataclasses import dataclass, field


@dataclass
class FilmWork:
    title: str
    type: str
    description: str = None
    creation_date: datetime = field(default=datetime.date(datetime.now()))
    created: datetime = field(default=datetime.now(timezone.utc))
    modified: datetime = field(default=datetime.now(timezone.utc))
    rating: float = field(default=0.0)
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class Genre:
    name: str
    description: str = None
    created: datetime = field(default=datetime.now(timezone.utc))
    modified: datetime = field(default=datetime.now(timezone.utc))
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class Person:
    full_name: str
    created: datetime = field(default=datetime.now(timezone.utc))
    modified: datetime = field(default=datetime.now(timezone.utc))
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class GenreFilmWork:
    genre_id: uuid.UUID
    film_work_id: uuid.UUID
    created: datetime = field(default=datetime.now(timezone.utc))
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class PersonFilmWork:
    role: str
    person_id: uuid.UUID
    film_work_id: uuid.UUID
    created: datetime = field(default=datetime.now(timezone.utc))
    id: uuid.UUID = field(default_factory=uuid.uuid4)


