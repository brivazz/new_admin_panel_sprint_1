import uuid
from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator


class TimeStampedMixin(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):
    name = models.CharField('name', max_length=255, unique=True)
    description = models.TextField('description', blank=True)

    class Meta:
        db_table = 'content\'.\'genre'
        verbose_name = 'Жанр'
        verbose_name_plural = 'Жанры'
    
    def __str__(self):
        return self.name


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(max_length=255)

    class Meta:
        db_table = 'content\'.\'person'
        verbose_name = 'Участник'
        verbose_name_plural = 'Участники'

    def __str__(self):
        return self.full_name


class Filmwork(UUIDMixin, TimeStampedMixin):
    CHOICES = (
        ('movie', 'Фильм'),
        ('tv_show', 'Телешоу'),
    )
    title = models.TextField('title')
    description = models.TextField('description', blank=True)
    creation_date = models.DateTimeField('creation date', default=2023)
    rating = models.FloatField('rating', blank=True,
                               validators=[MinValueValidator(0),
                                           MaxValueValidator(100)])
    type = models.TextField('type', choices=CHOICES)
    genres = models.ManyToManyField(Genre, through='GenreFilmwork')
    persons = models.ManyToManyField(Person, through='PersonFilmwork')

    class Meta:
        db_table = 'content\'.\'film_work'
        verbose_name = 'Фильм'
        verbose_name_plural = 'Фильмы'
    
    def __str__(self):
        return self.title


class GenreFilmwork(UUIDMixin):
    film_work = models.ForeignKey(Filmwork, on_delete=models.CASCADE)
    genre = models.ForeignKey(Genre, on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'content\'.\'genre_film_work'


class PersonFilmwork(UUIDMixin):
    film_work = models.ForeignKey(Filmwork, on_delete=models.CASCADE)
    person = models.ForeignKey(Person, on_delete=models.CASCADE)
    role = models.TextField('role', null=True)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'content\'.\'person_film_work'
