import uuid
from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.utils.translation import gettext_lazy as _


class TimeStampedMixin(models.Model):
    created = models.DateTimeField(_('created'), auto_now_add=True)
    modified = models.DateTimeField(_('modified'), auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):
    name = models.CharField(_('name'), max_length=255, unique=True)
    description = models.TextField(_('description'), blank=True)

    class Meta:
        db_table = 'content\'.\'genre'
        verbose_name = _('Genre')
        verbose_name_plural = _('Genres')

    def __str__(self):
        return self.name


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(_('full name'), max_length=255, db_index=True)

    class Meta:
        db_table = 'content\'.\'person'
        verbose_name = _('Participant')
        verbose_name_plural = _('Participants')

    def __str__(self):
        return self.full_name


class Filmwork(UUIDMixin, TimeStampedMixin):
    CHOICES = (
        ('movie', _('Movie')),
        ('tv_show', _('TV Show')),
    )
    title = models.TextField(_('title'))
    description = models.TextField(_('description'), blank=True)
    creation_date = models.DateTimeField(_('creation date'))
    rating = models.FloatField(_('rating'), blank=True,
                               validators=[MinValueValidator(0),
                                           MaxValueValidator(100)])
    film_type = models.TextField(_('film type'), choices=CHOICES)
    genres = models.ManyToManyField(Genre, through='GenreFilmwork')
    persons = models.ManyToManyField(Person, through='PersonFilmwork')

    class Meta:
        db_table = 'content\'.\'film_work'
        verbose_name = _('Film')
        verbose_name_plural = _('Films')
        indexes = [
            models.Index(fields=['title', 'film_type']),
        ]

    def __str__(self):
        return self.title


class GenreFilmwork(UUIDMixin):
    film_work = models.ForeignKey(Filmwork, on_delete=models.CASCADE)
    genre = models.ForeignKey(Genre, on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'content\'.\'genre_film_work'
        indexes = [
            models.Index(fields=['film_work', 'genre'],
                         name='genre_film_work_idx'),
        ]


class PersonFilmwork(UUIDMixin):
    film_work = models.ForeignKey(Filmwork, on_delete=models.CASCADE)
    person = models.ForeignKey(Person, on_delete=models.CASCADE)
    role = models.TextField(_('role'))
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'content\'.\'person_film_work'
        indexes = [
            models.Index(fields=['role', 'person', 'film_work'],
                         name='role_person_film_work_idx'),
        ]
