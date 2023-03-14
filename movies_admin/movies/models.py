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
    name = models.CharField(_('name'), max_length=255)
    description = models.TextField(_('description'), blank=True)

    class Meta:
        db_table = 'content\'.\'genre'
        verbose_name = _('Genre')
        verbose_name_plural = _('Genres')

    def __str__(self):
        return self.name


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(_('full name'), max_length=255)

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
    rating = models.FloatField(_('rating'), blank=True, default=0.0,
                               validators=[MinValueValidator(0),
                                           MaxValueValidator(100)])
    film_type = models.TextField(_('film type'), choices=CHOICES)
    genres = models.ManyToManyField(Genre, through='GenreFilmwork')
    persons = models.ManyToManyField(Person, through='PersonFilmwork')

    class Meta:
        db_table = 'content\'.\'film_work'
        verbose_name = _('Film')
        verbose_name_plural = _('Films')

    def __str__(self):
        return self.title
    
    def get_queryset(self, request):
        queryset = (
            super()
            .get_queryset(request)
            .prefetch_related(*self.list_prefetch_related)
        )
        return queryset


class GenreFilmwork(UUIDMixin):
    film_work = models.ForeignKey(Filmwork, on_delete=models.CASCADE)
    genre = models.ForeignKey(Genre, on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'content\'.\'genre_film_work'
        indexes = [
            models.Index(fields=['film_work', 'genre'],
                         name='film_work_genre_idx'),
        ]


class PersonFilmwork(UUIDMixin):
    CHOICES = (
        ('director', _('Director')),
        ('screenwriter ', _('Screenwriter')),
        ('actor', _('Actor')),
    )
    film_work = models.ForeignKey(Filmwork, on_delete=models.CASCADE)
    person = models.ForeignKey(Person, on_delete=models.CASCADE)
    role = models.TextField(_('role'), choices=CHOICES)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'content\'.\'person_film_work'
        indexes = [
            models.Index(fields=['film_work', 'person', 'role'],
                         name='film_work_person_role__idx'),
        ]
