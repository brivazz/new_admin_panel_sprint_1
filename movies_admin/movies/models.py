import uuid
from django.db import models


class Genre(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField('Жанр', max_length=255, unique=True)
    description = models.TextField('Описание', blank=True)
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'content\'.\'genre'
        verbose_name = 'Жанр'
        verbose_name_plural = 'Жанры'
    
    def __str__(self):
        return self.name


class Filmwork(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    title = models.TextField('Кинопроизведение')
    description = models.TextField('Описание', blank=True)
    creation_date = models.DateTimeField('Дата выхода', default=2023)
    rating = models.FloatField('Рейтинг', blank=True)
    type = models.TextField('Тип', choices=['movie', 'tv_show'])
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'content\'.\'film_work'
        verbose_name = 'Фильм'
        verbose_name_plural = 'Фильмы'
    
    def __str__(self):
        return self.title
