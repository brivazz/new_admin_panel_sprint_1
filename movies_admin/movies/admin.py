from django.contrib import admin
from .models import (Genre, Filmwork, Person,
                     GenreFilmwork, PersonFilmwork)


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork


class PersonFilmworkInline(admin.TabularInline):
    model = PersonFilmwork


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ('name', 'description')


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ('full_name',)


@admin.register(Filmwork)
class FilmworkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmworkInline, PersonFilmworkInline)
    list_display = ('title', 'film_type', 'creation_date',
                    'rating', 'created', 'modified')
    list_filter = ('film_type', 'creation_date', 'genres')
    search_fields = ('title', 'description', 'id')
