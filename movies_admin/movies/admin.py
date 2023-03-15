from django.contrib import admin
from .models import (Genre, Filmwork, Person,
                     GenreFilmwork, PersonFilmwork)


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork
    autocomplete_fields = ('genre',)


class PersonFilmworkInline(admin.TabularInline):
    model = PersonFilmwork
    extra = 1
    autocomplete_fields = ('person',)


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ('name', 'description')
    extra = 1
    search_fields = ('name', 'description')


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ('full_name',)
    search_fields = ('full_name', 'id')


@admin.register(Filmwork)
class FilmworkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmworkInline, PersonFilmworkInline)
    list_display = ('title', 'film_type', 'creation_date',
                    'rating', 'get_genres',
                    'created', 'modified')
    list_filter = ('film_type', 'creation_date', 'genres')
    search_fields = ('title', 'description', 'id')
    save_on_top = True
    save_as = True

    def get_genres(self, obj):
        return ','.join([genre.name for genre in obj.genres.all()])

    get_genres.short_description = 'Жанры фильма'


admin.site.site_title = 'Кинопроизведения'
admin.site.site_header = 'Кинопроизведения'
