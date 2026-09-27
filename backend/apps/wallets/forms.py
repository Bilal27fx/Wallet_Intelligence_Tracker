"""Formulaires de l'admin de la qualification."""

from django import forms


class KnownAddressImportForm(forms.Form):
    file = forms.FileField(label="Fichier CSV (colonnes : address, kind, label, chain)")
