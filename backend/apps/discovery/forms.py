"""Formulaires de l'admin de la découverte."""

from django import forms

from apps.discovery.models import Chain


class ManualCandidateForm(forms.Form):
    chain = forms.ModelChoiceField(queryset=Chain.objects.active(), label="Chaîne")
    address = forms.CharField(max_length=66, label="Adresse du token")
