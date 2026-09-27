"""Erreurs typées des services externes."""


class IntegrationError(Exception):
    """Erreur d'un service externe."""


class NotFound(IntegrationError):
    """La ressource demandée n'existe pas (HTTP 404)."""


class RateLimited(IntegrationError):
    """Le service refuse encore après toutes les tentatives (HTTP 429)."""


class UpstreamError(IntegrationError):
    """Réponse invalide ou erreur serveur persistante."""


class TooManyTransfers(IntegrationError):
    """Le token dépasse le plafond de transferts autorisé."""


class BudgetExhausted(IntegrationError):
    """Le budget quotidien de requêtes du service est épuisé."""
